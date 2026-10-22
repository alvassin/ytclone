"""Versioned archive records and pure path/partition/fingerprint functions."""

import hashlib
import json
import re
from collections.abc import Iterator
from dataclasses import asdict, dataclass
from urllib.parse import quote, unquote, urlsplit

from yt import yson

from .table_format import (
    ROW_FORMAT,  # noqa: F401 -- compatibility import for existing callers
    TABLE_FORMATS,
    make_table_format,
    table_format_name,
    validate_table_format,
)

MIB = 1024 * 1024
FORMAT = "yt-s3-v2"
RESTORE_MARKER = "yt_s3_state"
ENCODING = {
    "tables": "binary_yson_list_fragment",
    "files": "bytes",
    "compression": "zstd",
}
DOCUMENT_KIND = "document"
KINDS = {"table", "file", "map_node", DOCUMENT_KIND}
ATTRIBUTES_NAME = "@attributes.yson"
ENCRYPTED_SUFFIX = ".enc"


def node_encoding(kind: str, table_format: dict | None = None) -> dict:
    """Describe the encoding used for each supported node kind."""
    if table_format is not None:
        if kind != "table":
            raise ValueError("Table format is only valid for tables")
        if table_format_name(table_format) == "skiff":
            return {**ENCODING, "tables": "skiff"}
    if kind == DOCUMENT_KIND:
        return {**ENCODING, "documents": "binary_yson_node"}
    return ENCODING


@dataclass(frozen=True)
class DiscoveredNode:
    """Source identity and lightweight metadata returned during discovery."""

    path: str
    kind: str
    node_id: str
    content_revision: int | None = None
    attribute_revision: int | None = None
    dynamic: bool | None = None
    disk_size: int | None = None
    estimated_size: int | None = None


@dataclass(frozen=True)
class Node:
    path: str
    kind: str
    node_id: str
    content_revision: int
    attributes: bytes
    row_count: int = 0
    data_size: int = 0
    data_weight: int = 0
    dynamic: bool = False
    disk_size: int | None = None
    attribute_revision: int | None = None
    table_format: dict | None = None


@dataclass(frozen=True)
class SourceEstimate:
    """Optional source resource totals for progress reporting."""

    node_count: int | None = None
    disk_size: int | None = None


@dataclass(frozen=True)
class Range:
    index: int
    start: int
    stop: int


@dataclass(frozen=True)
class ObjectRef:
    key: str
    size: int
    sha256: str
    # Bytes before encryption (compressed for data parts, raw YSON for attributes).
    plaintext_size: int | None = None
    plaintext_sha256: str | None = None


@dataclass(frozen=True)
class Part:
    object: ObjectRef
    start: int
    stop: int


def _integer(value: object) -> bool:
    """Check for a nonnegative integer, excluding booleans."""
    return type(value) is int and value >= 0


def _ref(value: dict) -> ObjectRef:
    """Validate and decode an object reference."""
    ref = ObjectRef(**value)
    if not isinstance(ref.key, str) or not ref.key or not _integer(ref.size):
        raise ValueError("Invalid object reference")
    if not isinstance(ref.sha256, str) or not re.fullmatch(r"[a-f0-9]{64}", ref.sha256):
        raise ValueError("Invalid object checksum")
    if ref.plaintext_size is not None or ref.plaintext_sha256 is not None:
        if not _integer(ref.plaintext_size) or not isinstance(
            ref.plaintext_sha256, str
        ):
            raise ValueError("Invalid plaintext integrity metadata")
        if not re.fullmatch(r"[a-f0-9]{64}", ref.plaintext_sha256):
            raise ValueError("Invalid plaintext checksum")
    return ref


def validate_encryption(value: dict) -> dict:
    """Accept only the fixed, versioned OpenSSL encryption profile."""
    if not isinstance(value, dict) or set(value) != {"format", "iterations", "key_id"}:
        raise ValueError("Invalid archive encryption settings")
    if (
        value["format"] != "openssl-aes-256-cbc-pbkdf2-sha256-v1"
        or type(value["iterations"]) is not int
        or value["iterations"] != 100000
        or not isinstance(value["key_id"], str)
        or not re.fullmatch(r"[a-f0-9]{64}", value["key_id"])
    ):
        raise ValueError("Unsupported archive encryption settings")
    return dict(value)


class UnsupportedArchiveFormat(ValueError):
    """The archive requires a different reader or a fresh export."""


def _validate_generation(generation: str) -> None:
    """Require a canonical UUID hex string safe to use in an object key."""
    if not isinstance(generation, str) or not re.fullmatch(r"[a-f0-9]{32}", generation):
        raise ValueError("Invalid archive generation: expected 32 lowercase hex digits")


@dataclass(frozen=True)
class Manifest:
    kind: str
    generation: str
    source: dict
    fingerprint: str
    attributes: ObjectRef
    parts: tuple[Part, ...]
    row_count: int = 0
    data_size: int = 0
    table_format: dict | None = None
    encryption: dict | None = None

    def to_bytes(self) -> bytes:
        """Serialize the versioned manifest as JSON."""
        record = asdict(self)
        if self.encryption is None:
            del record["encryption"]
        else:
            record["encryption"] = validate_encryption(self.encryption)
        # Keep unencrypted v2 records byte-compatible with existing readers.
        for ref in (record["attributes"], *(p["object"] for p in record["parts"])):
            if ref["plaintext_size"] is None and ref["plaintext_sha256"] is None:
                del ref["plaintext_size"]
                del ref["plaintext_sha256"]
        if self.table_format is None:
            del record["table_format"]
        else:
            record["table_format"] = validate_table_format(self.table_format)
        return json.dumps(
            {
                "format": FORMAT,
                "encoding": node_encoding(self.kind, self.table_format),
                **record,
            },
            sort_keys=True,
            indent=2,
        ).encode()

    @classmethod
    def from_bytes(cls, data: bytes) -> "Manifest":
        """Decode and validate an archive manifest."""
        try:
            raw = json.loads(data)
            archive_format = raw.pop("format")
            if archive_format != FORMAT:
                raise UnsupportedArchiveFormat(
                    f"Unsupported archive format {archive_format!r}; expected {FORMAT}. "
                    "Re-export the source to a new archive prefix."
                )
            table_format = raw.get("table_format")
            if table_format is not None:
                raw["table_format"] = validate_table_format(table_format)
            if raw.pop("encoding") != node_encoding(raw["kind"], table_format):
                raise ValueError("Unsupported archive encoding")
            raw["attributes"] = _ref(raw["attributes"])
            raw["parts"] = tuple(
                Part(object=_ref(p["object"]), start=p["start"], stop=p["stop"])
                for p in raw["parts"]
            )
            result = cls(**raw)
            if result.encryption is not None:
                validate_encryption(result.encryption)
            for ref in (result.attributes, *(p.object for p in result.parts)):
                if (ref.plaintext_sha256 is not None) != (
                    result.encryption is not None
                ):
                    raise ValueError(
                        "Object integrity metadata differs from encryption mode"
                    )
            _validate_generation(result.generation)
            if result.kind not in KINDS:
                raise ValueError("Unsupported node type")
            if not re.fullmatch(r"[a-f0-9]{64}", result.fingerprint):
                raise ValueError("Invalid fingerprint")
            if not all(_integer(v) for v in (result.row_count, result.data_size)):
                raise ValueError("Invalid node size")
            for key in ("cluster", "path", "node_id"):
                if not isinstance(result.source[key], str):
                    raise TypeError("Invalid source identity")
            if not _integer(result.source["content_revision"]):
                raise ValueError("Invalid content revision")
            if "attribute_revision" in result.source and not _integer(
                result.source["attribute_revision"]
            ):
                raise ValueError("Invalid attribute revision")
            end = 0
            keys = {result.attributes.key}
            for part in result.parts:
                if not _integer(part.start) or not _integer(part.stop):
                    raise ValueError("Invalid part range")
                if part.start != end or part.stop <= part.start:
                    raise ValueError("Parts must cover consecutive nonempty ranges")
                if part.object.key in keys:
                    raise ValueError("Duplicate object reference")
                keys.add(part.object.key)
                end = part.stop
            size = result.row_count if result.kind == "table" else result.data_size
            if result.kind == "map_node":
                size = 0
            if end != size or (result.kind == "map_node" and result.parts):
                raise ValueError("Parts do not cover the node")
            if result.kind == DOCUMENT_KIND and (
                len(result.parts) != 1 or result.data_size == 0 or result.row_count != 0
            ):
                raise ValueError("A document must contain exactly one YSON node part")
            return result
        except (KeyError, TypeError, AttributeError, json.JSONDecodeError) as exc:
            raise ValueError("Invalid archive manifest") from exc


def canonical_attributes(attributes: dict) -> bytes:
    """Serialize attributes as canonical binary YSON."""
    return yson.dumps(object=attributes, yson_format="binary", sort_keys=True)


def attributes_hash(data: bytes) -> str:
    """Compute the SHA-256 digest of serialized bytes."""
    return hashlib.sha256(data).hexdigest()


def verify_object(*, data: bytes, ref: ObjectRef) -> None:
    """Check object bytes against their size and checksum."""
    if len(data) != ref.size or attributes_hash(data) != ref.sha256:
        raise ValueError(f"Object checksum or size mismatch: {ref.key}")


def plan_parts(*, node: Node, part_size: int) -> Iterator[Range]:
    """Partition table rows or file bytes into contiguous ranges."""
    if part_size <= 0:
        raise ValueError("Part size must be positive")
    if node.kind == "table":
        size = node.row_count
        step = max(1, part_size * size // max(1, node.data_weight))
    elif node.kind == "file":
        size, step = node.data_size, part_size
    elif node.kind == DOCUMENT_KIND:
        if node.data_size <= 0:
            raise ValueError("A document must contain a serialized YSON value")
        yield Range(index=0, start=0, stop=node.data_size)
        return
    else:
        return
    for index, start in enumerate(range(0, size, step)):
        yield Range(index=index, start=start, stop=min(size, start + step))


def _fingerprint(
    *,
    kind: str,
    node_id: str,
    content_revision: int | None,
    attributes_digest: str,
    cluster: str,
    compression_level: int,
    part_size: int,
    table_format: dict | None = None,
    encryption: dict | None = None,
) -> str:
    """Hash source identity, the saved attribute digest and encoding settings."""
    settings = {
        "format": FORMAT,
        "encoding": node_encoding(kind, table_format),
        "cluster": cluster.rstrip("/"),
        "kind": kind,
        "node_id": node_id,
        "content_revision": content_revision if kind != "map_node" else None,
        "attributes": attributes_digest,
        "compression_level": compression_level,
        "part_size": part_size,
    }
    if encryption is not None:
        settings["encryption"] = validate_encryption(encryption)
    if kind == "table":
        settings["table_format"] = (
            make_table_format("yson", b"")
            if table_format is None
            else validate_table_format(table_format)
        )
        settings["table_compression"] = "yt-zstd-passthrough"
    return attributes_hash(
        json.dumps(
            settings,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    )


def make_fingerprint(
    *,
    node: Node,
    cluster: str,
    compression_level: int,
    part_size: int,
    table_format: str = "yson",
    encryption: dict | None = None,
) -> str:
    """Identify source state and archive encoding settings."""
    selected = None
    if node.kind == "table":
        selected = (
            make_table_format(table_format, node.attributes)
            if node.table_format is None
            else validate_table_format(node.table_format)
        )
        if selected["name"] != table_format:
            raise ValueError("Snapshot format does not match requested table format")
    return _fingerprint(
        kind=node.kind,
        node_id=node.node_id,
        content_revision=node.content_revision,
        attributes_digest=attributes_hash(node.attributes),
        cluster=cluster,
        compression_level=compression_level,
        part_size=part_size,
        table_format=selected,
        encryption=encryption,
    )


def matches_discovery(
    *,
    manifest: Manifest,
    node: DiscoveredNode,
    cluster: str,
    compression_level: int,
    part_size: int,
    table_format: str = "yson",
    encryption: dict | None = None,
) -> bool:
    """Compare discovery revisions and copy settings without reading attributes."""
    source = manifest.source
    if manifest.encryption != encryption:
        return False
    if (
        not isinstance(source, dict)
        or not isinstance(node.kind, str)
        or node.kind not in KINDS
    ):
        return False
    if not all(isinstance(value, str) and value for value in (node.path, node.node_id)):
        return False
    if manifest.kind != node.kind or source.get("path") != node.path:
        return False
    if source.get("node_id") != node.node_id:
        return False
    saved_cluster = source.get("cluster")
    if not isinstance(saved_cluster, str) or saved_cluster.rstrip(
        "/"
    ) != cluster.rstrip("/"):
        return False

    revision = source.get("attribute_revision")
    if not _integer(node.attribute_revision) or not _integer(revision):
        return False
    if node.attribute_revision != revision:
        return False
    if node.kind == "table" and node.dynamic is not False:
        return False
    if (
        node.kind == "table"
        and table_format_name(manifest.table_format) != table_format
    ):
        return False
    if node.kind != "map_node":
        revision = source.get("content_revision")
        if not _integer(node.content_revision) or not _integer(revision):
            return False
        if node.content_revision != revision:
            return False

    return manifest.fingerprint == _fingerprint(
        kind=node.kind,
        node_id=node.node_id,
        content_revision=node.content_revision,
        attributes_digest=(
            manifest.attributes.plaintext_sha256
            if encryption is not None
            else manifest.attributes.sha256
        ),
        cluster=cluster,
        compression_level=compression_level,
        part_size=part_size,
        table_format=manifest.table_format if node.kind == "table" else None,
        encryption=encryption,
    )


def path_components(path: str) -> list[str]:
    """Validate and split an absolute YT path."""
    if not path.startswith("//") or path == "//":
        raise ValueError("Expected an absolute non-root YT path")
    components = path[2:].split("/")
    if any(part in ("", ".", "..") for part in components):
        raise ValueError("Invalid YT path")
    return components


def node_prefix(*, prefix: str, path: str) -> str:
    """Map a full YT path beneath an S3 prefix."""
    return "/".join(
        filter(
            None,
            (prefix.strip("/"), *(quote(p, safe="") for p in path_components(path))),
        )
    )


def object_key(*, prefix: str, name: str) -> str:
    """Join an S3 prefix and object name."""
    return prefix.rstrip("/") + "/" + name if prefix else name


def generation_prefix(*, prefix: str, generation: str) -> str:
    """Build the isolated object prefix for one immutable node generation."""
    _validate_generation(generation)
    return object_key(prefix=prefix, name=f"@generations/{generation}")


def attributes_key(*, prefix: str, encrypted: bool = False) -> str:
    """Build the attribute object key, marking encrypted bytes explicitly."""
    suffix = ENCRYPTED_SUFFIX if encrypted else ""
    return object_key(prefix=prefix, name=ATTRIBUTES_NAME + suffix)


def data_key(
    *,
    prefix: str,
    kind: str,
    index: int,
    table_format: str = "yson",
    encrypted: bool = False,
) -> str:
    """Build the object key for an ordered data part."""
    if kind == "table":
        if table_format not in TABLE_FORMATS:
            raise ValueError("Unsupported table format")
        extension = table_format
    else:
        extension = "yson" if kind == DOCUMENT_KIND else "bin"
    suffix = ENCRYPTED_SUFFIX if encrypted else ""
    return object_key(
        prefix=prefix, name=f"@data/part-{index:06d}.{extension}.zst{suffix}"
    )


def validate_manifest_location(*, manifest: Manifest, prefix: str) -> None:
    """Require the exact ordered object keys in the declared node generation."""
    prefix = generation_prefix(prefix=prefix, generation=manifest.generation)
    # Early encrypted archives used the plaintext filenames. Keep those readable,
    # but require one consistent naming scheme within each generation.
    encrypted_names = manifest.encryption is not None and manifest.attributes.key == (
        attributes_key(prefix=prefix, encrypted=True)
    )
    if manifest.attributes.key != attributes_key(
        prefix=prefix, encrypted=encrypted_names
    ):
        raise ValueError("Attributes reference escapes selected generation")
    for index, part in enumerate(manifest.parts):
        if part.object.key != data_key(
            prefix=prefix,
            kind=manifest.kind,
            index=index,
            table_format=table_format_name(manifest.table_format),
            encrypted=encrypted_names,
        ):
            raise ValueError(
                "Data reference escapes selected generation or has invalid ordering"
            )


def restore_path(*, source_prefix: str, node: str, destination: str) -> str:
    """Map an archived node to its destination YT path."""
    path_components(destination)
    if node == source_prefix:
        return destination
    base = source_prefix.rstrip("/") + "/" if source_prefix else ""
    if not node.startswith(base):
        raise ValueError("Node escapes source prefix")
    decoded = []
    for encoded in node[len(base) :].split("/"):
        part = unquote(encoded, errors="strict")
        if not part or part in (".", "..") or "/" in part or "\\" in part:
            raise ValueError("Invalid encoded node name")
        if quote(part, safe="") != encoded:
            raise ValueError("Noncanonical encoded node name")
        # Escape Cypress metacharacters in literal child names.
        decoded.append("".join("\\" + c if c in "@&*[]{}<>" else c for c in part))
    return destination.rstrip("/") + "/" + "/".join(decoded)


def parse_s3_url(url: str) -> tuple[str, str]:
    """Validate an S3 URL and extract its bucket and prefix."""
    value = urlsplit(url)
    if (
        value.scheme != "s3"
        or not value.netloc
        or value.query
        or value.fragment
        or any(c in value.netloc for c in ":@")
    ):
        raise ValueError("Expected s3://bucket/prefix")
    prefix = value.path.strip("/")
    if any(p in (".", "..", "") for p in prefix.split("/")) and prefix:
        raise ValueError("Invalid S3 prefix")
    return value.netloc, prefix
