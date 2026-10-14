"""YTsaurus adapter: all client operations stay on one thread per session."""

import asyncio
import contextlib
import copy
import functools
import hashlib
import logging
import os
from collections.abc import AsyncIterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import TYPE_CHECKING

import yt.wrapper as yt
import zstandard
from yt import yson
from yt.common import YT_NULL_TRANSACTION_ID
from yt.wrapper.driver import make_request
from yt.wrapper.errors import YtChunkUnavailable, YtError
from yt.wrapper.http_helpers import get_retriable_errors
from yt.wrapper.ypath import escape_ypath_literal

from .codec import AsyncReader
from .format import (
    MIB,
    RESTORE_MARKER,
    ROW_FORMAT,
    DiscoveredNode,
    Node,
    Range,
    SourceEstimate,
    attributes_hash,
    canonical_attributes,
)
from .interfaces import (
    RestoreSink,
    RetryableDownloadError,
    RetryableReadError,
    RetryableRestoreError,
    SourceSnapshot,
    YtBackend,
    YtWorkerFactory,
)

if TYPE_CHECKING:
    from .progress import Progress

log = logging.getLogger(__name__)

WIRE = yt.YsonFormat(attributes={"format": "binary"})
PORTABLE_SYSTEM = {
    "table": (b"schema", b"compression_codec", b"erasure_codec", b"optimize_for"),
    "file": (b"compression_codec", b"erasure_codec", b"file_name", b"executable"),
    "map_node": (),
}
EXCLUDED = {
    b"acl",
    b"inherit_acl",
    b"owner",
    b"account",
    b"expiration_time",
    b"expiration_timeout",
    b"primary_medium",
    b"media",
    b"replication_factor",
    RESTORE_MARKER.encode(),
}
DISCOVERY_ATTRIBUTES = (
    b"id",
    b"type",
    b"dynamic",
    b"content_revision",
    b"attribute_revision",
    b"resource_usage",
    b"data_weight",
    b"uncompressed_data_size",
)
LIST_LIMIT = 1_000_000
ESTIMATE_REQUEST_TIMEOUT_MS = 5_000


def path_components(path: str) -> list[str]:
    """Validate and split a supported absolute YT path."""
    if not path.startswith("//") or any(char in path for char in "\\[]{}<>&*@"):
        raise ValueError("Use an absolute YT path without selectors or special names")
    components = path[2:].split("/")
    if any(component in ("", ".", "..") for component in components):
        raise ValueError("Invalid YT path")
    return components


def _text(value) -> str:
    """Decode YSON bytes or stringify a metadata value."""
    return value.decode("utf-8") if isinstance(value, bytes) else str(value)


def _attribute_path(*, path: str, name: bytes) -> str:
    """Build an escaped path to one node attribute."""
    return path + "/@" + escape_ypath_literal(name).decode("ascii")


def _attributes(*, client, path: str, names: list[bytes]) -> dict:
    """Fetch selected attributes without following the node itself."""
    data = client.get(
        path=path + "&",
        attributes=[_text(name) for name in names],
        max_size=0,
        format=WIRE,
    )
    return yson.loads(string=data, encoding=None).attributes


def _resolve(*, client, path: str, create_parents: bool = False) -> str:
    """Resolve through parent IDs, never through a link in any path component."""
    components = path_components(path)
    parent = "/"
    for index, component in enumerate(components):
        current = parent + "/" + component if parent != "/" else "//" + component
        if create_parents:
            client.create(type="map_node", path=current, ignore_existing=True)

        attrs = _attributes(client=client, path=current, names=[b"id", b"type"])
        kind = _text(attrs[b"type"])
        if (index < len(components) - 1 or create_parents) and kind != "map_node":
            raise ValueError(
                f"Ancestor is not a directory: {'//' + '/'.join(components[: index + 1])}"
            )
        parent = "#" + _text(attrs[b"id"])
    return parent


def _list_children(*, client, path: str, pinned: str) -> list[DiscoveredNode]:
    """List child identities and change hints in one non-following request."""
    data = client.list(
        path=pinned + "&",
        attributes=[_text(name) for name in DISCOVERY_ATTRIBUTES],
        max_size=LIST_LIMIT,
        format=WIRE,
    )
    names = yson.loads(string=data, encoding=None)
    if not isinstance(names, list):
        raise TypeError(f"Invalid directory listing: {path}")
    if names.attributes.get(b"incomplete") or names.attributes.get("incomplete"):
        raise ValueError(f"Directory listing incomplete: {path}")

    children = []
    for name in names:
        component = _text(name)
        if "/" in component:
            raise ValueError(f"Directory contains unsupported special names: {path}")
        child_path = path + "/" + component
        path_components(child_path)

        children.append(
            _discovered_node(path=child_path, attrs=getattr(name, "attributes", {}))
        )
    return children


def _optional_integer(*, value: object, minimum: int = 0) -> int | None:
    """Read an optional integer, excluding booleans and malformed values."""
    if (
        isinstance(value, int)
        and not isinstance(value, (bool, yson.YsonBoolean))
        and value >= minimum
    ):
        return int(value)
    return None


def _usage_integer(*, usage: object, key: str | bytes, minimum: int = 0) -> int | None:
    """Read an optional count from a resource usage dictionary."""
    if not isinstance(usage, dict):
        return None
    return _optional_integer(value=usage.get(key), minimum=minimum)


def _discovered_node(*, path: str, attrs: dict) -> DiscoveredNode:
    """Retain listing hints without trusting absent or malformed optional fields."""
    node_id = attrs.get(b"id")
    kind = attrs.get(b"type")
    if any(
        not isinstance(value, (bytes, str)) or not value for value in (node_id, kind)
    ):
        raise ValueError(f"Discovery omitted node id or type: {path}")
    kind = _text(kind)
    dynamic = attrs.get(b"dynamic")
    if kind in ("file", "map_node"):
        dynamic = False
    elif isinstance(dynamic, (bool, yson.YsonBoolean)):
        dynamic = bool(dynamic)
    else:
        dynamic = None

    estimated_size = None
    if kind == "map_node":
        estimated_size = 0
    elif kind == "table":
        estimated_size = _optional_integer(value=attrs.get(b"data_weight"))
    elif kind == "file":
        estimated_size = _optional_integer(value=attrs.get(b"uncompressed_data_size"))

    return DiscoveredNode(
        path=path,
        kind=kind,
        node_id=_text(node_id),
        content_revision=_optional_integer(value=attrs.get(b"content_revision")),
        attribute_revision=_optional_integer(value=attrs.get(b"attribute_revision")),
        dynamic=dynamic,
        disk_size=(
            0
            if kind == "map_node"
            else _usage_integer(usage=attrs.get(b"resource_usage"), key=b"disk_space")
        ),
        estimated_size=estimated_size,
    )


def _read_node(*, client, pinned: str, path: str) -> Node:
    """Read portable metadata and state for one resolved node."""
    attrs = _attributes(
        client=client,
        path=pinned,
        names=[b"id", b"type", b"dynamic", b"user_attribute_keys", b"resource_usage"],
    )
    kind = _text(attrs[b"type"])
    dynamic = bool(attrs.get(b"dynamic", False))
    disk_size = (
        0
        if kind == "map_node"
        else _usage_integer(usage=attrs.get(b"resource_usage"), key=b"disk_space")
    )
    if dynamic:
        return Node(
            path=path,
            kind=kind,
            node_id=_text(attrs[b"id"]),
            content_revision=0,
            attributes=canonical_attributes({}),
            dynamic=True,
            disk_size=disk_size,
        )
    user_keys = attrs.get(b"user_attribute_keys", ())
    if getattr(user_keys, "attributes", {}).get(b"incomplete") or getattr(
        user_keys, "attributes", {}
    ).get("incomplete"):
        raise ValueError("Incomplete user attribute listing")
    custom = set(user_keys) - EXCLUDED
    names = custom | set(PORTABLE_SYSTEM.get(kind, ()))
    metadata = _attributes(
        client=client,
        path=pinned,
        names=list(
            names
            | {
                b"content_revision",
                b"attribute_revision",
                b"row_count",
                b"uncompressed_data_size",
                b"data_weight",
            }
        ),
    )
    required = set(custom)
    if kind == "table" and not dynamic:
        required.update({b"schema", b"row_count", b"content_revision", b"data_weight"})
    elif kind == "file":
        required.update({b"uncompressed_data_size", b"content_revision"})
    missing = required - metadata.keys()
    if missing:
        raise ValueError(f"Missing required YT attributes: {sorted(missing)!r}")
    portable = {name: metadata[name] for name in names if name in metadata}
    return Node(
        path=path,
        kind=kind,
        node_id=_text(attrs[b"id"]),
        content_revision=int(metadata.get(b"content_revision", 0)),
        attributes=canonical_attributes(portable),
        row_count=int(metadata.get(b"row_count", 0)),
        data_size=int(metadata.get(b"uncompressed_data_size", 0)),
        data_weight=int(metadata.get(b"data_weight", 0)),
        dynamic=dynamic,
        disk_size=disk_size,
        attribute_revision=_optional_integer(value=metadata.get(b"attribute_revision")),
    )


def _chunks_digest(*, client, pinned: str) -> str:
    """Hash ordered chunk IDs to detect content changes at commit."""
    attrs = _attributes(client=client, path=pinned, names=[b"chunk_ids"])
    if b"chunk_ids" not in attrs:
        raise ValueError("Cannot verify committed content without chunk_ids")
    digest = hashlib.sha256()
    for chunk_id in attrs[b"chunk_ids"]:
        digest.update(_text(chunk_id).encode("ascii") + b"\0")
    return digest.hexdigest()


def _read_table(*, client, pinned: str, part: Range, transaction_id: str):
    """Open raw YSON bytes in the caller's snapshot without SDK row parsing."""
    if not transaction_id or transaction_id == YT_NULL_TRANSACTION_ID:
        raise ValueError("A live snapshot transaction is required for table reads")

    return make_request(
        command_name="read_table",
        params={
            "transaction_id": transaction_id,
            "path": yt.TablePath(
                name=pinned, start_index=part.start, end_index=part.stop
            ),
            "output_format": yt.YsonFormat(attributes=ROW_FORMAT),
            "unordered": False,
            "control_attributes": {
                "enable_row_index": False,
                "enable_table_index": False,
                "enable_range_index": False,
            },
            "omit_inaccessible_columns": False,
            "omit_inaccessible_rows": False,
        },
        return_content=False,
        use_heavy_proxy=True,
        allow_retries=False,
        client=client,
    )


def _is_retryable_read(*, error: Exception) -> bool:
    """Recognize transient SDK reads while keeping snapshot and access errors fatal."""
    if isinstance(error, YtError) and (
        error.is_no_such_transaction()
        or error.is_authentication_error()
        or error.is_access_denied()
        or error.is_resolve_error()
    ):
        return False
    status = getattr(getattr(error, "response", None), "status_code", None)
    if status is not None and 400 <= status < 500 and status not in (408, 429):
        return False
    return isinstance(error, (*get_retriable_errors(), YtChunkUnavailable))


class _Session:
    def __init__(self, *, adapter):
        """Reserve one worker thread for a client and its transaction."""
        self.adapter = adapter
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="ytclone")
        self.client = None
        self.transaction = None
        self.poisoned = False

    async def call(self, function, /, *, operation="yt_metadata", **kwargs):
        """Measure a logical SDK operation, including retries and cancellation drain."""
        progress = self.adapter.progress
        measurement = (
            progress.measure(operation=operation)
            if progress is not None and operation is not None
            else contextlib.nullcontext()
        )
        with measurement:
            future = asyncio.get_running_loop().run_in_executor(
                executor=self.executor, func=functools.partial(function, **kwargs)
            )
            try:
                return await asyncio.shield(future)
            except asyncio.CancelledError:
                # Cleanup must not race a still running SDK request on this client.
                with contextlib.suppress(BaseException):
                    await asyncio.shield(future)
                raise

    def open(self):
        """Create a client configured for sequential streaming."""
        self.client = self.adapter.client_factory(
            proxy=self.adapter.proxy,
            config={
                # YtClient does not inherit these environment shortcuts itself.
                "token": os.environ.get("YT_TOKEN") or None,
                "token_path": os.environ.get("YT_TOKEN_PATH") or None,
                "ping_failed_mode": "pass",
                # File reads use SDK retries; tables retry whole archive parts.
                "read_retries": {
                    "enable": True,
                    "create_transaction_and_take_snapshot_lock": False,
                },
                "read_parallel": {"enable": False},
                "write_parallel": {"enable": False},
                "write_retries": {"enable": False},
                "proxy": {"enable_proxy_discovery": self.adapter.proxy_discovery},
            },
        )

    def ready(self):
        """Replace a client whose previous transaction cleanup is uncertain."""
        if self.poisoned:
            self.dispose()
            self.open()
            self.poisoned = False
        if self.transaction is not None:
            raise RuntimeError("YT worker already has an active transaction")

    def begin(self):
        """Open a self-pinging transaction and track partial initialization."""
        try:
            self.transaction = self.client.Transaction(
                timeout=600_000, interrupt_on_failed=False
            )
            self.transaction.__enter__()
        except BaseException:
            self.poisoned = True
            raise

    def check(self):
        """Reject a transaction whose keepalive has failed."""
        if not self.transaction.is_pinger_alive():
            raise RuntimeError("YT transaction ping failed")

    def end(self, *, error=None):
        """Commit successful work or abort after an error."""
        transaction = self.transaction
        if transaction is None:
            return
        try:
            if error is None:
                transaction.__exit__(None, None, None)
            else:
                transaction.__exit__(type(error), error, error.__traceback__)
        except BaseException:
            self.poisoned = True
            raise
        else:
            self.transaction = None

    def dispose(self):
        """Abort unfinished work and close the client HTTP pools."""
        try:
            self.end(error=InterruptedError("YT session closed before completion"))
        finally:
            if self.client is not None:
                # SDK 0.13 has no public close(); its destructor calls _cleanup.
                # Transaction cleanup must precede closing the shared HTTP pool.
                self.client._cleanup()
                self.client = None

    async def close(self):
        """Release client resources and stop the worker thread."""
        try:
            await self.call(self.dispose, operation=None)
        finally:
            await asyncio.to_thread(self.executor.shutdown, wait=True)


@dataclass
class _ReadStream:
    raw: object
    compressed: object

    def close(self):
        """Close both the compression stream and its YT input."""
        try:
            self.compressed.close()
        finally:
            self.raw.close()


class Snapshot(SourceSnapshot):
    def __init__(self, *, session: _Session, node: Node):
        """Bind a pinned node to its reading session."""
        self.session = session
        self.node = node
        self.pinned = "#" + node.node_id
        self._stream = None
        self._reading = False

    def _open_part(self, *, part: Range, compression_level: int):
        """Open and compress one table-row or file-byte range."""
        if self._stream is not None:
            raise RuntimeError("Read one part at a time within a snapshot")
        self.session.check()
        client = self.session.client
        if self.node.kind == "table":
            raw = _read_table(
                client=client,
                pinned=self.pinned,
                part=part,
                transaction_id=self.session.transaction.transaction_id,
            )
        elif self.node.kind == "file":
            raw = client.read_file(
                path=self.pinned,
                offset=part.start,
                length=part.stop - part.start,
                enable_read_parallel=False,
            )
        else:
            raise ValueError(f"Cannot read data of {self.node.kind}")

        try:
            compressed = zstandard.ZstdCompressor(
                level=compression_level
            ).stream_reader(source=raw, read_size=MIB, closefd=False)
            self._stream = _ReadStream(raw=raw, compressed=compressed)
            return self._stream
        except BaseException:
            try:
                raw.close()
            except BaseException:
                self.session.poisoned = True
                log.exception("read_cleanup_failed path=%s", self.node.path)
            raise

    async def _read(self, function, /, **kwargs):
        """Translate only source-call failures into whole-part retry signals."""
        try:
            return await self.session.call(function, operation="yt_read", **kwargs)
        except Exception as error:
            if (
                self.node.kind == "table"
                and not self.session.poisoned
                and _is_retryable_read(error=error)
            ):
                raise RetryableReadError(str(error)) from error
            raise

    def _close_part(self):
        """Release the currently open data part."""
        stream, self._stream = self._stream, None
        if stream is not None:
            stream.close()

    async def read_part(
        self, *, part: Range, compression_level: int, chunk_size: int
    ) -> AsyncIterator[bytes]:
        """Stream a compressed part with one chunk of prefetch."""
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        if self._reading:
            raise RuntimeError("Read one part at a time within a snapshot")
        self._reading = True
        pending = None
        error = None
        try:
            stream = await self._read(
                self._open_part,
                part=part,
                compression_level=compression_level,
            )
            chunk = await self._read(stream.compressed.read, size=chunk_size)
            while chunk:
                # One extra chunk overlaps YT reading/compression with S3 upload.
                pending = asyncio.create_task(
                    self._read(stream.compressed.read, size=chunk_size)
                )
                yield chunk
                chunk = await pending
                pending = None
        except BaseException as caught:
            error = caught
            raise
        finally:
            if pending is not None:
                pending.cancel()
                with contextlib.suppress(BaseException):
                    await pending
            try:
                await self.session.call(self._close_part, operation=None)
            except BaseException as cleanup_error:
                self.session.poisoned = True
                if error is None:
                    raise
                log.exception("read_cleanup_failed path=%s", self.node.path)
                if isinstance(error, RetryableReadError):
                    # Preserve the source failure, but forbid retries after unsafe cleanup.
                    raise error.__cause__ from cleanup_error
            finally:
                self._reading = False


class Restore(RestoreSink):
    def __init__(self, *, session: _Session, path: str, node: Node, fingerprint: str):
        """Track one transactional node replacement and its verification state."""
        self.session = session
        self.path = path
        self.node = node
        self.fingerprint = fingerprint
        self.pinned = None
        self.part_count = 0
        self.byte_count = 0
        self.committed_chunks = None
        self.expected_attributes = None

    def prepare(self):
        """Create a replacement node or update a directory safely."""
        client = self.session.client
        components = path_components(self.path)
        if len(components) == 1:
            target = self.path
        else:
            parent = _resolve(
                client=client,
                path="//" + "/".join(components[:-1]),
                create_parents=True,
            )
            target = parent + "/" + components[-1]

        attrs = yson.loads(string=self.node.attributes, encoding=None)
        forbidden = set(attrs) & EXCLUDED
        if forbidden:
            raise ValueError(f"Non-portable restore attributes: {sorted(forbidden)!r}")
        if self.node.kind not in PORTABLE_SYSTEM or self.node.dynamic:
            raise ValueError(f"Unsupported restore node: {self.node.kind}")

        if client.exists(path=target + "&"):
            previous = _read_node(client=client, pinned=target, path=self.path)
            if previous.kind == "link" or previous.dynamic:
                raise ValueError("Refusing to replace a link or dynamic table")
            if (previous.kind == "map_node") != (self.node.kind == "map_node"):
                raise ValueError(
                    "Refusing to replace a directory with data or vice versa"
                )
            if previous.kind not in PORTABLE_SYSTEM:
                raise ValueError(f"Unsupported existing node: {previous.kind}")
            client.lock(path="#" + previous.node_id, mode="exclusive")
            if self.node.kind == "map_node":
                self.pinned = "#" + previous.node_id
                previous_attrs = yson.loads(string=previous.attributes, encoding=None)
                for name in previous_attrs.keys() - attrs.keys():
                    client.remove(path=_attribute_path(path=self.pinned, name=name))
                for name, value in attrs.items():
                    client.set(
                        path=_attribute_path(path=self.pinned, name=name), value=value
                    )
                marker_path = _attribute_path(
                    path=self.pinned, name=RESTORE_MARKER.encode()
                )
                if client.exists(path=marker_path):
                    client.remove(path=marker_path)
                return
            client.remove(path=target + "&")

        self.pinned = "#" + _text(
            client.create(type=self.node.kind, path=target, attributes=attrs)
        )

    def _write_part(self, *, source: AsyncReader):
        """Decompress and append one fully validated input part."""
        self.session.check()
        with zstandard.ZstdDecompressor().stream_reader(
            source=source, read_size=MIB, closefd=False
        ) as decompressed:

            def chunks():
                """Yield bounded decompressed chunks and count their bytes."""
                while chunk := decompressed.read(size=MIB):
                    self.byte_count += len(chunk)
                    yield chunk

            if self.node.kind == "table":
                self.session.client.write_table(
                    table=yt.TablePath(self.pinned, append=self.part_count > 0),
                    input_stream=chunks(),
                    format=yt.YsonFormat(attributes=ROW_FORMAT),
                    raw=True,
                    force_create=False,
                )
            elif self.node.kind == "file":
                self.session.client.write_file(
                    destination=yt.FilePath(self.pinned, append=self.part_count > 0),
                    stream=chunks(),
                    force_create=False,
                )
            else:
                raise ValueError("Directories cannot have data parts")
            # A SDK implementation returning before consuming the body must not
            # publish data while an S3 checksum error still waits at iterator EOF.
            if decompressed.read(size=1):
                raise ValueError("YT writer did not consume the complete data part")
        source.finish()
        self.part_count += 1

    async def write_part(self, *, chunks: AsyncIterator[bytes]) -> None:
        """Bridge async input to the writer and propagate cancellation."""
        source = AsyncReader(chunks=chunks, loop=asyncio.get_running_loop())
        task = asyncio.create_task(
            self.session.call(self._write_part, operation="yt_write", source=source)
        )
        try:
            await asyncio.shield(task)
        except BaseException:
            source.cancel()
            with contextlib.suppress(BaseException):
                await asyncio.shield(task)
            raise
        finally:
            close = getattr(chunks, "aclose", None)
            if close is not None:
                await close()

    def validate(self):
        """Verify restored counts and metadata before commit."""
        self.session.check()
        current = _read_node(
            client=self.session.client, pinned=self.pinned, path=self.path
        )
        if self.node.kind == "table" and current.row_count != self.node.row_count:
            raise ValueError(
                f"Restored row count mismatch: {current.row_count} != {self.node.row_count}"
            )
        if self.node.kind == "file" and current.data_size != self.node.data_size:
            raise ValueError(
                f"Restored file size mismatch: {current.data_size} != {self.node.data_size}"
            )
        if self.node.kind in ("table", "file"):
            self.committed_chunks = _chunks_digest(
                client=self.session.client, pinned=self.pinned
            )
        self.expected_attributes = attributes_hash(current.attributes)
        if self.expected_attributes != attributes_hash(self.node.attributes):
            raise ValueError("Restored attributes differ from source attributes")

    def mark(self):
        """Record completion only if committed content remains unchanged."""
        client = self.session.client
        resolved = _resolve(client=client, path=self.path)
        if resolved != self.pinned:
            raise ValueError("Destination was replaced after restore commit")
        current = _read_node(client=client, pinned=self.pinned, path=self.path)
        if attributes_hash(current.attributes) != self.expected_attributes:
            raise ValueError("Destination attributes changed after restore commit")
        if self.node.kind in ("table", "file") and (
            _chunks_digest(client=client, pinned=self.pinned) != self.committed_chunks
        ):
            raise ValueError("Destination content changed after restore commit")
        client.set(
            path=_attribute_path(path=self.pinned, name=RESTORE_MARKER.encode()),
            value={
                "fingerprint": self.fingerprint,
                "node_id": current.node_id,
                "content_revision": current.content_revision,
                "attributes_hash": self.expected_attributes,
            },
        )


class YtAdapter(YtWorkerFactory):
    def __init__(
        self,
        *,
        proxy: str,
        proxy_discovery: bool = True,
        client_factory=None,
        progress: "Progress | None" = None,
    ):
        """Configure persistent workers for one cluster."""
        self.proxy = proxy
        self.proxy_discovery = proxy_discovery
        self.client_factory = client_factory or yt.YtClient
        self.progress = progress

    @contextlib.asynccontextmanager
    async def worker(self) -> AsyncIterator["YtWorker"]:
        """Keep one SDK client and thread alive for a sequence of node operations."""
        session = _Session(adapter=self)
        try:
            await session.call(session.open, operation=None)
            yield YtWorker(session=session)
        finally:
            await session.close()


class YtWorker(YtBackend):
    def __init__(self, *, session: _Session):
        """Bind sequential node operations to a persistent client session."""
        self.session = session

    @contextlib.asynccontextmanager
    async def _transaction(self) -> AsyncIterator[_Session]:
        """Isolate one node transaction, including partial begin failures."""
        session = self.session
        await session.call(session.ready, operation=None)
        try:
            await session.call(session.begin)
            yield session
            await session.call(session.check, operation=None)
        except BaseException as error:
            await session.call(session.end, error=error)
            raise
        else:
            await session.call(session.end)

    async def estimate_source(self, *, path: str) -> SourceEstimate:
        """Read optional subtree resources with bounded requests and no scanning."""
        session = self.session

        def estimate():
            """Temporarily bound this client's metadata requests without shared mutation."""
            client = session.client
            original = client.config
            bounded = copy.deepcopy(original)
            bounded["proxy"].update(
                {
                    "connect_timeout": ESTIMATE_REQUEST_TIMEOUT_MS,
                    "request_timeout": ESTIMATE_REQUEST_TIMEOUT_MS,
                    "retries": {
                        **bounded["proxy"].get("retries", {}),
                        "enable": False,
                    },
                }
            )
            client.config = bounded
            try:
                # Each path lookup is bounded; this is not a whole-operation deadline.
                pinned = _resolve(client=client, path=path)
                usage = client.get(path=pinned + "&/@recursive_resource_usage")
                return SourceEstimate(
                    node_count=_usage_integer(usage=usage, key="node_count", minimum=1),
                    disk_size=_usage_integer(usage=usage, key="disk_space"),
                )
            finally:
                client.config = original

        try:
            await session.call(session.ready, operation=None)
            return await session.call(estimate)
        except Exception:
            log.debug("source_estimate_unavailable path=%s", path, exc_info=True)
            return SourceEstimate()

    async def inspect_node(self, *, path: str) -> DiscoveredNode:
        """Validate ancestry and read one node's identity and size hints."""
        path_components(path)
        session = self.session
        await session.call(session.ready, operation=None)

        def inspect():
            """Validate source ancestry and read the root's change hints."""
            pinned = _resolve(client=session.client, path=path)
            attrs = _attributes(
                client=session.client, path=pinned, names=list(DISCOVERY_ATTRIBUTES)
            )
            return _discovered_node(path=path, attrs=attrs)

        return await session.call(inspect)

    async def list_children(self, *, node: DiscoveredNode) -> list[DiscoveredNode]:
        """List the complete directory by its discovered ID, never through a link."""
        if node.kind != "map_node":
            raise ValueError(f"Expected a discovered directory: {node.path}")

        session = self.session
        await session.call(session.ready, operation=None)
        return await session.call(
            _list_children,
            client=session.client,
            path=node.path,
            pinned="#" + node.node_id,
        )

    async def walk(self, *, path: str) -> AsyncIterator[DiscoveredNode]:
        """Resolve the root once, then list directories by ID without following links."""
        stack = [iter([await self.inspect_node(path=path)])]
        while stack:
            entry = next(stack[-1], None)
            if entry is None:
                stack.pop()
                continue
            yield entry
            if entry.kind != "map_node":
                continue

            children = await self.list_children(node=entry)
            if children:
                stack.append(iter(children))

    async def ensure_directory(self, *, path: str) -> None:
        """Create missing directories while preserving existing metadata."""
        async with self._transaction() as session:
            pinned = await session.call(
                _resolve, client=session.client, path=path, create_parents=True
            )
            attrs = await session.call(
                _attributes, client=session.client, path=pinned, names=[b"type"]
            )
            if attrs[b"type"] != b"map_node":
                raise ValueError("Expected a directory")

    @contextlib.asynccontextmanager
    async def snapshot(self, *, path: str) -> AsyncIterator[Snapshot]:
        """Pin a node for consistent metadata and data reads."""
        async with self._transaction() as session:

            def pin():
                """Lock a resolved node and read its snapshot metadata."""
                resolved = _resolve(client=session.client, path=path)
                node_id = session.client.lock(path=resolved + "&", mode="snapshot")[
                    "node_id"
                ]
                return _read_node(
                    client=session.client, pinned="#" + _text(node_id), path=path
                )

            node = await session.call(pin)
            yield Snapshot(session=session, node=node)

    async def matches(self, *, path: str, fingerprint: str) -> bool:
        """Check whether a completed restore still matches its fingerprint."""
        session = self.session
        await session.call(session.ready, operation=None)

        def check():
            """Compare the completion marker with the current node state."""
            try:
                pinned = _resolve(client=session.client, path=path)
            except yt.YtResponseError as error:
                if error.is_resolve_error():
                    return False
                raise
            current = _read_node(client=session.client, pinned=pinned, path=path)
            if current.kind not in PORTABLE_SYSTEM or current.dynamic:
                return False
            data = _attributes(
                client=session.client, path=pinned, names=[RESTORE_MARKER.encode()]
            )
            marker = data.get(RESTORE_MARKER.encode())
            if not isinstance(marker, dict):
                return False
            return (
                marker.get(b"fingerprint") == fingerprint.encode()
                and marker.get(b"node_id") == current.node_id.encode()
                and marker.get(b"attributes_hash")
                == attributes_hash(current.attributes).encode()
                and (
                    current.kind == "map_node"
                    or marker.get(b"content_revision") == current.content_revision
                )
            )

        return await session.call(check)

    @contextlib.asynccontextmanager
    async def restore(
        self, *, path: str, node: Node, fingerprint: str
    ) -> AsyncIterator[Restore]:
        """Restore a node atomically, then record verified completion."""
        path_components(path)
        source_error = None
        try:
            async with self._transaction() as session:
                sink = Restore(
                    session=session, path=path, node=node, fingerprint=fingerprint
                )
                try:
                    await session.call(sink.prepare)
                    yield sink
                    await session.call(sink.validate)
                except BaseException as error:
                    source_error = error
                    raise
        except BaseException as error:
            if source_error is not None and error is not source_error:
                # Keep the original failure, but never retry an uncertain abort.
                raise source_error from error
            if (
                error is source_error
                and not session.poisoned
                and isinstance(error, Exception)
                and (
                    isinstance(error, RetryableDownloadError)
                    or _is_retryable_read(error=error)
                )
            ):
                raise RetryableRestoreError(str(error)) from error
            raise

        await session.call(sink.mark)
