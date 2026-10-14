"""Pure archive-format tests: portable metadata, identity and untrusted records."""

import json
from dataclasses import replace

import pytest
from yt import yson

from ytclone.format import (
    DiscoveredNode,
    Manifest,
    Node,
    ObjectRef,
    Part,
    attributes_hash,
    canonical_attributes,
    data_key,
    make_fingerprint,
    matches_discovery,
    node_prefix,
    parse_s3_url,
    plan_parts,
    restore_path,
    validate_manifest_location,
    verify_object,
)


def node(*, kind="table", size=7, attributes=None):
    return Node(
        path="//source/table",
        kind=kind,
        node_id="a-b-c-d",
        content_revision=42,
        attributes=canonical_attributes({} if attributes is None else attributes),
        row_count=size if kind == "table" else 0,
        data_size=size if kind == "file" else 0,
        data_weight=size * 10,
    )


def fingerprint(value, *, cluster="test", compression_level=3, part_size=40):
    return make_fingerprint(
        node=value,
        cluster=cluster,
        compression_level=compression_level,
        part_size=part_size,
    )


def manifest():
    prefix = "archive/source/table"
    attrs = b"{}"
    return Manifest(
        kind="table",
        source={
            "cluster": "test",
            "path": "//source/table",
            "node_id": "a-b-c-d",
            "content_revision": 42,
        },
        fingerprint="a" * 64,
        attributes=ObjectRef(
            key=prefix + "/@attributes.yson",
            size=len(attrs),
            sha256=attributes_hash(attrs),
        ),
        parts=tuple(
            Part(
                object=ObjectRef(
                    key=data_key(prefix=prefix, kind="table", index=index),
                    size=stop - start,
                    sha256=attributes_hash(b"x" * (stop - start)),
                ),
                start=start,
                stop=stop,
            )
            for index, (start, stop) in enumerate(((0, 4), (4, 7)))
        ),
        row_count=7,
    )


def test_canonical_yson_preserves_nested_binary_unsigned_and_attributed_values():
    value = yson.YsonString(b"\x00\xff")
    value.attributes = {b"z": b"last", b"a": b"first"}
    source = {b"z": {b"b": yson.YsonUint64(2**64 - 1), b"a": value}, b"a": [None, True]}
    reordered_value = yson.YsonString(b"\x00\xff")
    reordered_value.attributes = {b"a": b"first", b"z": b"last"}
    reordered = {
        b"a": [None, True],
        b"z": {b"a": reordered_value, b"b": yson.YsonUint64(2**64 - 1)},
    }

    encoded = canonical_attributes(source)
    assert encoded == canonical_attributes(reordered)
    restored = yson.loads(string=encoded, encoding=None)
    assert bytes(restored[b"z"][b"a"]) == b"\x00\xff"
    assert restored[b"z"][b"a"].attributes == value.attributes
    assert isinstance(restored[b"z"][b"b"], yson.YsonUint64)
    assert restored[b"z"][b"b"] == 2**64 - 1
    assert canonical_attributes({b"n": 1}) != canonical_attributes(
        {b"n": yson.YsonUint64(1)}
    )


def test_fingerprint_tracks_data_schema_and_encoding_but_not_directory_revision():
    table = node(attributes={b"schema": [{b"name": b"x", b"type": b"int64"}]})
    baseline = fingerprint(table)
    assert baseline == fingerprint(table, cluster="test/")
    assert baseline == fingerprint(replace(table, disk_size=100_000))
    changed_schema = canonical_attributes(
        {b"schema": [{b"name": b"x", b"type": b"string"}]}
    )
    for changed in (
        replace(table, node_id="new-id"),
        replace(table, content_revision=43),
        replace(table, attributes=changed_schema),
    ):
        assert fingerprint(changed) != baseline
    assert fingerprint(table, cluster="other") != baseline
    assert fingerprint(table, part_size=80) != baseline
    assert fingerprint(table, compression_level=1) != baseline

    directory = node(kind="map_node", size=0)
    assert fingerprint(directory) == fingerprint(
        replace(directory, content_revision=999)
    )
    assert fingerprint(directory) != fingerprint(
        replace(
            directory, attributes=canonical_attributes({b"description": b"changed"})
        )
    )


def test_schema_attributes_are_part_of_the_fingerprint():
    schema = yson.YsonList([{b"name": b"id", b"type": b"int64"}])
    schema.attributes = {b"strict": True, b"unique_keys": False}
    baseline = fingerprint(node(attributes={b"schema": schema}))
    schema.attributes[b"unique_keys"] = True
    assert fingerprint(node(attributes={b"schema": schema})) != baseline


@pytest.mark.parametrize(
    "kind,size,part_size", [("table", 7, 40), ("table", 7, 1), ("file", 7, 4)]
)
def test_ranges_cover_node_exactly_once(kind, size, part_size):
    source = node(kind=kind, size=size)
    parts = list(plan_parts(node=source, part_size=part_size))
    assert [part.index for part in parts] == list(range(len(parts)))
    assert [value for part in parts for value in range(part.start, part.stop)] == list(
        range(size)
    )
    assert all(part.stop > part.start for part in parts)
    assert len(parts) == (7 if part_size == 1 else 2)


@pytest.mark.parametrize("kind", ["table", "file", "map_node"])
def test_empty_nodes_have_no_data_parts(kind):
    assert list(plan_parts(node=node(kind=kind, size=0), part_size=4)) == []
    with pytest.raises(ValueError):
        list(plan_parts(node=node(kind=kind, size=0), part_size=0))


def test_manifest_round_trip_and_object_verification():
    value = manifest()
    assert Manifest.from_bytes(value.to_bytes()) == value
    validate_manifest_location(manifest=value, prefix="archive/source/table")
    verify_object(data=b"{}", ref=value.attributes)
    with pytest.raises(ValueError):
        verify_object(data=b"[]", ref=value.attributes)
    with pytest.raises(ValueError):
        verify_object(data=b"{}\n", ref=value.attributes)


@pytest.mark.parametrize(
    "path,value",
    [
        (("format",), "future-format"),
        (("encoding", "compression"), "gzip"),
        (("kind",), "link"),
        (("fingerprint",), "not-a-hash"),
        (("source", "node_id"), None),
        (("source", "content_revision"), True),
        (("attributes", "size"), -1),
        (("attributes", "sha256"), "abc"),
        (("parts", 0, "start"), 1),
        (("parts", 1, "start"), 3),
        (("parts", 1, "stop"), 8),
        (("parts", 0, "stop"), False),
        (("row_count",), 8),
    ],
)
def test_manifest_rejects_invalid_identity_encoding_checksums_and_ranges(path, value):
    raw = json.loads(manifest().to_bytes())
    target = raw
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    with pytest.raises(ValueError):
        Manifest.from_bytes(json.dumps(raw).encode())


@pytest.mark.parametrize("payload", [b"not json", b"null", b"[]", b"{}"])
def test_manifest_rejects_invalid_documents(payload):
    with pytest.raises(ValueError):
        Manifest.from_bytes(payload)


def test_manifest_rejects_duplicate_or_foreign_references():
    original = manifest()
    duplicate = replace(
        original,
        parts=(
            original.parts[0],
            replace(original.parts[1], object=original.parts[0].object),
        ),
    )
    with pytest.raises(ValueError):
        Manifest.from_bytes(duplicate.to_bytes())

    foreign = replace(
        original, attributes=replace(original.attributes, key="other/@attributes.yson")
    )
    with pytest.raises(ValueError):
        validate_manifest_location(manifest=foreign, prefix="archive/source/table")
    reordered = replace(original, parts=tuple(reversed(original.parts)))
    with pytest.raises(ValueError):
        validate_manifest_location(manifest=reordered, prefix="archive/source/table")


def test_path_mapping_preserves_full_source_and_unicode_literal_percent():
    exported = node_prefix(prefix="archive/", path="//home/данные/100%")
    assert exported == "archive/home/%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D0%B5/100%25"
    assert (
        restore_path(source_prefix="archive/home", node=exported, destination="//copy")
        == "//copy/данные/100%"
    )
    assert (
        restore_path(source_prefix=exported, node=exported, destination="//one")
        == "//one"
    )
    assert node_prefix(prefix="", path="//home/table") == "home/table"


@pytest.mark.parametrize(
    "relative",
    ["../escape", "%2E%2E/escape", "%2Fabsolute", "%5Cescape", "%zz", "bad//child"],
)
def test_restore_rejects_noncanonical_or_escaping_names(relative):
    with pytest.raises(ValueError):
        restore_path(
            source_prefix="archive/node",
            node="archive/node/" + relative,
            destination="//copy",
        )


def test_restore_prefix_requires_component_boundary():
    with pytest.raises(ValueError):
        restore_path(
            source_prefix="archive/node",
            node="archive/node-sibling/child",
            destination="//copy",
        )


@pytest.mark.parametrize(
    "url,expected",
    [
        ("s3://bucket", ("bucket", "")),
        ("s3://bucket/a/b/", ("bucket", "a/b")),
        ("s3://bucket/100%25", ("bucket", "100%25")),
    ],
)
def test_s3_url_keeps_literal_key_prefix(url, expected):
    assert parse_s3_url(url) == expected


@pytest.mark.parametrize(
    "url",
    [
        "https://bucket/path",
        "s3:///path",
        "s3://user@bucket/a",
        "s3://bucket:80/a",
        "s3://bucket/a?version=1",
        "s3://bucket/a#fragment",
        "s3://bucket/a//b",
        "s3://bucket/a/../b",
    ],
)
def test_s3_url_rejects_ambiguous_addresses(url):
    with pytest.raises(ValueError):
        parse_s3_url(url)


def discovery_case(*, kind="table"):
    """Build a complete archive and the matching lightweight source record."""
    snapshot = replace(node(kind=kind, size=0), attribute_revision=17)
    archived = Manifest(
        kind=kind,
        source={
            "cluster": "test",
            "path": snapshot.path,
            "node_id": snapshot.node_id,
            "content_revision": snapshot.content_revision,
            "attribute_revision": snapshot.attribute_revision,
        },
        fingerprint=fingerprint(snapshot),
        attributes=ObjectRef(
            key="archive/source/table/@attributes.yson",
            size=len(snapshot.attributes),
            sha256=attributes_hash(snapshot.attributes),
        ),
        parts=(),
    )
    discovered = DiscoveredNode(
        path=snapshot.path,
        kind=kind,
        node_id=snapshot.node_id,
        content_revision=snapshot.content_revision,
        attribute_revision=snapshot.attribute_revision,
        dynamic=False if kind == "table" else None,
    )
    return archived, discovered


def discovery_matches(*, archived, discovered, **options):
    """Compare discovery with the default fixture's copy settings."""
    return matches_discovery(
        manifest=archived,
        node=discovered,
        **{"cluster": "test", "compression_level": 3, "part_size": 40, **options},
    )


def test_fingerprint_remains_compatible_with_archives_before_revision_hints():
    """Adding source revisions does not invalidate the established fingerprint."""
    snapshot = node(attributes={b"description": b"example"})
    expected = "73cb6c30eae05f7691cfd2943e1234aa9febcedebfa09a86915ba67f001c9e15"
    assert fingerprint(snapshot) == expected
    assert fingerprint(replace(snapshot, attribute_revision=999)) == expected


def test_manifest_accepts_legacy_source_without_attribute_revision():
    """Old source records remain readable and require the full comparison."""
    archived, discovered = discovery_case()
    source = dict(archived.source)
    del source["attribute_revision"]
    archived = replace(archived, source=source)

    decoded = Manifest.from_bytes(archived.to_bytes())
    assert decoded == archived
    assert "attribute_revision" not in decoded.source
    assert not discovery_matches(archived=decoded, discovered=discovered)


@pytest.mark.parametrize("revision", [0, 1, 2**64 - 1])
def test_manifest_round_trips_attribute_revision(revision):
    """Known nonnegative revisions are stored without changing the format version."""
    archived = manifest()
    archived = replace(
        archived, source={**archived.source, "attribute_revision": revision}
    )
    assert Manifest.from_bytes(archived.to_bytes()) == archived


@pytest.mark.parametrize("revision", [None, True, False, -1, 1.0, "1", [], {}])
def test_manifest_rejects_malformed_attribute_revision(revision):
    """An optional revision must be an integer when present."""
    archived = manifest()
    archived = replace(
        archived, source={**archived.source, "attribute_revision": revision}
    )
    with pytest.raises(ValueError, match="Invalid attribute revision"):
        Manifest.from_bytes(archived.to_bytes())


@pytest.mark.parametrize("kind", ["table", "file", "map_node"])
def test_discovery_matches_complete_snapshot_identity_and_options(kind):
    """All supported node kinds can bypass reading full source metadata."""
    archived, discovered = discovery_case(kind=kind)
    archived = Manifest.from_bytes(archived.to_bytes())
    assert discovery_matches(archived=archived, discovered=discovered)
    assert discovery_matches(archived=archived, discovered=discovered, cluster="test/")
    assert discovery_matches(
        archived=replace(archived, source={**archived.source, "cluster": "test/"}),
        discovered=discovered,
    )
    assert discovery_matches(
        archived=archived, discovered=replace(discovered, disk_size=999)
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"path": "//other/table"},
        {"path": None},
        {"kind": "file"},
        {"kind": "link"},
        {"kind": []},
        {"node_id": "other-id"},
        {"node_id": None},
        {"node_id": ""},
        {"content_revision": 43},
        {"content_revision": None},
        {"content_revision": True},
        {"content_revision": -1},
        {"content_revision": "42"},
        {"content_revision": 42.0},
        {"attribute_revision": 18},
        {"attribute_revision": None},
        {"attribute_revision": True},
        {"attribute_revision": -1},
        {"attribute_revision": "17"},
        {"attribute_revision": 17.0},
        {"dynamic": True},
        {"dynamic": None},
        {"dynamic": 0},
        {"dynamic": "false"},
    ],
)
def test_discovery_rejects_changed_missing_or_malformed_hints(changes):
    """Incomplete or changed discovery records fall back to snapshot comparison."""
    archived, discovered = discovery_case()
    assert not discovery_matches(
        archived=archived, discovered=replace(discovered, **changes)
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"cluster": "other"},
        {"cluster": None},
        {"path": "//other/table"},
        {"node_id": "other-id"},
        {"content_revision": 43},
        {"content_revision": True},
        {"attribute_revision": 18},
        {"attribute_revision": True},
        {"attribute_revision": 17.0},
    ],
)
def test_discovery_rejects_inconsistent_manifest_source(changes):
    """The saved fingerprint cannot override inconsistent saved identity fields."""
    archived, discovered = discovery_case()
    assert not discovery_matches(
        archived=replace(archived, source={**archived.source, **changes}),
        discovered=discovered,
    )


@pytest.mark.parametrize(
    "missing", ["cluster", "path", "node_id", "content_revision", "attribute_revision"]
)
def test_discovery_rejects_missing_manifest_source_fields(missing):
    """Every required table identity field must have been saved."""
    archived, discovered = discovery_case()
    source = dict(archived.source)
    del source[missing]
    assert not discovery_matches(
        archived=replace(archived, source=source), discovered=discovered
    )


@pytest.mark.parametrize(
    "options", [{"cluster": "other"}, {"compression_level": 1}, {"part_size": 80}]
)
def test_discovery_rejects_changed_copy_options(options):
    """An archive made with other copy settings must undergo full comparison."""
    archived, discovered = discovery_case()
    assert not discovery_matches(archived=archived, discovered=discovered, **options)


@pytest.mark.parametrize("content_revision", [999, None])
def test_directory_discovery_ignores_changes_to_children(content_revision):
    """A directory's child revision does not change its own archived attributes."""
    archived, discovered = discovery_case(kind="map_node")
    assert discovery_matches(
        archived=archived,
        discovered=replace(discovered, content_revision=content_revision),
    )
    assert not discovery_matches(
        archived=archived, discovered=replace(discovered, attribute_revision=18)
    )


def test_file_discovery_requires_content_revision_but_not_dynamic_flag():
    """File discovery uses revisions without table-only attributes."""
    archived, discovered = discovery_case(kind="file")
    assert discovery_matches(archived=archived, discovered=discovered)
    assert not discovery_matches(
        archived=archived, discovered=replace(discovered, content_revision=None)
    )


def test_discovery_checks_saved_attribute_digest_and_archive_encoding(monkeypatch):
    """Fingerprint recomputation catches changed digests and encoder settings."""
    import ytclone.format as archive_format

    archived, discovered = discovery_case()
    assert not discovery_matches(
        archived=replace(
            archived, attributes=replace(archived.attributes, sha256="0" * 64)
        ),
        discovered=discovered,
    )
    monkeypatch.setattr(
        archive_format, "ENCODING", {**archive_format.ENCODING, "compression": "other"}
    )
    assert not discovery_matches(archived=archived, discovered=discovered)
