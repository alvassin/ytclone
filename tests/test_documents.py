"""Document archive contracts using the real adapter and transactional SDK fake."""

import asyncio
import copy
import json
from dataclasses import replace

import pytest
import zstandard
from test_engine import MemoryStorage
from test_yt import Client, Store, add_node
from yt import yson

from ytclone.engine import CopyOptions, export_node, restore_node
from ytclone.format import (
    ENCODING,
    Manifest,
    Node,
    ObjectRef,
    Part,
    Range,
    attributes_hash,
    data_key,
    make_fingerprint,
    plan_parts,
)
from ytclone.yt import RESTORE_MARKER, YtAdapter

VALUES = [
    None,
    {},
    [],
    b"",
    b"\x00\xff",
    True,
    False,
    -(2**63),
    yson.YsonUint64(2**64 - 1),
    1.25,
    float("nan"),
    float("inf"),
    float("-inf"),
    yson.to_yson_type(None, attributes={b"tag": b"entity"}),
    yson.to_yson_type([1], attributes={b"tag": b"list"}),
    {b"\xff": b"binary key"},
    {b"nested": [None, yson.YsonUint64(1), b"\xff"]},
    yson.to_yson_type({b"x": 1}, attributes={b"id": b"fake", b"type": b"file"}),
    {b"x": yson.to_yson_type(b"\xff", attributes={b"unit": b"bytes"})},
]


def binary(value):
    return yson.dumps(object=value, yson_format="binary", sort_keys=True)


def setup_document(value):
    store = Store()
    Client(store).create(type="map_node", path="//tmp")
    key = add_node(
        store=store,
        kind="document",
        data=binary(value),
        attributes={"note": "portable metadata"},
    )
    return store, key, YtAdapter(proxy="test", client_factory=store.factory)


def archive_record(size=1):
    node = Node(
        path="//tmp/source",
        kind="document",
        node_id="id",
        content_revision=1,
        attributes=binary({}),
        data_size=size,
    )
    ref = ObjectRef(key="archive/@data/part-000000.yson.zst", size=10, sha256="a" * 64)
    return node, Manifest(
        kind="document",
        source={
            "cluster": "test",
            "path": node.path,
            "node_id": node.node_id,
            "content_revision": 1,
        },
        fingerprint="b" * 64,
        attributes=replace(ref, key="archive/@attributes.yson"),
        parts=(Part(object=ref, start=0, stop=size),),
        data_size=size,
    )


def test_document_format_requires_one_complete_value_and_preserves_legacy_encoding():
    node, manifest = archive_record(100)
    assert list(plan_parts(node=node, part_size=1)) == [
        Range(index=0, start=0, stop=100)
    ]
    assert Manifest.from_bytes(manifest.to_bytes()) == manifest
    assert json.loads(manifest.to_bytes())["encoding"] == {
        **ENCODING,
        "documents": "binary_yson_node",
    }
    assert data_key(prefix="archive", kind="document", index=0).endswith(".yson.zst")
    for changed in (
        replace(manifest, parts=()),
        replace(manifest, row_count=1),
        replace(
            manifest,
            parts=(
                replace(manifest.parts[0], stop=50),
                replace(
                    manifest.parts[0],
                    object=replace(
                        manifest.parts[0].object,
                        key="archive/@data/part-000001.yson.zst",
                    ),
                    start=50,
                ),
            ),
        ),
    ):
        with pytest.raises(ValueError):
            Manifest.from_bytes(changed.to_bytes())
    raw = json.loads(manifest.to_bytes())
    raw["encoding"] = ENCODING
    with pytest.raises(ValueError):
        Manifest.from_bytes(json.dumps(raw).encode())
    with pytest.raises(ValueError):
        list(plan_parts(node=replace(node, data_size=0), part_size=1))


@pytest.mark.parametrize("value", VALUES)
def test_document_roundtrip_preserves_types_attributes_and_unchanged_skips(value):
    store, key, adapter = setup_document(value)
    storage = MemoryStorage(events=[])
    options = CopyOptions(part_size=1)

    async def scenario():
        async with adapter.worker() as worker:

            async def export():
                return await export_node(
                    yt=worker,
                    storage=storage,
                    path="//tmp/source",
                    prefix="archive",
                    cluster="test",
                    options=options,
                )

            async def restore():
                return await restore_node(
                    yt=worker,
                    storage=storage,
                    prefix="archive/tmp/source",
                    destination="//tmp/restored",
                )

            assert await export() == ("completed", "copied")
            assert await export() == ("skipped", "unchanged")
            assert await restore() == ("completed", "copied")
            destination = Client(store).find("//tmp/restored")
            assert store.nodes[destination]["type"] == "document"
            assert store.nodes[destination]["data"] == binary(value)
            assert store.nodes[destination]["attrs"]["note"] == "portable metadata"
            assert await restore() == ("skipped", "unchanged")
            Client(store).set(path="//tmp/restored", value=binary(b"changed"))
            assert await restore() == ("completed", "copied")
            store.nodes[key]["data"] = binary({b"updated": value})
            store.nodes[key]["revision"] += 1
            assert await export() == ("completed", "copied")
            assert await restore() == ("completed", "copied")
            assert store.nodes[Client(store).find("//tmp/restored")]["data"] == binary(
                {b"updated": value}
            )

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "failure", ["checksum", "invalid_yson", "trailing_yson", "oversize", "undersize"]
)
def test_bad_document_archive_rolls_back_existing_destination(failure):
    store, _key, adapter = setup_document({b"original": 1})
    destination = add_node(
        store=store, kind="document", name="restored", data=binary(b"keep")
    )
    before = copy.deepcopy(store.nodes[destination])
    storage = MemoryStorage(events=[])

    async def scenario():
        async with adapter.worker() as worker:
            await export_node(
                yt=worker,
                storage=storage,
                path="//tmp/source",
                prefix="archive",
                cluster="test",
                options=CopyOptions(),
            )
            manifest_key = "archive/tmp/source/@manifest.json"
            manifest = Manifest.from_bytes(storage.objects[manifest_key])
            part = manifest.parts[0]
            if failure in ("invalid_yson", "trailing_yson"):
                raw = b"{broken" if failure == "invalid_yson" else b"{} {}"
                encoded = zstandard.ZstdCompressor().compress(raw)
                storage.objects[part.object.key] = encoded
                manifest = replace(
                    manifest,
                    data_size=len(raw),
                    parts=(
                        replace(
                            part,
                            stop=len(raw),
                            object=replace(
                                part.object,
                                size=len(encoded),
                                sha256=attributes_hash(encoded),
                            ),
                        ),
                    ),
                )
            elif failure == "checksum":
                manifest = replace(
                    manifest,
                    parts=(
                        replace(part, object=replace(part.object, sha256="0" * 64)),
                    ),
                )
            else:
                size = manifest.data_size + (1 if failure == "undersize" else -1)
                manifest = replace(
                    manifest, data_size=size, parts=(replace(part, stop=size),)
                )
            storage.objects[manifest_key] = manifest.to_bytes()
            with pytest.raises((ValueError, yson.YsonError)):
                # Parsing and transport integrity must fail before transaction commit.
                await restore_node(
                    yt=worker,
                    storage=storage,
                    prefix="archive/tmp/source",
                    destination="//tmp/restored",
                )
            assert store.nodes[destination] == before
            assert Client(store).find("//tmp/restored") == destination

    asyncio.run(scenario())


def test_document_modified_after_commit_is_not_marked_complete():
    store, _key, adapter = setup_document({b"x": 1})
    storage = MemoryStorage(events=[])

    async def scenario():
        async with adapter.worker() as worker:
            await export_node(
                yt=worker,
                storage=storage,
                path="//tmp/source",
                prefix="archive",
                cluster="test",
                options=CopyOptions(),
            )

            def change(client):
                Client(store).set(path="//tmp/restored", value=binary({b"x": 2}))

            store.after_commit = change
            with pytest.raises(ValueError, match="document content mismatch"):
                await restore_node(
                    yt=worker,
                    storage=storage,
                    prefix="archive/tmp/source",
                    destination="//tmp/restored",
                )
            assert (
                RESTORE_MARKER
                not in store.nodes[Client(store).find("//tmp/restored")]["attrs"]
            )

    asyncio.run(scenario())


def test_document_snapshot_reads_pinned_value_despite_source_changes():
    store, key, adapter = setup_document({b"x": 1})

    async def scenario():
        async with (
            adapter.worker() as worker,
            worker.snapshot(path="//tmp/source") as snapshot,
        ):
            store.nodes[key]["data"] = binary({b"x": 2})
            compressed = b"".join(
                [
                    chunk
                    async for chunk in snapshot.read_part(
                        part=next(plan_parts(node=snapshot.node, part_size=1)),
                        compression_level=3,
                        chunk_size=3,
                    )
                ]
            )
            with zstandard.ZstdDecompressor().stream_reader(compressed) as reader:
                assert reader.read() == binary({b"x": 1})

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "kind,expected",
    [
        ("table", "1b36127893bc99184412e9e3e7175876ae4a3995eb61a363e29cce08a9fea76b"),
        ("file", "24a1247857886633f85cb069e84956396d68c307717dd278792bce6ac017bf35"),
        (
            "map_node",
            "947474aa509f558c5e1870104052f8f6fb00ee539273ae31f9d6a47ed0f6724f",
        ),
    ],
)
def test_legacy_fingerprints_and_manifest_encodings_are_unchanged(kind, expected):
    node, manifest = archive_record()
    node = replace(node, kind=kind, data_size=0)
    assert (
        make_fingerprint(node=node, cluster="test", compression_level=3, part_size=40)
        == expected
    )
    legacy = replace(manifest, kind=kind, data_size=0, parts=())
    assert json.loads(legacy.to_bytes())["encoding"] == ENCODING
    assert Manifest.from_bytes(legacy.to_bytes()) == legacy
