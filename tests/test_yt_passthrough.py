"""YT-compressed tables retain the existing archive and restore contracts."""

import io
import struct
import threading
from contextlib import nullcontext
from types import SimpleNamespace

import pytest
import zstandard
from test_engine import MemoryStorage
from test_yt import Client, Store, add_node, run
from test_yt_pool import SharedClient, SharedStore
from yt import yson

import ytclone.yt as yt_module
from ytclone.engine import CopyOptions, export_node, restore_node
from ytclone.export_pool import PartPool
from ytclone.format import Manifest, plan_parts
from ytclone.yt import YtAdapter

ARCHIVE = "archive/tmp/source"


def decode(data):
    """Read every concatenated frame using the standard Zstandard decoder."""
    with zstandard.ZstdDecompressor().stream_reader(io.BytesIO(data)) as stream:
        return stream.read()


@pytest.fixture
def table_source(monkeypatch):
    """Serve snapshot-bound row ranges as short, independent proxy blocks."""
    store = SharedStore()
    Client(store).create(type="map_node", path="//tmp")
    rows = [{"x": index} for index in range(4)]
    data = yson.dumps(object=rows, yson_type="list_fragment", yson_format="binary")
    attributes = {
        "schema": [{"name": "x", "type": "int64"}],
        "note": "portable metadata",
    }
    add_node(store=store, data=data, attributes=attributes)
    source = SimpleNamespace(
        store=store,
        rows=rows,
        data=data,
        attributes=attributes,
        requests=[],
        streams=[],
        failure=None,
        adapter=YtAdapter(
            proxy="test", client_factory=store.factory, yt_zstd_passthrough=True
        ),
    )
    compressor = zstandard.ZstdCompressor(level=3)
    compression_lock = threading.Lock()

    def block(raw):
        frame = compressor.compress(raw)
        return struct.pack("<HQQ", 0x5B0C, 8 + len(frame), len(raw)) + frame, frame

    class Response(io.BytesIO):
        """Expose HTTP encoding and a possible error at final response EOF."""

        def __init__(self, encoded, frames):
            super().__init__(encoded)
            self.frames = frames

        def _get_response(self):
            return SimpleNamespace(headers={"Content-Encoding": "z-zstd_3"})

        def read(self, size=-1):
            if source.failure == "late-error" and self.tell() == len(self.getbuffer()):
                raise RuntimeError("late source error")
            return super().read(size)

    def make_request(**kwargs):
        client, params = kwargs["client"], kwargs["params"]
        assert isinstance(client, SharedClient)
        assert client.working is None
        assert params["transaction_id"] in store.snapshots
        source.requests.append((client, params["transaction_id"]))
        with client.read_response(params=params) as raw:
            payload = raw.read()
        with compression_lock:
            records = [
                block(payload[index : index + 3]) for index in range(0, len(payload), 3)
            ]
            terminator, _ = block(b"")
        encoded = b"".join(record for record, _ in records)
        if source.failure != "missing-eos":
            encoded += terminator
        response = Response(encoded, b"".join(frame for _, frame in records))
        source.streams.append(response)
        return response

    monkeypatch.setattr(yt_module, "make_request", make_request)
    monkeypatch.setattr(
        yt_module, "_zstd_transport", lambda **kwargs: nullcontext(), raising=False
    )
    return source


async def export_table(source, storage):
    """Exercise the real adapter, data-worker pool and manifest publication."""
    options = CopyOptions(concurrency=1, max_streams=2, part_size=1)
    async with (
        source.adapter.worker() as owner,
        PartPool(yt=source.adapter, concurrency=options.stream_limit) as pool,
    ):
        return await export_node(
            pool=pool,
            yt=owner,
            storage=storage,
            path="//tmp/source",
            prefix="archive",
            cluster="test",
            options=options,
        )


def test_passthrough_parts_restore_without_local_table_compression(
    table_source, monkeypatch
):
    """Published parts are unchanged proxy frames accepted by normal restore."""
    source = table_source
    storage = MemoryStorage(events=[])
    target_store = Store()
    Client(target_store).create(type="map_node", path="//tmp")
    target = YtAdapter(proxy="test", client_factory=target_store.factory)

    def forbidden_compressor(*args, **kwargs):
        pytest.fail("Passthrough tables must not create a local compressor")

    monkeypatch.setattr(yt_module.zstandard, "ZstdCompressor", forbidden_compressor)

    async def scenario():
        assert await export_table(source, storage) == ("completed", "copied")
        manifest = Manifest.from_bytes(storage.objects[f"{ARCHIVE}/@manifest.json"])
        assert len(manifest.parts) == len(source.rows)
        parts = [storage.objects[part.object.key] for part in manifest.parts]
        assert all(part.object.key.endswith(".yson.zst") for part in manifest.parts)
        assert sorted(parts) == sorted(stream.frames for stream in source.streams)
        assert b"".join(decode(part) for part in parts) == source.data
        async with target.worker() as worker:
            assert await restore_node(
                yt=worker,
                storage=storage,
                prefix=ARCHIVE,
                destination="//tmp/restored",
            ) == ("completed", "copied")

    run(scenario())
    restored = target_store.nodes[Client(target_store).find("//tmp/restored")]
    assert restored["data"] == source.data
    assert restored["attrs"]["schema"] == source.attributes["schema"]
    assert restored["attrs"]["note"] == source.attributes["note"]
    assert {transaction for _, transaction in source.requests} == {"snapshot-1"}
    assert all("transaction" not in client.calls for client, _ in source.requests)
    assert all(stream.closed for stream in source.streams)
    assert source.store.snapshots == {}
    assert source.store.commits == target_store.commits == 1


@pytest.mark.parametrize(
    "failure,error,message",
    [
        ("missing-eos", ValueError, "Truncated"),
        ("late-error", RuntimeError, "late source error"),
    ],
)
def test_incomplete_proxy_response_cannot_publish_a_manifest(
    table_source, failure, error, message
):
    """A valid compressed payload still needs its terminator and HTTP EOF."""
    table_source.failure = failure
    storage = MemoryStorage(events=[])
    with pytest.raises(error, match=message):
        run(export_table(table_source, storage))
    assert f"{ARCHIVE}/@manifest.json" not in storage.objects
    assert table_source.streams
    assert all(stream.closed for stream in table_source.streams)
    assert table_source.store.snapshots == {}
    assert table_source.store.aborts == 1


@pytest.mark.parametrize("kind", ["file", "document"])
def test_passthrough_flag_keeps_local_compression_for_other_nodes(monkeypatch, kind):
    """The table transport setting leaves files and documents on their compressor."""
    store = Store()
    Client(store).create(type="map_node", path="//tmp")
    data = (
        b"file bytes"
        if kind == "file"
        else yson.dumps(object={"key": "document value"}, yson_format="binary")
    )
    add_node(store=store, kind=kind, data=data)
    adapter = YtAdapter(
        proxy="test", client_factory=store.factory, yt_zstd_passthrough=True
    )
    compressor = zstandard.ZstdCompressor
    levels = []

    def local_compressor(*, level):
        levels.append(level)
        return compressor(level=level)

    monkeypatch.setattr(yt_module.zstandard, "ZstdCompressor", local_compressor)
    monkeypatch.setattr(
        yt_module,
        "make_request",
        lambda **kwargs: pytest.fail(
            "Non-table nodes must not request table transport"
        ),
    )

    async def scenario():
        async with (
            adapter.worker() as worker,
            worker.snapshot(path="//tmp/source") as snapshot,
        ):
            parts = list(plan_parts(node=snapshot.node, part_size=3))
            restored = []
            for part in parts:
                encoded = b"".join(
                    [
                        chunk
                        async for chunk in worker.read_part(
                            snapshot=snapshot,
                            part=part,
                            compression_level=3,
                            chunk_size=4,
                        )
                    ]
                )
                restored.append(decode(encoded))
            assert b"".join(restored) == data
            assert levels == [3] * len(parts)

    run(scenario())
