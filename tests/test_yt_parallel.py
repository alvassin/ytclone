"""Concurrent table ranges must retain one snapshot and drain every SDK reader."""

import asyncio
import contextlib
import io
import threading
from types import SimpleNamespace

import pytest
import zstandard
from test_yt import Transaction, add_node, read_error, run
from test_yt import store as store  # noqa: PLC0414 - re-export shared fixture
from yt import yson

import ytclone.yt as yt_module
from ytclone.format import Range
from ytclone.interfaces import RetryableReadError
from ytclone.yt import YtAdapter, _ReadStream, _SnapshotReader


@pytest.fixture
def parallel_adapter(store, monkeypatch):
    """Model a server snapshot shared by independent clients through its ID."""
    requests = SimpleNamespace(calls=[], streams=[], before_read=None)

    def make_request(
        *, command_name, params, return_content, use_heavy_proxy, allow_retries, client
    ):
        assert command_name == "read_table"
        assert return_content is False
        assert use_heavy_proxy is True
        assert allow_retries is False
        assert params["transaction_id"] == "snapshot-transaction"
        client.record("read_table")
        owner = next(item for item in store.clients if item.working is not None)
        path = params["path"]
        assert str(path).startswith("#")
        limits = path.attributes["ranges"][0]
        start = limits["lower_limit"]["row_index"]
        stop = limits["upper_limit"]["row_index"]
        requests.calls.append((client, threading.get_ident(), start, stop))
        if requests.before_read is not None:
            requests.before_read(start)
        rows = yson.loads(
            string=owner.working[str(path)[1:]]["data"],
            yson_type="list_fragment",
        )
        raw = io.BytesIO(
            yson.dumps(
                object=list(rows)[start:stop],
                yson_type="list_fragment",
                yson_format="binary",
            )
        )
        requests.streams.append(raw)
        return raw

    monkeypatch.setattr(yt_module, "make_request", make_request)
    return YtAdapter(proxy="test", client_factory=store.factory), requests


async def read_rows(snapshot, index):
    """Fully consume and decode one independent compressed range."""
    async with contextlib.aclosing(
        snapshot.read_part(
            part=Range(index=index, start=index, stop=index + 1),
            compression_level=3,
            chunk_size=8,
        )
    ) as chunks:
        data = b"".join([chunk async for chunk in chunks])
    with zstandard.ZstdDecompressor().stream_reader(io.BytesIO(data)) as source:
        return list(yson.loads(string=source.read(), yson_type="list_fragment"))


def test_parallel_readers_share_snapshot_and_reuse_clients(parallel_adapter, store):
    """Overlapping parts use separate threads and one pinned source generation."""
    adapter, requests = parallel_adapter
    source = add_node(store=store)
    barrier = threading.Barrier(2, timeout=3)
    requests.before_read = lambda start: barrier.wait()

    async def main():
        async with adapter.worker() as worker:
            async with worker.snapshot(path="//tmp/source") as snapshot:
                # A mutation outside the transaction must never reach a borrowed reader.
                store.nodes[source]["data"] = b'{"x"=999;};'
                for _ in range(2):
                    rows = await asyncio.gather(
                        read_rows(snapshot, 0), read_rows(snapshot, 1)
                    )
                    assert rows == [[{"x": 1}], [{"x": 2}]]
                    assert len(store.clients) == 2
                    assert all(stream.closed for stream in requests.streams)
                assert sum(c.calls.count("transaction") for c in store.clients) == 1
                assert sum(c.calls.count("lock:snapshot") for c in store.clients) == 1
                assert store.clients[1].working is None
            assert store.clients[1].calls[-1] == "cleanup"
            assert "cleanup" not in store.clients[0].calls

    run(main())
    assert len({thread for _, thread, _, _ in requests.calls}) == 2
    assert all(client.thread == thread for client, thread, _, _ in requests.calls)
    assert store.commits == 1
    assert store.aborts == 0
    assert all(client.calls.count("cleanup") == 1 for client in store.clients)


def test_cancelled_parallel_reads_drain_before_transaction_exit(
    parallel_adapter, store, monkeypatch
):
    """Cancel both blocked initial requests and close their streams before abort."""
    adapter, requests = parallel_adapter
    add_node(store=store)
    entered = [threading.Event(), threading.Event()]
    release = threading.Event()
    original_exit = Transaction.__exit__

    def block(start):
        entered[start].set()
        assert release.wait(timeout=3)

    def transaction_exit(self, error_type, error, traceback):
        assert len(requests.streams) == 2
        assert all(stream.closed for stream in requests.streams)
        assert store.clients[1].calls[-1] == "cleanup"
        return original_exit(self, error_type, error, traceback)

    requests.before_read = block
    monkeypatch.setattr(Transaction, "__exit__", transaction_exit)

    async def main():
        async with adapter.worker() as worker:
            with pytest.raises(asyncio.CancelledError):
                async with worker.snapshot(path="//tmp/source") as snapshot:
                    tasks = [
                        asyncio.create_task(read_rows(snapshot, index))
                        for index in range(2)
                    ]
                    try:
                        for event in entered:
                            assert await asyncio.to_thread(event.wait, 3)
                        for task in tasks:
                            task.cancel()
                        await asyncio.sleep(0)
                        assert store.aborts == store.commits == 0
                        release.set()
                        await asyncio.gather(*tasks)
                    finally:
                        release.set()
                        await asyncio.gather(*tasks, return_exceptions=True)

    run(main())
    assert store.aborts == 1
    assert store.commits == 0


def test_secondary_reader_retries_in_same_snapshot(parallel_adapter, store):
    """A failed borrowed reader is reusable only after its source stream is closed."""
    adapter, requests = parallel_adapter
    add_node(store=store)
    attempts = 0

    def fail_once(start):
        nonlocal attempts
        if start == 1:
            attempts += 1
            if attempts == 1:
                raise read_error(name="timeout")

    requests.before_read = fail_once

    async def main():
        async with (
            adapter.worker() as worker,
            worker.snapshot(path="//tmp/source") as snapshot,
            contextlib.aclosing(
                snapshot.read_part(
                    part=Range(index=0, start=0, stop=1),
                    compression_level=3,
                    chunk_size=8,
                )
            ) as primary,
        ):
            assert await anext(primary)
            with pytest.raises(RetryableReadError):
                await read_rows(snapshot, 1)
            assert await read_rows(snapshot, 1) == [{"x": 2}]
            assert len(store.clients) == 2
            assert sum(c.calls.count("transaction") for c in store.clients) == 1

    run(main())
    secondary = [client for client, _, start, _ in requests.calls if start == 1]
    assert secondary == [store.clients[1], store.clients[1]]


def test_secondary_reader_rejects_failed_owner_pinger(parallel_adapter, store):
    """No borrowed client may read after the sole snapshot keepalive fails."""
    adapter, requests = parallel_adapter
    add_node(store=store)

    async def main():
        async with adapter.worker() as worker:
            with pytest.raises(RuntimeError, match="transaction ping failed"):
                async with (
                    worker.snapshot(path="//tmp/source") as snapshot,
                    contextlib.aclosing(
                        snapshot.read_part(
                            part=Range(index=0, start=0, stop=1),
                            compression_level=3,
                            chunk_size=8,
                        )
                    ) as primary,
                ):
                    assert await anext(primary)
                    store.ping_alive = False
                    await read_rows(snapshot, 1)

    run(main())
    assert len(requests.calls) == 1
    assert store.aborts == 1
    assert all(client.calls[-1] == "cleanup" for client in store.clients)


def test_secondary_cleanup_failure_aborts_after_other_readers_close(
    parallel_adapter, store, monkeypatch
):
    """An unsafe borrowed client is not reused, and cleanup still reaches every lane."""
    adapter, requests = parallel_adapter
    add_node(store=store)
    original_request = yt_module.make_request
    failure = RuntimeError("secondary close failed")
    broken = []

    class BrokenReader(io.BytesIO):
        def close(self):
            super().close()
            raise failure

    def make_request(**kwargs):
        response = original_request(**kwargs)
        start = kwargs["params"]["path"].attributes["ranges"][0]["lower_limit"][
            "row_index"
        ]
        if start == 1:
            replacement = BrokenReader(response.getvalue())
            response.close()
            broken.append(replacement)
            return replacement
        return response

    monkeypatch.setattr(yt_module, "make_request", make_request)

    async def main():
        async with adapter.worker() as worker:
            with pytest.raises(RuntimeError, match="secondary close failed"):
                async with (
                    worker.snapshot(path="//tmp/source") as snapshot,
                    contextlib.aclosing(
                        snapshot.read_part(
                            part=Range(index=0, start=0, stop=1),
                            compression_level=3,
                            chunk_size=8,
                        )
                    ) as primary,
                ):
                    assert await anext(primary)
                    with pytest.raises(RuntimeError) as caught:
                        await read_rows(snapshot, 1)
                    assert caught.value is failure
                    # A caller cannot silently reuse a client with uncertain cleanup.
                    with pytest.raises(RuntimeError, match="reader cleanup failed"):
                        await read_rows(snapshot, 1)

    run(main())
    assert len(requests.calls) == 2
    assert all(stream.closed for stream in [*requests.streams, *broken])
    assert all(client.calls[-1] == "cleanup" for client in store.clients)
    assert store.aborts == 1
    assert store.commits == 0


def test_failed_secondary_client_open_is_cleaned_before_abort(
    parallel_adapter, store, monkeypatch
):
    """A partially acquired reader must not leak its executor or end the owner early."""
    adapter, _ = parallel_adapter
    add_node(store=store)
    factory = adapter.client_factory
    sessions_closed = []
    original_close = yt_module._Session.close

    def client_factory(**kwargs):
        if store.clients:
            raise ValueError("cannot open secondary client")
        return factory(**kwargs)

    async def close(self):
        try:
            await original_close(self)
        finally:
            sessions_closed.append(self)

    adapter.client_factory = client_factory
    monkeypatch.setattr(yt_module._Session, "close", close)

    async def main():
        async with adapter.worker() as worker:
            with pytest.raises(ValueError, match="cannot open secondary client"):
                async with (
                    worker.snapshot(path="//tmp/source") as snapshot,
                    contextlib.aclosing(
                        snapshot.read_part(
                            part=Range(index=0, start=0, stop=1),
                            compression_level=3,
                            chunk_size=8,
                        )
                    ) as primary,
                ):
                    assert await anext(primary)
                    await read_rows(snapshot, 1)
            assert len(sessions_closed) == 1
            assert sessions_closed[0] is not worker.session

    run(main())
    assert len(sessions_closed) == 2
    assert store.aborts == 1
    assert store.commits == 0


def test_snapshot_exit_drains_abandoned_parallel_prefetch(
    parallel_adapter, store, monkeypatch
):
    """Leaving a snapshot closes all borrowed readers before its transaction ends."""
    adapter, _ = parallel_adapter
    add_node(store=store)
    entered = [threading.Event(), threading.Event()]
    release = threading.Event()
    readers = []

    class Reader:
        def __init__(self, index):
            self.index = index
            self.calls = 0
            self.closed = False

        def read(self, *, size):
            self.calls += 1
            if self.calls == 1:
                return b"first"
            entered[self.index].set()
            assert release.wait(timeout=3)
            return b"second"

        def close(self):
            assert release.is_set()
            self.closed = True

    def open_part(self, *, part, compression_level):
        compressed = Reader(part.index)
        readers.append(compressed)
        self._stream = _ReadStream(raw=io.BytesIO(), compressed=compressed)
        return self._stream

    monkeypatch.setattr(_SnapshotReader, "_open_part", open_part)

    async def main():
        async with adapter.worker() as worker:
            context = worker.snapshot(path="//tmp/source")
            snapshot = await context.__aenter__()
            parts = [
                snapshot.read_part(
                    part=Range(index=index, start=index, stop=index + 1),
                    compression_level=3,
                    chunk_size=8,
                )
                for index in range(2)
            ]
            try:
                assert await asyncio.gather(*(anext(part) for part in parts)) == [
                    b"first",
                    b"first",
                ]
                for event in entered:
                    assert await asyncio.to_thread(event.wait, 3)
                exiting = asyncio.create_task(context.__aexit__(None, None, None))
                await asyncio.sleep(0)
                assert not exiting.done()
                assert store.commits == 0
                release.set()
                await exiting
                assert all(reader.closed for reader in readers)
                assert store.clients[1].calls[-1] == "cleanup"
                with pytest.raises(RuntimeError, match="Snapshot is closed"):
                    await read_rows(snapshot, 0)
            finally:
                release.set()
                await asyncio.gather(*(part.aclose() for part in parts))

    run(main())
    assert store.commits == 1
