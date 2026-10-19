"""Independent data workers borrow snapshots without owning their transactions."""

import asyncio
import contextlib
import copy
import io
import threading

import pytest
import zstandard
from test_yt import Client, Store, add_node, run
from yt import yson
from yt.wrapper.config import get_command_param

import ytclone.yt as yt_module
from ytclone.format import MIB, Range
from ytclone.yt import YtAdapter


class SharedStore(Store):
    def __init__(self):
        super().__init__()
        self.snapshots = {}
        self.sequence = 0

    def factory(self, *, proxy, config):
        client = SharedClient(self)
        client.config = config
        self.clients.append(client)
        return client


class OwnerTransaction:
    def __init__(self, client):
        self.client = client
        client.store.sequence += 1
        self.transaction_id = f"snapshot-{client.store.sequence}"

    def __enter__(self):
        self.client.record("begin")
        self.client.working = copy.deepcopy(self.client.store.nodes)
        self.client.store.snapshots[self.transaction_id] = self.client.working
        return self

    def is_pinger_alive(self):
        return self.client.store.ping_alive

    def __exit__(self, error_type, error, traceback):
        self.client.record("end")
        self.client.store.snapshots.pop(self.transaction_id)
        self.client.working = None
        if error_type is None:
            self.client.store.commits += 1
        else:
            self.client.store.aborts += 1


class SharedClient(Client):
    def Transaction(self, *, timeout, interrupt_on_failed):
        self.record("transaction")
        return OwnerTransaction(self)

    def read_response(self, *, params):
        # A low-level table request carries its snapshot explicitly; the reader
        # client has no implicit transaction or ownership of the snapshot.
        assert self.working is None
        self.working = self.store.snapshots[params["transaction_id"]]
        try:
            return super().read_response(params=params)
        finally:
            self.working = None


@pytest.fixture
def shared(monkeypatch):
    store = SharedStore()
    Client(store).create(type="map_node", path="//tmp")

    def make_request(**kwargs):
        assert kwargs["command_name"] == "read_table"
        return kwargs["client"].read_response(params=kwargs["params"])

    monkeypatch.setattr(yt_module, "make_request", make_request)
    return store, YtAdapter(proxy="test", client_factory=store.factory)


async def read_range(*, worker, snapshot, start=0, stop=2, chunk_size=8):
    return b"".join(
        [
            chunk
            async for chunk in worker.read_part(
                snapshot=snapshot,
                part=Range(index=start, start=start, stop=stop),
                compression_level=3,
                chunk_size=chunk_size,
            )
        ]
    )


def decode(data):
    with zstandard.ZstdDecompressor().stream_reader(io.BytesIO(data)) as stream:
        return stream.read()


def test_distinct_data_workers_read_one_snapshot_concurrently(shared, monkeypatch):
    store, adapter = shared
    source = add_node(store=store)
    barrier = threading.Barrier(2)
    original = SharedClient.read_response
    reads = []

    def read_response(self, *, params):
        reads.append((self, params["transaction_id"]))
        barrier.wait(timeout=3)
        return original(self, params=params)

    monkeypatch.setattr(SharedClient, "read_response", read_response)

    async def main():
        async with contextlib.AsyncExitStack() as stack:
            owner, first, second = [
                await stack.enter_async_context(adapter.worker()) for _ in range(3)
            ]
            async with owner.snapshot(path="//tmp/source") as snapshot:
                store.nodes[source]["data"] = b"changed after snapshot"
                outputs = await asyncio.gather(
                    read_range(worker=first, snapshot=snapshot, start=0, stop=1),
                    read_range(worker=second, snapshot=snapshot, start=1, stop=2),
                )
                assert [
                    list(yson.loads(string=decode(data), yson_type="list_fragment"))
                    for data in outputs
                ] == [[{"x": 1}], [{"x": 2}]]
                assert not snapshot.readers
                assert len(store.clients) == 3
                assert store.commits == 0

    run(main())
    assert len({client.thread for client in store.clients}) == 3
    assert len({transaction for _, transaction in reads}) == 1
    assert store.clients[0].calls.count("lock:snapshot") == 1
    assert all("transaction" not in client.calls for client in store.clients[1:])
    assert all(client.calls[-1] == "cleanup" for client in store.clients)
    assert store.commits == 1


def test_data_client_and_thread_are_reused_across_snapshots(shared, monkeypatch):
    store, adapter = shared
    add_node(store=store, name="first")
    add_node(store=store, name="second")
    original = SharedClient.read_response
    reads = []

    def read_response(self, *, params):
        reads.append((self, threading.get_ident(), params["transaction_id"]))
        return original(self, params=params)

    monkeypatch.setattr(SharedClient, "read_response", read_response)

    async def main():
        async with adapter.worker() as owner, adapter.worker() as reader:
            for name in ("first", "second"):
                async with owner.snapshot(path=f"//tmp/{name}") as snapshot:
                    assert decode(await read_range(worker=reader, snapshot=snapshot))
                assert reader.session.client is store.clients[1]
                assert "cleanup" not in store.clients[1].calls

    run(main())
    assert len(store.clients) == 2
    assert reads[0][:2] == reads[1][:2]
    assert reads[0][2] != reads[1][2]
    assert "transaction" not in store.clients[1].calls
    assert store.commits == 2


@pytest.mark.parametrize("fail_open", [False, True])
def test_file_borrows_real_sdk_context_until_lazy_stream_closes(shared, fail_open):
    store, adapter = shared
    payload = b"abc" * MIB
    add_node(store=store, kind="file", data=payload)
    captured = []
    original_factory = adapter.client_factory

    def factory(**kwargs):
        if not store.clients:
            return original_factory(**kwargs)
        client = yt_module.yt.YtClient(**kwargs)
        client_thread = threading.get_ident()

        def current_transaction():
            assert threading.get_ident() == client_thread
            transaction_id = get_command_param("transaction_id", client)
            assert transaction_id in store.snapshots
            return transaction_id

        class Stream(io.BytesIO):
            def read(self, size=-1):
                captured.append(("read", current_transaction()))
                return super().read(size)

            def close(self):
                captured.append(("close", current_transaction()))
                super().close()

        def read_file(**kwargs):
            captured.append(("open", current_transaction()))
            if fail_open and len(captured) == 1:
                raise TimeoutError("file open failed")
            assert kwargs["offset"] == 0
            assert kwargs["length"] == len(payload)
            return Stream(payload)

        client.read_file = read_file
        return client

    adapter.client_factory = factory

    async def main():
        async with adapter.worker() as owner, adapter.worker() as reader:
            for index in range(2):
                async with owner.snapshot(path="//tmp/source") as snapshot:
                    if fail_open and index == 0:
                        with pytest.raises(TimeoutError, match="file open failed"):
                            await read_range(
                                worker=reader, snapshot=snapshot, stop=len(payload)
                            )
                        assert get_command_param(
                            "transaction_id", reader.session.client
                        ) in (None, yt_module.YT_NULL_TRANSACTION_ID)
                    assert (
                        decode(
                            await read_range(
                                worker=reader, snapshot=snapshot, stop=len(payload)
                            )
                        )
                        == payload
                    )
                    assert get_command_param(
                        "transaction_id", reader.session.client
                    ) in (
                        None,
                        yt_module.YT_NULL_TRANSACTION_ID,
                    )
                    assert (
                        snapshot.session.transaction.transaction_id in store.snapshots
                    )

    run(main())
    assert len([event for event, _ in captured if event == "open"]) == 2 + fail_open
    assert len([event for event, _ in captured if event == "close"]) == 2
    assert {transaction for _, transaction in captured} == {"snapshot-1", "snapshot-2"}
    assert store.commits == 2


def test_snapshot_exit_drains_prefetch_without_disposing_data_worker(
    shared, monkeypatch
):
    store, adapter = shared
    add_node(store=store)
    entered = threading.Event()
    release = threading.Event()
    closed = threading.Event()

    class Compressed:
        calls = 0

        def read(self, *, size):
            self.calls += 1
            if self.calls == 1:
                return b"first"
            entered.set()
            assert release.wait(timeout=3)
            return b"second"

        def close(self):
            assert release.is_set()
            closed.set()

    def open_part(self, **kwargs):
        self._stream = yt_module._ReadStream(raw=io.BytesIO(), compressed=Compressed())
        return self._stream

    monkeypatch.setattr(yt_module._PartReader, "_open_part", open_part)

    async def main():
        async with adapter.worker() as owner, adapter.worker() as reader:
            context = owner.snapshot(path="//tmp/source")
            snapshot = await context.__aenter__()
            chunks = reader.read_part(
                snapshot=snapshot,
                part=Range(index=0, start=0, stop=2),
                compression_level=3,
                chunk_size=8,
            )
            assert await anext(chunks) == b"first"
            assert await asyncio.to_thread(entered.wait, 3)
            closing = asyncio.create_task(context.__aexit__(None, None, None))
            await asyncio.sleep(0)
            assert not closing.done()
            assert store.commits == 0
            release.set()
            await closing
            assert closed.is_set()
            assert store.commits == 1
            assert "cleanup" not in store.clients[1].calls
            await chunks.aclose()
            assert reader._reader is None

    run(main())


@pytest.mark.parametrize("cancellations", [1, 4])
def test_cancel_initial_read_drains_before_snapshot_aborts(
    shared, monkeypatch, cancellations
):
    store, adapter = shared
    add_node(store=store)
    entered = threading.Event()
    release = threading.Event()
    stream = io.BytesIO(b"payload")

    def read_response(self, **kwargs):
        self.record("read_table")
        entered.set()
        assert release.wait(timeout=3)
        return stream

    monkeypatch.setattr(SharedClient, "read_response", read_response)

    async def main():
        async with adapter.worker() as owner, adapter.worker() as reader:

            async def transfer():
                async with owner.snapshot(path="//tmp/source") as snapshot:
                    return await read_range(worker=reader, snapshot=snapshot)

            task = asyncio.create_task(transfer())
            assert await asyncio.to_thread(entered.wait, 3)
            for _ in range(cancellations):
                task.cancel()
                await asyncio.sleep(0)
            assert not task.done()
            assert store.aborts == 0
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert stream.closed
            assert reader._reader is None
            assert store.aborts == 1
            assert "cleanup" not in store.clients[1].calls

    run(main())


def test_uncertain_data_cleanup_replaces_client_before_reuse(shared, monkeypatch):
    store, adapter = shared
    add_node(store=store)
    original = SharedClient.read_response
    attempts = 0

    class BrokenClose(io.BytesIO):
        def close(self):
            super().close()
            raise RuntimeError("uncertain response cleanup")

    def read_response(self, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return BrokenClose(b"first response")
        return original(self, **kwargs)

    monkeypatch.setattr(SharedClient, "read_response", read_response)

    async def main():
        async with (
            adapter.worker() as owner,
            adapter.worker() as reader,
            owner.snapshot(path="//tmp/source") as snapshot,
        ):
            with pytest.raises(RuntimeError, match="uncertain response cleanup"):
                await read_range(worker=reader, snapshot=snapshot)
            assert reader.session.poisoned
            assert decode(await read_range(worker=reader, snapshot=snapshot))
            assert not reader.session.poisoned
            assert store.clients[1].calls[-1] == "cleanup"
            assert store.clients[1].thread == store.clients[2].thread

    run(main())
    assert len(store.clients) == 3
    assert store.commits == 1


def test_one_data_worker_rejects_overlapping_ranges(shared):
    store, adapter = shared
    add_node(store=store)

    async def main():
        async with (
            adapter.worker() as owner,
            adapter.worker() as reader,
            owner.snapshot(path="//tmp/source") as snapshot,
            contextlib.aclosing(
                reader.read_part(
                    snapshot=snapshot,
                    part=Range(index=0, start=0, stop=2),
                    compression_level=3,
                    chunk_size=1,
                )
            ) as chunks,
        ):
            assert await anext(chunks)
            with pytest.raises(RuntimeError, match="one part at a time"):
                await read_range(worker=reader, snapshot=snapshot)

    run(main())


@pytest.mark.parametrize("failure", ["dispose", "open"])
def test_failed_poisoned_client_reset_is_a_worker_lifecycle_error(
    shared, monkeypatch, failure
):
    from ytclone.interfaces import WorkerLifecycleError

    store, adapter = shared
    add_node(store=store)
    source_error = RuntimeError("source close failed")
    reset_error = RuntimeError("client reset failed")
    reads = []

    class BrokenClose(io.BytesIO):
        def close(self):
            super().close()
            raise source_error

    def read_response(self, **kwargs):
        reads.append(self)
        return BrokenClose(b"source bytes")

    monkeypatch.setattr(SharedClient, "read_response", read_response)
    if failure == "dispose":
        original = SharedClient._cleanup
        failed = False

        def cleanup(self):
            nonlocal failed
            if self is store.clients[1] and not failed:
                failed = True
                raise reset_error
            original(self)

        monkeypatch.setattr(SharedClient, "_cleanup", cleanup)
    else:
        original = adapter.client_factory

        def factory(**kwargs):
            if len(store.clients) == 2:
                raise reset_error
            return original(**kwargs)

        adapter.client_factory = factory

    async def main():
        async with (
            adapter.worker() as owner,
            adapter.worker() as reader,
            owner.snapshot(path="//tmp/source") as snapshot,
        ):
            with pytest.raises(RuntimeError) as caught:
                await read_range(worker=reader, snapshot=snapshot)
            assert caught.value is source_error
            assert not isinstance(caught.value, WorkerLifecycleError)
            assert reader.session.poisoned
            with pytest.raises(WorkerLifecycleError) as caught:
                await read_range(worker=reader, snapshot=snapshot)
            assert caught.value.__cause__ is reset_error
            assert reads == [store.clients[1]]
            assert reader._reader is None
            assert not snapshot.readers

    run(main())
