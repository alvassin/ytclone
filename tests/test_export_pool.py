"""Shared reader bounds, failure isolation and cancellation without network I/O."""

import asyncio
from contextlib import asynccontextmanager

import pytest

from ytclone.export_pool import PartPool
from ytclone.format import ObjectRef, Part, Range
from ytclone.interfaces import WorkerLifecycleError


class Readers:
    """Record leased clients and allow initialization to fail."""

    def __init__(self, *, fail=False):
        self.opened = []
        self.closed = []
        self.fail = fail

    @asynccontextmanager
    async def worker(self):
        reader = object()
        self.opened.append(reader)
        try:
            if self.fail:
                raise OSError("client initialization failed")
            yield reader
        finally:
            self.closed.append(reader)


def ranges(count):
    for index in range(count):
        yield Range(index=index, start=index, stop=index + 1)


def result(part):
    return Part(
        object=ObjectRef(key=str(part.index), size=1, sha256="0" * 64),
        start=part.start,
        stop=part.stop,
    )


@pytest.mark.parametrize("concurrency", [1, 3])
@pytest.mark.parametrize("parallel", [False, True])
def test_one_node_uses_available_readers_and_reuses_them(concurrency, parallel):
    async def scenario():
        readers = Readers()
        started = asyncio.Event()
        release = asyncio.Event()
        assigned = []
        active = set()
        expected = concurrency if parallel else 1

        def planned():
            for part in ranges(9):
                assigned.append(part.index)
                yield part

        async def transfer(*, reader, part):
            assert reader not in active
            active.add(reader)
            if len(active) == expected:
                started.set()
            try:
                await release.wait()
                await asyncio.sleep(0)
                return result(part)
            finally:
                active.remove(reader)

        async with PartPool(yt=readers, concurrency=concurrency) as pool:
            job = asyncio.create_task(
                pool.copy(parts=planned(), transfer=transfer, parallel=parallel)
            )
            try:
                await asyncio.wait_for(started.wait(), timeout=2)
                # Keep only one unassigned lookahead range per node.
                assert assigned == list(range(expected + 1))
                assert len(readers.opened) == expected
            finally:
                release.set()
            assert await job == [result(part) for part in ranges(9)]
            opened = list(readers.opened)
            assert await pool.copy(
                parts=ranges(3), transfer=transfer, parallel=parallel
            ) == [result(part) for part in ranges(3)]
            assert readers.opened == opened
        assert set(readers.closed) == set(readers.opened)

    asyncio.run(scenario())


def test_new_table_receives_next_round_without_waiting_for_large_table():
    async def scenario():
        readers = Readers()
        requests = asyncio.Queue()
        gates = {}

        def transfer_for(name):
            async def transfer(*, reader, part):
                key = (name, part.index)
                gate = gates[key] = asyncio.Event()
                requests.put_nowait(key)
                await gate.wait()
                return result(part)

            return transfer

        async with PartPool(yt=readers, concurrency=1) as pool:
            first = asyncio.create_task(
                pool.copy(parts=ranges(4), transfer=transfer_for("A"), parallel=True)
            )
            assert await asyncio.wait_for(requests.get(), timeout=2) == ("A", 0)
            second = asyncio.create_task(
                pool.copy(parts=ranges(1), transfer=transfer_for("B"), parallel=True)
            )
            await asyncio.sleep(0)
            gates["A", 0].set()
            assert await asyncio.wait_for(requests.get(), timeout=2) == ("A", 1)
            gates["A", 1].set()
            assert await asyncio.wait_for(requests.get(), timeout=2) == ("B", 0)
            gates["B", 0].set()
            assert await second == [result(next(ranges(1)))]
            assert not first.done()
            for index in (2, 3):
                assert await asyncio.wait_for(requests.get(), timeout=2) == ("A", index)
                gates["A", index].set()
            assert len(await first) == 4

    asyncio.run(scenario())


def test_failed_node_drains_its_active_range_while_other_node_finishes():
    async def scenario():
        readers = Readers()
        requests = asyncio.Queue()
        fail = asyncio.Event()
        drain = asyncio.Event()
        attempted = []

        async def broken(*, reader, part):
            attempted.append(part.index)
            requests.put_nowait(part.index)
            if part.index == 0:
                await fail.wait()
                raise OSError("broken table")
            await drain.wait()
            return result(part)

        async def healthy(*, reader, part):
            return result(part)

        async with PartPool(yt=readers, concurrency=2) as pool:
            first = asyncio.create_task(
                pool.copy(parts=ranges(10), transfer=broken, parallel=True)
            )
            for index in (0, 1):
                assert await asyncio.wait_for(requests.get(), timeout=2) == index
            second = asyncio.create_task(
                pool.copy(parts=ranges(4), transfer=healthy, parallel=True)
            )
            try:
                fail.set()
                assert len(await asyncio.wait_for(second, timeout=2)) == 4
                assert not first.done()
                assert attempted == [0, 1]
            finally:
                drain.set()
            with pytest.raises(OSError, match="broken table"):
                await first
            assert (
                len(await pool.copy(parts=ranges(2), transfer=healthy, parallel=True))
                == 2
            )
        assert len(readers.opened) == len(readers.closed) == 2

    asyncio.run(scenario())


def test_cancelled_owner_retains_inflight_reads_and_does_not_plan_more():
    async def scenario():
        readers = Readers()
        started = asyncio.Event()
        release = asyncio.Event()
        assigned = []
        closed = []

        async def transfer(*, reader, part):
            assigned.append(part.index)
            if len(assigned) == 3:
                started.set()
            try:
                await release.wait()
                return result(part)
            finally:
                closed.append(part.index)

        async with PartPool(yt=readers, concurrency=3) as pool:
            job = asyncio.create_task(
                pool.copy(parts=ranges(10**9), transfer=transfer, parallel=True)
            )
            try:
                await asyncio.wait_for(started.wait(), timeout=2)
                for _ in range(4):
                    job.cancel()
                    await asyncio.sleep(0)
                assert not job.done()
                assert assigned == [0, 1, 2]
                assert not closed
            finally:
                release.set()
            with pytest.raises(asyncio.CancelledError):
                await job
            assert sorted(closed) == assigned

    asyncio.run(scenario())


@pytest.mark.parametrize("failure", ["initialization", "reset"])
def test_reader_lifecycle_failure_stops_pool_and_rejects_new_work(failure):
    async def scenario():
        readers = Readers(fail=failure == "initialization")
        error_type = OSError if failure == "initialization" else WorkerLifecycleError

        async def transfer(*, reader, part):
            raise WorkerLifecycleError("client reset failed")

        pool = PartPool(yt=readers, concurrency=1)
        await pool.__aenter__()
        try:
            with pytest.raises(error_type):
                await pool.copy(parts=ranges(2), transfer=transfer, parallel=True)
            with pytest.raises(error_type):
                pool.check()
            with pytest.raises(error_type):
                await pool.copy(parts=ranges(1), transfer=transfer, parallel=True)
        finally:
            with pytest.raises(error_type):
                await pool.close()
        assert len(readers.opened) == len(readers.closed) == 1

    asyncio.run(scenario())


def test_empty_and_failed_range_plans_do_not_open_clients():
    async def scenario():
        readers = Readers()

        async def transfer(*, reader, part):
            return result(part)

        def broken():
            raise ValueError("invalid range plan")
            yield  # pragma: no cover

        async with PartPool(yt=readers, concurrency=3) as pool:
            assert (
                await pool.copy(parts=ranges(0), transfer=transfer, parallel=True) == []
            )
            with pytest.raises(ValueError, match="invalid range plan"):
                await pool.copy(parts=broken(), transfer=transfer, parallel=True)
            assert not readers.opened
            assert (
                len(await pool.copy(parts=ranges(1), transfer=transfer, parallel=True))
                == 1
            )

    asyncio.run(scenario())


def test_fatal_reader_failure_is_visible_before_gated_backend_cleanup():
    """An owner cannot admit another node while the failed reader is closing."""

    async def scenario():
        cleanup_started = asyncio.Event()
        release_cleanup = asyncio.Event()
        failure = WorkerLifecycleError("client reset failed")

        class GatedReaders(Readers):
            @asynccontextmanager
            async def worker(self):
                reader = object()
                self.opened.append(reader)
                try:
                    yield reader
                finally:
                    cleanup_started.set()
                    await release_cleanup.wait()
                    self.closed.append(reader)

        async def transfer(*, reader, part):
            raise failure

        readers = GatedReaders()
        pool = PartPool(yt=readers, concurrency=1)
        await pool.__aenter__()
        owner = asyncio.create_task(
            pool.copy(parts=ranges(2), transfer=transfer, parallel=True)
        )
        try:
            await asyncio.wait_for(cleanup_started.wait(), timeout=2)
            with pytest.raises(WorkerLifecycleError) as caught:
                await asyncio.wait_for(owner, timeout=2)
            assert caught.value is failure
            assert not readers.closed
            with pytest.raises(WorkerLifecycleError) as caught:
                pool.check()
            assert caught.value is failure
            with pytest.raises(WorkerLifecycleError) as caught:
                await pool.copy(parts=ranges(0), transfer=transfer, parallel=True)
            assert caught.value is failure
            assert len(readers.opened) == 1
        finally:
            release_cleanup.set()
            with pytest.raises(WorkerLifecycleError):
                await pool.close()
            await asyncio.gather(owner, return_exceptions=True)
        assert readers.closed == readers.opened

    asyncio.run(scenario())


@pytest.mark.parametrize("concurrency", [1, 3])
def test_empty_node_completes_while_every_data_worker_is_busy(concurrency):
    """Metadata-only nodes do not wait for an unrelated table's reader slot."""

    async def scenario():
        readers = Readers()
        started = asyncio.Event()
        release = asyncio.Event()
        active = set()

        async def busy_transfer(*, reader, part):
            active.add(reader)
            if len(active) == concurrency:
                started.set()
            try:
                await release.wait()
                return result(part)
            finally:
                active.remove(reader)

        async def empty_transfer(*, reader, part):
            pytest.fail("An empty node must not use a data worker")

        async with PartPool(yt=readers, concurrency=concurrency) as pool:
            busy = asyncio.create_task(
                pool.copy(
                    parts=ranges(concurrency), transfer=busy_transfer, parallel=True
                )
            )
            try:
                await asyncio.wait_for(started.wait(), timeout=2)
                assert (
                    await asyncio.wait_for(
                        pool.copy(
                            parts=ranges(0), transfer=empty_transfer, parallel=False
                        ),
                        timeout=2,
                    )
                    == []
                )
                assert not busy.done()
                assert len(active) == concurrency
                assert len(readers.opened) == concurrency
            finally:
                release.set()
                await busy
        assert set(readers.closed) == set(readers.opened)

    asyncio.run(scenario())
