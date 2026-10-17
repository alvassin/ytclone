"""Parallel table export keeps range order, snapshot lifetime and resource bounds."""

import asyncio
from collections import Counter, defaultdict
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
import zstandard
from test_engine import MemoryStorage, MemoryYt, make_node
from test_parallel_worker import MemoryWriter, feed
from yt import yson

from ytclone import engine
from ytclone.engine import CopyOptions, export_tree, restore_tree
from ytclone.format import Manifest, node_prefix
from ytclone.interfaces import RetryableReadError
from ytclone.parallel.worker import serve_jobs
from ytclone.progress import Progress

SOURCE = "//source"
PREFIX = "archive"
DESTINATION = "//restored"


async def wait_until(predicate):
    """Wait for a behavioral checkpoint without relying on transfer timing."""

    async def wait():
        while not predicate():
            await asyncio.sleep(0)

    await asyncio.wait_for(wait(), timeout=5)


class GatedYt(MemoryYt):
    """Allow independent ranges to finish in an explicitly controlled order."""

    def __init__(self, *, nodes):
        super().__init__(nodes=nodes, events=[])
        self.gates = defaultdict(asyncio.Event)
        self.cleanup_gates = {}
        self.cleanup_started = set()
        self.read_tokens = []
        self.open_ranges = Counter()
        self.max_ranges = Counter()
        self.max_total_ranges = 0
        self.snapshot_exit_open_ranges = []
        self.auto_release = False
        self.rows = {
            node.path: [
                {
                    b"id": index,
                    b"binary": b"\x00\xff" + str(index).encode(),
                    b"unsigned": yson.YsonUint64(2**63 + index),
                    b"nested": {b"value": [None, True, -1, 2.5]},
                }
                for index in range(node.row_count)
            ]
            for node in nodes
        }

    def release_all(self):
        self.auto_release = True
        for gate in self.gates.values():
            gate.set()

    @asynccontextmanager
    async def snapshot(self, *, path):
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        self.events.append(("snapshot_enter", path))
        node = self.nodes[path]
        rows = tuple(self.rows[path])
        token = object()

        async def read_part(*, part, compression_level, chunk_size):
            key = (path, part.index)
            self.reads.append(key)
            self.read_tokens.append(token)
            self.open_ranges[path] += 1
            self.max_ranges[path] = max(self.max_ranges[path], self.open_ranges[path])
            self.max_total_ranges = max(
                self.max_total_ranges, sum(self.open_ranges.values())
            )
            self.read_started.set()
            try:
                if not self.auto_release:
                    await self.gates[key].wait()
                failures = self.read_failures.get(key, [])
                if failures:
                    yield b"incomplete compressed stream"
                    raise failures.pop(0)
                if node.kind == "table":
                    data = yson.dumps(
                        object=rows[part.start : part.stop],
                        yson_type="list_fragment",
                        yson_format="binary",
                    )
                else:
                    data = b"x" * (part.stop - part.start)
                payload = zstandard.ZstdCompressor(level=compression_level).compress(
                    data
                )
                yield payload[:3]
                await asyncio.sleep(0)
                yield payload[3:]
            finally:
                self.cleanup_started.add(key)
                if key in self.cleanup_gates:
                    await self.cleanup_gates[key].wait()
                self.open_ranges[path] -= 1
                self.closed_reads.append(key)

        try:
            yield SimpleNamespace(node=node, read_part=read_part)
            if path in self.fail_snapshot_exit:
                raise OSError("snapshot expired")
        finally:
            self.snapshot_exit_open_ranges.append(self.open_ranges[path])
            self.active -= 1
            self.events.append(("snapshot_exit", path))


async def export(*, yt, storage, options, progress=None):
    return await export_tree(
        yt=yt,
        storage=storage,
        source=SOURCE,
        prefix=PREFIX,
        cluster="test-cluster",
        options=options,
        progress=progress,
    )


def manifest_key(*, path=SOURCE):
    return node_prefix(prefix=PREFIX, path=path) + "/@manifest.json"


def test_out_of_order_parts_restore_exact_rows_and_credit_only_completed_ranges():
    """Finishing the final range first must not imply all rows were uploaded."""

    async def scenario():
        yt = GatedYt(nodes=[make_node(kind="table", size=9, disk_size=360)])
        expected = yt.rows[SOURCE].copy()
        storage = MemoryStorage(events=yt.events)
        options = CopyOptions(concurrency=1, part_concurrency=3, part_size=3)
        progress = Progress()
        task = asyncio.create_task(
            export(yt=yt, storage=storage, options=options, progress=progress)
        )
        try:
            await wait_until(lambda: len(yt.reads) == 3)
            # Replacing the live source after pinning must not change any range.
            yt.rows[SOURCE] = [{b"wrong": True}] * 9
            yt.nodes[SOURCE] = make_node(kind="table", size=9, revision=2)
            for index, copied in [(2, 120), (1, 240)]:
                yt.gates[(SOURCE, index)].set()
                await wait_until(
                    lambda copied=copied: progress.source_copied_bytes == copied
                )
                assert progress.source_partial_nodes == pytest.approx(copied / 360)
                assert await storage.read_bytes(key=manifest_key()) is None
            yt.gates[(SOURCE, 0)].set()
            result = await asyncio.wait_for(task, timeout=5)
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

        assert (result.completed, result.failed) == (1, 0)
        assert [index for _, index in yt.closed_reads] == [2, 1, 0]
        assert yt.max_total_ranges == 3
        assert yt.snapshot_exit_open_ranges == [0]
        assert progress.source_copied_bytes == 360
        assert progress.source_partial_nodes == 0
        manifest = Manifest.from_bytes(await storage.read_bytes(key=manifest_key()))
        assert [(part.start, part.stop) for part in manifest.parts] == [
            (0, 3),
            (3, 6),
            (6, 9),
        ]
        assert manifest.source["content_revision"] == 1

        result = await restore_tree(
            yt=yt,
            storage=storage,
            source_prefix=node_prefix(prefix=PREFIX, path=SOURCE),
            destination=DESTINATION,
            options=options,
        )
        assert (result.completed, result.failed) == (1, 0)
        restored = []
        for payload in yt.restored[DESTINATION][1]:
            restored.extend(
                yson.loads(
                    string=zstandard.ZstdDecompressor().decompress(payload),
                    yson_type="list_fragment",
                    encoding=None,
                )
            )
        assert restored == expected
        assert all(isinstance(row[b"unsigned"], yson.YsonUint64) for row in restored)
        assert all(bytes(row[b"binary"]).startswith(b"\x00\xff") for row in restored)

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("node_concurrency", "part_concurrency", "max_streams", "expected_active"),
    [(2, 3, 4, 4), (2, 3, 1, 1), (2, 3, 0, 3), (3, 1, 0, 3)],
)
def test_total_stream_limit_is_shared_across_tables(
    node_concurrency, part_concurrency, max_streams, expected_active
):
    """Node and part parallelism must never multiply the process stream budget."""

    async def scenario():
        nodes = [
            make_node(path=f"{SOURCE}/{index}", kind="table", size=60)
            for index in range(3)
        ]
        yt = GatedYt(nodes=nodes)
        storage = MemoryStorage(events=yt.events)
        options = CopyOptions(
            concurrency=node_concurrency,
            part_concurrency=part_concurrency,
            max_streams=max_streams,
            part_size=3,
        )
        task = asyncio.create_task(export(yt=yt, storage=storage, options=options))
        try:
            await wait_until(lambda: sum(yt.open_ranges.values()) == expected_active)
            # Give every worker a scheduling turn while the active reads are held.
            for _ in range(20):
                await asyncio.sleep(0)
            assert len(yt.reads) == expected_active
            assert max(yt.open_ranges.values()) <= part_concurrency
            yt.release_all()
            result = await asyncio.wait_for(task, timeout=5)
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        assert (result.completed, result.failed) == (3, 0)
        assert yt.max_total_ranges == expected_active
        assert max(yt.max_ranges.values()) <= part_concurrency
        assert yt.snapshot_exit_open_ranges == [0, 0, 0]
        assert Counter(yt.reads) == Counter(
            (node.path, index) for node in nodes for index in range(20)
        )

    asyncio.run(scenario())


def test_large_files_remain_sequential_with_table_parallelism_enabled():
    async def scenario():
        yt = GatedYt(nodes=[make_node(kind="file", size=12)])
        storage = MemoryStorage(events=yt.events)
        options = CopyOptions(concurrency=1, part_concurrency=4, part_size=3)
        task = asyncio.create_task(export(yt=yt, storage=storage, options=options))
        try:
            await wait_until(lambda: bool(yt.reads))
            for _ in range(20):
                await asyncio.sleep(0)
            assert yt.reads == [(SOURCE, 0)]
            yt.release_all()
            result = await asyncio.wait_for(task, timeout=5)
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        assert (result.completed, result.failed) == (1, 0)
        assert yt.max_total_ranges == 1
        assert yt.reads == [(SOURCE, index) for index in range(4)]

    asyncio.run(scenario())


def test_parallel_retry_preserves_completed_ranges_and_the_original_snapshot(
    monkeypatch,
):
    monkeypatch.setattr(engine, "PART_RETRY_PAUSE", 0)

    async def scenario():
        yt = GatedYt(nodes=[make_node(kind="table", size=9, disk_size=360)])
        yt.read_failures[(SOURCE, 1)] = [RetryableReadError("connection reset")]
        storage = MemoryStorage(events=yt.events)
        progress = Progress()
        task = asyncio.create_task(
            export(
                yt=yt,
                storage=storage,
                options=CopyOptions(concurrency=1, part_concurrency=3, part_size=3),
                progress=progress,
            )
        )
        try:
            await wait_until(lambda: len(yt.reads) == 3)
            yt.gates[(SOURCE, 0)].set()
            yt.gates[(SOURCE, 2)].set()
            await wait_until(lambda: progress.source_copied_bytes == 240)
            yt.gates[(SOURCE, 1)].set()
            result = await asyncio.wait_for(task, timeout=5)
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        assert (result.completed, result.failed) == (1, 0)
        assert Counter(yt.reads) == Counter(
            {(SOURCE, 0): 1, (SOURCE, 1): 2, (SOURCE, 2): 1}
        )
        assert len(set(yt.read_tokens)) == 1
        assert Counter(yt.closed_reads) == Counter(yt.reads)
        assert yt.events.count(("snapshot_enter", SOURCE)) == 1
        assert yt.events.count(("snapshot_exit", SOURCE)) == 1
        assert progress.source_copied_bytes == 360
        assert progress.source_partial_nodes == 0

    asyncio.run(scenario())


@pytest.mark.parametrize("failure", ["fatal_read", "cancel"])
def test_failed_parallel_export_drains_readers_before_snapshot_exit(failure):
    """A failed or cancelled table is never published, even after some successes."""

    async def scenario():
        yt = GatedYt(nodes=[make_node(kind="table", size=9, disk_size=360)])
        if failure == "fatal_read":
            yt.read_failures[(SOURCE, 1)] = [ValueError("snapshot lost")]
        cleanup_gate = asyncio.Event()
        yt.cleanup_gates[(SOURCE, 0)] = cleanup_gate
        storage = MemoryStorage(events=yt.events)
        progress = Progress()
        task = asyncio.create_task(
            export(
                yt=yt,
                storage=storage,
                options=CopyOptions(concurrency=1, part_concurrency=3, part_size=3),
                progress=progress,
            )
        )
        try:
            await wait_until(lambda: len(yt.reads) == 3)
            yt.gates[(SOURCE, 2)].set()
            await wait_until(lambda: progress.source_copied_bytes == 120)
            if failure == "fatal_read":
                yt.gates[(SOURCE, 1)].set()
            else:
                task.cancel()
            await wait_until(lambda: (SOURCE, 0) in yt.cleanup_started)
            assert not task.done()
            assert ("snapshot_exit", SOURCE) not in yt.events
            assert await storage.read_bytes(key=manifest_key()) is None
            cleanup_gate.set()
            if failure == "fatal_read":
                result = await asyncio.wait_for(task, timeout=5)
                assert (result.completed, result.failed) == (0, 1)
            else:
                with pytest.raises(asyncio.CancelledError):
                    await asyncio.wait_for(task, timeout=5)
        finally:
            cleanup_gate.set()
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        assert yt.snapshot_exit_open_ranges == [0]
        assert yt.active == 0
        assert Counter(yt.closed_reads) == Counter(yt.reads)
        assert len(yt.reads) == 3
        assert await storage.read_bytes(key=manifest_key()) is None
        assert progress.source_copied_bytes == progress.source_partial_nodes == 0
        assert progress.completed_bytes == progress.completed == 0

    asyncio.run(scenario())


def test_snapshot_failure_after_all_parallel_uploads_does_not_publish():
    async def scenario():
        yt = GatedYt(nodes=[make_node(kind="table", size=9, disk_size=360)])
        yt.release_all()
        yt.fail_snapshot_exit.add(SOURCE)
        storage = MemoryStorage(events=yt.events)
        progress = Progress()
        result = await export(
            yt=yt,
            storage=storage,
            options=CopyOptions(concurrency=1, part_concurrency=3, part_size=3),
            progress=progress,
        )
        assert (result.completed, result.failed) == (0, 1)
        assert len(yt.closed_reads) == 3
        assert yt.snapshot_exit_open_ranges == [0]
        assert await storage.read_bytes(key=manifest_key()) is None
        assert progress.source_copied_bytes == progress.source_partial_nodes == 0
        assert progress.completed_bytes == 0

    asyncio.run(scenario())


def test_subprocess_worker_shares_one_stream_limit_between_assigned_tables():
    """The supervisor's node pool must use the same stream budget as export_tree."""

    async def scenario():
        yt = GatedYt(
            nodes=[
                make_node(path=f"{SOURCE}/{index}", kind="table", size=18)
                for index in (1, 2)
            ]
        )
        storage = MemoryStorage(events=yt.events)
        reader, writer = asyncio.StreamReader(), MemoryWriter()
        for index in (1, 2):
            feed(reader=reader, job_id=index)
        feed(reader=reader, message_type="stop")
        task = asyncio.create_task(
            serve_jobs(
                reader=reader,
                writer=writer,
                yt=yt,
                storage=storage,
                prefix=PREFIX,
                cluster="test-cluster",
                options=CopyOptions(
                    concurrency=2, part_concurrency=3, max_streams=4, part_size=3
                ),
                progress=Progress(),
                progress_interval=100,
            )
        )
        try:
            await wait_until(lambda: sum(yt.open_ranges.values()) == 4)
            for _ in range(20):
                await asyncio.sleep(0)
            assert len(yt.reads) == 4
            assert max(yt.open_ranges.values()) <= 3
            yt.release_all()
            await asyncio.wait_for(task, timeout=5)
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        assert yt.max_total_ranges == 4
        assert max(yt.max_ranges.values()) <= 3
        assert yt.snapshot_exit_open_ranges == [0, 0]
        results = [
            message for message in writer.messages if message["type"] == "result"
        ]
        assert {message["id"] for message in results} == {1, 2}
        assert all(message["status"] == "completed" for message in results)

    asyncio.run(scenario())


def test_stream_budget_is_held_until_destination_finishes_upload():
    """Storage backpressure consumes a slot even after the source reached EOF."""

    async def scenario():
        yt = GatedYt(nodes=[make_node(kind="table", size=12)])
        yt.release_all()
        storage = MemoryStorage(events=yt.events)
        original_upload = storage.upload
        release_uploads = asyncio.Event()
        uploading = peak = 0

        async def delayed_upload(*, key, chunks):
            nonlocal uploading, peak
            uploading += 1
            peak = max(peak, uploading)
            try:
                ref = await original_upload(key=key, chunks=chunks)
                await release_uploads.wait()
                return ref
            finally:
                uploading -= 1

        storage.upload = delayed_upload
        task = asyncio.create_task(
            export(
                yt=yt,
                storage=storage,
                options=CopyOptions(
                    concurrency=1, part_concurrency=4, max_streams=2, part_size=3
                ),
            )
        )
        try:
            await wait_until(lambda: len(yt.closed_reads) >= 2)
            for _ in range(20):
                await asyncio.sleep(0)
            assert uploading == peak == 2
            assert len(yt.reads) == 2
            assert await storage.read_bytes(key=manifest_key()) is None
            release_uploads.set()
            result = await asyncio.wait_for(task, timeout=5)
        finally:
            release_uploads.set()
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        assert (result.completed, result.failed) == (1, 0)
        assert peak == 2
        assert len(yt.reads) == 4

    asyncio.run(scenario())
