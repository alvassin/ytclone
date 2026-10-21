"""Copy orchestration contracts, with in-memory storage and transactional YT."""

import asyncio
import hashlib
import json
import logging
import re
from contextlib import aclosing, asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest

from ytclone import engine
from ytclone.engine import CopyOptions, export_node, export_tree, restore_tree
from ytclone.export_pool import PartPool
from ytclone.format import (
    DiscoveredNode,
    Manifest,
    Node,
    ObjectRef,
    Range,
    canonical_attributes,
    node_prefix,
)
from ytclone.interfaces import PublicationConflict, RetryableReadError, StoredBytes
from ytclone.progress import Progress

PREFIX = "archive"
SOURCE = "//source"
DESTINATION = "//restored"


def make_node(
    *,
    path=SOURCE,
    kind="file",
    size=12,
    revision=1,
    attribute_revision=1,
    dynamic=False,
    disk_size=None,
):
    return Node(
        path=path,
        kind=kind,
        node_id="id-" + path,
        content_revision=revision,
        attribute_revision=attribute_revision,
        attributes=canonical_attributes({b"description": b"saved\xff"}),
        row_count=size if kind == "table" else 0,
        data_size=size if kind == "file" else 0,
        data_weight=size,
        dynamic=dynamic,
        disk_size=disk_size,
    )


def discovered_node(node):
    """Capture the metadata carried from discovery to a transfer worker."""
    return DiscoveredNode(
        path=node.path,
        kind=node.kind,
        node_id=node.node_id,
        content_revision=node.content_revision,
        attribute_revision=node.attribute_revision,
        dynamic=node.dynamic,
        disk_size=node.disk_size,
    )


def object_ref(*, key, data):
    return ObjectRef(key=key, size=len(data), sha256=hashlib.sha256(data).hexdigest())


class MemoryStorage:
    def __init__(self, *, events):
        self.objects = {}
        self.events = events
        self.upload_failures = set()
        self.list_failure_after = None
        self.downloaded = []
        self.reads = []
        self.checked = []

    async def read_bytes(self, *, key):
        self.reads.append(key)
        return self.objects.get(key)

    async def read_versioned(self, *, key):
        data = await self.read_bytes(key=key)
        if data is None:
            return None
        return StoredBytes(data=data, etag=hashlib.sha256(data).hexdigest())

    async def put_conditional(self, *, key, data, etag):
        current = self.objects.get(key)
        actual = hashlib.sha256(current).hexdigest() if current is not None else None
        if actual != etag:
            raise PublicationConflict("Manifest changed")
        await self.put_bytes(key=key, data=data)

    async def put_bytes(self, *, key, data):
        self.events.append(("put", key))
        self.objects[key] = data
        return object_ref(key=key, data=data)

    async def upload(self, *, key, chunks):
        self.events.append(("upload", key))
        data = bytearray()
        async for chunk in chunks:
            flat_key = re.sub(r"/@generations/[a-f0-9]{32}/", "/", key)
            if flat_key in self.upload_failures:
                raise OSError("upload interrupted")
            data.extend(chunk)

        self.objects[key] = bytes(data)
        return object_ref(key=key, data=data)

    async def delete(self, *, key):
        self.events.append(("delete", key))
        self.objects.pop(key, None)

    async def exists(self, *, ref):
        self.checked.append(ref.key)
        data = self.objects.get(ref.key)
        return data is not None and len(data) == ref.size

    async def list_keys(self, *, prefix):
        for index, key in enumerate(sorted(self.objects)):
            if not key.startswith(prefix):
                continue
            if self.list_failure_after == index:
                raise OSError("listing interrupted")
            # Each yield may be a page boundary; callers must consume to EOF.
            await asyncio.sleep(0)
            yield key

    async def download(self, *, ref):
        self.downloaded.append(ref.key)
        data = self.objects[ref.key]
        yield data[:2]
        await asyncio.sleep(0)
        yield data[2:]
        if object_ref(key=ref.key, data=data) != ref:
            raise ValueError("download checksum mismatch")


class MemoryYt:
    def __init__(self, *, nodes, events):
        self.nodes = {node.path: node for node in nodes}
        self.events = events
        self.reads = []
        self.read_failures = {}
        self.closed_reads = []
        self.discovered = []
        self.discovery_overrides = {}
        self.discovery_done = False
        self.discovery_failure = False
        self.fail_snapshot_exit = set()
        self.active = 0
        self.max_active = 0
        self.read_started = asyncio.Event()
        self.release_read = None
        self.restored = {}
        self.fingerprints = {}
        self.aborted = []
        self.workers = []
        self.fail_acquire_index = None
        self.acquire_gate = None
        self.fail_producer_cleanup = False
        self.cleanup_gate = None
        self.discovery_waiting = asyncio.Event()

    @asynccontextmanager
    async def worker(self):
        """Lease a distinct backend over the shared in-memory YT state."""
        worker = MemoryYtWorker(owner=self)
        index = len(self.workers)
        self.workers.append(worker)
        try:
            if index == self.fail_acquire_index:
                if self.acquire_gate is not None:
                    await self.acquire_gate.wait()
                raise OSError("worker acquisition failed")
            yield worker
        finally:
            worker.closed = True
            if self.fail_producer_cleanup and any(
                method == "walk" for method, _ in worker.calls
            ):
                if self.cleanup_gate is not None:
                    await self.cleanup_gate.wait()
                raise OSError("producer cleanup failed")

    async def walk(self, *, path):
        for source in self.nodes:
            if source == path or source.startswith(path + "/"):
                self.discovered.append(source)
                if len(self.discovered) >= 2:
                    self.discovery_waiting.set()
                yield self.discovery_overrides.get(
                    source, discovered_node(self.nodes[source])
                )
                await asyncio.sleep(0)

        if self.discovery_failure:
            raise OSError("source listing incomplete")
        self.discovery_done = True

    @asynccontextmanager
    async def snapshot(self, *, path):
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        self.events.append(("snapshot_enter", path))

        try:
            yield SimpleNamespace(node=self.nodes[path])
            if path in self.fail_snapshot_exit:
                raise OSError("snapshot expired")
        finally:
            self.active -= 1
            self.events.append(("snapshot_exit", path))

    async def read_part(self, *, snapshot, part, compression_level, chunk_size):
        path = snapshot.node.path
        self.reads.append((path, part.index))
        self.read_started.set()
        try:
            if self.release_read is not None:
                await self.release_read.wait()
            failures = self.read_failures.get((path, part.index), [])
            if failures:
                yield b"x"
                raise failures.pop(0)
            yield b"x" * (part.stop - part.start)
        finally:
            self.closed_reads.append((path, part.index))

    async def ensure_directory(self, *, path):
        self.events.append(("ensure_directory", path))
        self.restored.setdefault(
            path, (make_node(path=path, kind="map_node", size=0), [])
        )

    async def matches(self, *, path, fingerprint):
        return self.fingerprints.get(path) == fingerprint

    @asynccontextmanager
    async def restore(self, *, path, node, fingerprint):
        self.events.append(("restore_enter", path))
        parts = []

        async def write_part(*, chunks):
            data = bytearray()
            async for chunk in chunks:
                data.extend(chunk)
            parts.append(bytes(data))

        try:
            yield SimpleNamespace(write_part=write_part)
        except BaseException:
            self.aborted.append(path)
            raise
        else:
            self.restored[path] = (node, parts)
            self.fingerprints[path] = fingerprint
            self.events.append(("restore_commit", path))


class MemoryYtWorker:
    """Record which operations use each independently leased backend."""

    def __init__(self, *, owner):
        """Bind shared state and track this worker's lifetime."""
        self.owner = owner
        self.calls = []
        self.restore_kinds = {}
        self.closed = False

    def record(self, *, method, path):
        """Reject operations after closing and record backend ownership."""
        assert not self.closed
        self.calls.append((method, path))

    async def walk(self, *, path):
        """Traverse shared nodes through this backend."""
        self.record(method="walk", path=path)
        async for child in self.owner.walk(path=path):
            yield child

    @asynccontextmanager
    async def snapshot(self, *, path):
        """Open a source snapshot through this backend."""
        self.record(method="snapshot", path=path)
        async with self.owner.snapshot(path=path) as snapshot:
            yield snapshot

    async def read_part(self, *, snapshot, **kwargs):
        self.record(method="read_part", path=snapshot.node.path)
        async with aclosing(
            self.owner.read_part(snapshot=snapshot, **kwargs)
        ) as chunks:
            async for chunk in chunks:
                yield chunk

    async def ensure_directory(self, *, path):
        """Create a missing directory through this backend."""
        self.record(method="ensure_directory", path=path)
        await self.owner.ensure_directory(path=path)

    async def matches(self, *, path, fingerprint):
        """Check destination state through this backend."""
        self.record(method="matches", path=path)
        return await self.owner.matches(path=path, fingerprint=fingerprint)

    @asynccontextmanager
    async def restore(self, *, path, node, fingerprint):
        """Restore a node through this backend."""
        self.record(method="restore", path=path)
        self.restore_kinds[path] = node.kind
        async with self.owner.restore(
            path=path, node=node, fingerprint=fingerprint
        ) as sink:
            yield sink


def setup(*, nodes=None):
    events = []
    storage = MemoryStorage(events=events)
    yt = MemoryYt(nodes=[make_node()] if nodes is None else nodes, events=events)
    return yt, storage, events


def options(*, concurrency=1, queue_size=1):
    return CopyOptions(concurrency=concurrency, queue_size=queue_size, part_size=4)


async def export(
    *, yt, storage, source=SOURCE, prefix=PREFIX, copy_options=None, progress=None
):
    return await export_tree(
        yt=yt,
        storage=storage,
        source=source,
        prefix=prefix,
        cluster="local-test",
        options=options() if copy_options is None else copy_options,
        progress=progress,
    )


async def restore(*, yt, storage, source=SOURCE, prefix=PREFIX, progress=None):
    return await restore_tree(
        yt=yt,
        storage=storage,
        source_prefix=node_prefix(prefix=prefix, path=source),
        destination=DESTINATION,
        options=options(),
        progress=progress,
    )


def manifest_key(*, path=SOURCE):
    return node_prefix(prefix=PREFIX, path=path).rstrip("/") + "/@manifest.json"


def test_export_publishes_after_snapshot_and_skips_only_complete_nodes():
    async def scenario():
        yt, storage, events = setup()
        result = await export(yt=yt, storage=storage)
        assert (result.completed, result.failed) == (1, 0)
        key = manifest_key()
        assert events.index(("snapshot_exit", SOURCE)) < events.index(("put", key))
        manifest = Manifest.from_bytes(storage.objects[key])
        assert len(manifest.parts) == 3
        first_reads = list(yt.reads)

        result = await export(yt=yt, storage=storage)
        assert (result.skipped, result.failed) == (1, 0)
        assert yt.reads == first_reads

        del storage.objects[manifest.parts[0].object.key]
        result = await export(yt=yt, storage=storage)
        assert (result.completed, result.skipped, result.failed) == (1, 0, 0)
        assert len(yt.reads) == len(first_reads) * 2

    asyncio.run(scenario())


def test_changed_node_publishes_before_cleaning_only_its_predecessor():
    async def scenario():
        yt, storage, events = setup()
        await export(yt=yt, storage=storage)
        key = manifest_key()
        old = Manifest.from_bytes(storage.objects[key])
        sibling_key = "archive/source-other/@data/part-000000.bin.zst"
        storage.objects[sibling_key] = b"keep"
        events.clear()
        yt.nodes[SOURCE] = make_node(size=2, revision=2)

        result = await export(yt=yt, storage=storage)
        assert result.completed == 1 and result.failed == 0
        manifest = Manifest.from_bytes(storage.objects[key])
        assert len(manifest.parts) == 1
        assert manifest.generation != old.generation
        assert ("delete", key) not in events
        assert events.index(("put", key)) < events.index(("delete", old.attributes.key))
        assert events.index(("snapshot_exit", SOURCE)) < events.index(("put", key))
        assert all(part.object.key not in storage.objects for part in old.parts)
        assert old.attributes.key not in storage.objects
        assert storage.objects[sibling_key] == b"keep"

    asyncio.run(scenario())


@pytest.mark.parametrize("failure", ["upload", "snapshot_exit"])
def test_failed_export_has_no_manifest_and_can_be_retried(failure):
    async def scenario():
        yt, storage, _ = setup()
        data_key = "archive/source/@data/part-000000.bin.zst"
        if failure == "upload":
            storage.upload_failures.add(data_key)
        else:
            yt.fail_snapshot_exit.add(SOURCE)

        result = await export(yt=yt, storage=storage)
        assert (result.completed, result.failed) == (0, 1)
        assert manifest_key() not in storage.objects
        assert yt.active == 0

        storage.upload_failures.clear()
        yt.fail_snapshot_exit.clear()
        result = await export(yt=yt, storage=storage)
        assert (result.completed, result.failed) == (1, 0)
        assert manifest_key() in storage.objects

    asyncio.run(scenario())


@pytest.mark.parametrize("kind", ["table", "file"])
def test_part_retry_restarts_only_failed_range_in_same_snapshot(
    kind, monkeypatch, caplog
):
    """Retry a partial read without redoing prior ranges or doubling progress."""
    monkeypatch.setattr(engine, "PART_RETRY_PAUSE", 0)

    async def scenario():
        """Fail the second range once, then publish one complete archive."""
        yt, storage, events = setup(nodes=[make_node(kind=kind, disk_size=240)])
        yt.read_failures[(SOURCE, 1)] = [RetryableReadError("connection reset")]
        progress = Progress()
        original_upload = storage.upload
        observations = []

        async def observe_upload(*, key, chunks):
            """Record source credit and verify prior streams are already closed."""
            assert yt.closed_reads == yt.reads
            observations.append(
                (progress.source_copied_bytes, progress.source_partial_nodes)
            )
            return await original_upload(key=key, chunks=chunks)

        storage.upload = observe_upload
        result = await export(yt=yt, storage=storage, progress=progress)

        assert (result.completed, result.failed) == (1, 0)
        assert yt.reads == [(SOURCE, index) for index in (0, 1, 1, 2)]
        assert yt.closed_reads == yt.reads
        assert events.count(("snapshot_enter", SOURCE)) == 1
        assert events.count(("snapshot_exit", SOURCE)) == 1
        assert observations == pytest.approx(
            [(0, 0), (80, 1 / 3), (80, 1 / 3), (160, 2 / 3)]
        )
        assert progress.source_copied_bytes == 240
        assert progress.source_partial_nodes == 0
        assert progress.completed_bytes == 12

        manifest = Manifest.from_bytes(storage.objects[manifest_key()])
        assert [(part.start, part.stop) for part in manifest.parts] == [
            (0, 4),
            (4, 8),
            (8, 12),
        ]
        assert [storage.objects[part.object.key] for part in manifest.parts] == [
            b"xxxx",
            b"xxxx",
            b"xxxx",
        ]
        assert all(
            part.object == object_ref(key=part.object.key, data=b"xxxx")
            for part in manifest.parts
        )

    with caplog.at_level(logging.INFO, logger="ytclone.engine"):
        asyncio.run(scenario())

    retries = [
        record.getMessage()
        for record in caplog.records
        if "part_read_failed" in record.getMessage()
    ]
    assert len(retries) == 1
    assert f"path={SOURCE}" in retries[0]
    assert "part=1" in retries[0]
    assert "attempt=1/3" in retries[0]
    assert "connection reset" in retries[0]


def test_part_retry_exhaustion_fails_without_manifest(monkeypatch):
    """Three failed attempts leave prior objects but never a completed node."""
    monkeypatch.setattr(engine, "PART_RETRY_PAUSE", 0)

    async def scenario():
        """Exhaust the second part and roll back the first part's ETA credit."""
        yt, storage, events = setup(nodes=[make_node(disk_size=240)])
        yt.read_failures[(SOURCE, 1)] = [
            RetryableReadError("connection reset") for _ in range(3)
        ]
        progress = Progress()
        result = await export(yt=yt, storage=storage, progress=progress)

        assert (result.completed, result.failed) == (0, 1)
        assert yt.reads == [(SOURCE, index) for index in (0, 1, 1, 1)]
        assert yt.closed_reads == yt.reads
        assert events.count(("snapshot_enter", SOURCE)) == 1
        assert events.count(("snapshot_exit", SOURCE)) == 1
        assert yt.active == 0
        assert [
            value for key, value in storage.objects.items() if "/@data/" in key
        ] == [b"xxxx"]
        assert manifest_key() not in storage.objects
        assert progress.failed == 1
        assert progress.source_observed_bytes == 240
        assert progress.source_copied_bytes == progress.source_partial_nodes == 0
        assert progress.completed == progress.completed_bytes == 0

    asyncio.run(scenario())


@pytest.mark.parametrize("failure", ["permanent_read", "cancelled_read", "s3_timeout"])
def test_part_copy_does_not_retry_unmarked_errors_or_cancellation(failure, monkeypatch):
    """Only explicitly retryable YT read failures can restart an entire part."""
    monkeypatch.setattr(engine, "PART_RETRY_PAUSE", 0)

    async def scenario():
        """Propagate the original error and close the consumed source stream."""
        yt, storage, events = setup()
        if failure == "s3_timeout":
            error = asyncio.TimeoutError("S3 upload timed out")

            async def fail_upload(*, key, chunks):
                """Fail at the destination after consuming some source bytes."""
                events.append(("upload", key))
                await chunks.__anext__()
                raise error

            storage.upload = fail_upload
        else:
            error = (
                asyncio.CancelledError()
                if failure == "cancelled_read"
                else ValueError("snapshot expired")
            )
            yt.read_failures[(SOURCE, 0)] = [error]

        async with yt.snapshot(path=SOURCE) as snapshot:
            with pytest.raises(type(error)) as caught:
                await engine.copy_part(
                    reader=yt,
                    snapshot=snapshot,
                    storage=storage,
                    key="part",
                    part=Range(index=0, start=0, stop=4),
                    compression_level=3,
                    chunk_size=32 * 1024 * 1024,
                )

        assert caught.value is error
        assert yt.reads == yt.closed_reads == [(SOURCE, 0)]
        assert events.count(("upload", "part")) == 1
        assert storage.objects == {}
        assert yt.active == 0

    asyncio.run(scenario())


def test_dynamic_is_skipped_links_fail_and_other_nodes_complete():
    async def scenario():
        nodes = [
            make_node(kind="map_node", size=0),
            make_node(path=SOURCE + "/dynamic", kind="table", dynamic=True),
            make_node(path=SOURCE + "/link", kind="link"),
            make_node(path=SOURCE + "/file"),
        ]
        yt, storage, _ = setup(nodes=nodes)
        result = await export(yt=yt, storage=storage)
        assert (result.completed, result.skipped, result.failed) == (2, 1, 1)
        assert {path for path, _ in yt.reads} == {SOURCE + "/file"}

        result = await export(yt=yt, storage=storage)
        assert result.failed == 1 and result.skipped == 3
        assert len(yt.discovered) == 8

    asyncio.run(scenario())


def test_discovery_failure_does_not_discard_already_queued_work():
    async def scenario():
        yt, storage, _ = setup()
        yt.discovery_failure = True
        progress = Progress()
        result = await export(yt=yt, storage=storage, progress=progress)
        assert result.completed == 1 and result.failed >= 1
        assert progress.discovered == 1
        assert progress.completed == result.completed
        assert progress.failed == result.failed
        assert not progress.discovery_complete
        assert manifest_key() in storage.objects

    asyncio.run(scenario())


def test_discovery_overlaps_bounded_workers_and_cancellation_closes_snapshots():
    async def scenario():
        nodes = [make_node(path=SOURCE + f"/{index}") for index in range(30)]
        yt, storage, _ = setup(nodes=nodes)
        yt.release_read = asyncio.Event()
        progress = Progress()
        task = asyncio.create_task(
            export(
                yt=yt,
                storage=storage,
                copy_options=options(concurrency=2, queue_size=1),
                progress=progress,
            )
        )
        await asyncio.wait_for(yt.read_started.wait(), timeout=2)
        for _ in range(10):
            await asyncio.sleep(0)

        assert not yt.discovery_done
        assert not progress.discovery_complete
        assert len(yt.discovered) <= 4
        assert yt.max_active <= 2
        assert progress.active == 2
        assert progress.queued == 1
        assert progress.discovered == len(yt.discovered)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=2)
        assert yt.active == 0
        assert progress.active == progress.queued == 0
        assert progress.completed_bytes == 0
        assert len(yt.workers) == 5
        assert all(worker.closed for worker in yt.workers)

    asyncio.run(scenario())


def test_restore_preserves_siblings_orders_directories_and_skips_completed_nodes():
    async def scenario():
        nodes = [
            make_node(kind="map_node", size=0),
            make_node(path=SOURCE + "/nested", kind="map_node", size=0),
            make_node(path=SOURCE + "/nested/table", kind="table", size=5),
            make_node(path=SOURCE + "/empty", kind="file", size=0),
        ]
        yt, storage, events = setup(nodes=nodes)
        await export(yt=yt, storage=storage)
        yt.restored[DESTINATION + "/unrelated"] = "keep"
        result = await restore(yt=yt, storage=storage)
        assert (result.completed, result.failed) == (4, 0)
        assert yt.restored[DESTINATION + "/unrelated"] == "keep"
        assert yt.restored[DESTINATION + "/empty"][1] == []
        table_node, parts = yt.restored[DESTINATION + "/nested/table"]
        assert table_node.row_count == 5 and b"".join(parts) == b"xxxxx"
        assert events.index(("restore_commit", DESTINATION)) < events.index(
            ("restore_enter", DESTINATION + "/nested")
        )
        assert events.index(("restore_commit", DESTINATION + "/nested")) < events.index(
            ("restore_enter", DESTINATION + "/nested/table")
        )
        downloaded = list(storage.downloaded)

        result = await restore(yt=yt, storage=storage)
        assert (result.skipped, result.failed) == (4, 0)
        assert storage.downloaded == downloaded

    asyncio.run(scenario())


@pytest.mark.parametrize("corrupt", ["attributes", "data"])
def test_corrupt_archive_never_commits_restore(corrupt):
    async def scenario():
        yt, storage, _ = setup()
        await export(yt=yt, storage=storage)
        manifest = Manifest.from_bytes(storage.objects[manifest_key()])
        ref = (
            manifest.attributes if corrupt == "attributes" else manifest.parts[0].object
        )
        data = storage.objects[ref.key]
        storage.objects[ref.key] = bytes([data[0] ^ 1]) + data[1:]

        result = await restore(yt=yt, storage=storage)
        assert result.failed == 1 and result.completed == 0
        assert DESTINATION not in yt.restored
        if corrupt == "data":
            assert yt.aborted == [DESTINATION]

    asyncio.run(scenario())


def test_restore_rejects_reference_outside_node_before_reading_it():
    async def scenario():
        yt, storage, _ = setup()
        await export(yt=yt, storage=storage)
        key = manifest_key()
        manifest = Manifest.from_bytes(storage.objects[key])
        foreign = "unrelated/@attributes.yson"
        storage.objects[foreign] = storage.objects[manifest.attributes.key]
        # Keep bytes valid JSON while simulating an untrusted object reference.
        storage.objects[key] = storage.objects[key].replace(
            manifest.attributes.key.encode(), foreign.encode()
        )
        storage.reads.clear()

        result = await restore(yt=yt, storage=storage)
        assert result.failed == 1 and result.completed == 0
        assert foreign not in storage.reads
        assert DESTINATION not in yt.restored

    asyncio.run(scenario())


def test_restore_reports_incomplete_descendants_but_restores_complete_siblings():
    async def scenario():
        nodes = [
            make_node(kind="map_node", size=0),
            make_node(path=SOURCE + "/complete"),
            make_node(path=SOURCE + "/incomplete"),
        ]
        yt, storage, _ = setup(nodes=nodes)
        await export(yt=yt, storage=storage)
        del storage.objects[manifest_key(path=SOURCE + "/incomplete")]
        result = await restore(yt=yt, storage=storage)
        assert result.failed >= 1
        assert DESTINATION + "/complete" in yt.restored
        assert DESTINATION + "/incomplete" not in yt.restored

    asyncio.run(scenario())


def test_restore_virtual_parent_and_listing_failure_are_not_false_success():
    async def scenario():
        yt, storage, _ = setup(nodes=[make_node(path=SOURCE + "/child")])
        await export(yt=yt, storage=storage, source=SOURCE + "/child")
        existing = (make_node(path=DESTINATION, kind="map_node", size=0), [])
        yt.restored[DESTINATION] = existing
        result = await restore(yt=yt, storage=storage)
        assert result.failed == 0
        assert DESTINATION + "/child" in yt.restored
        assert yt.restored[DESTINATION] is existing

        storage.list_failure_after = 1
        result = await restore(yt=yt, storage=storage)
        assert result.failed >= 1

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "prefix", ["archive/@data/custom", "archive/@manifest.json/custom"]
)
def test_user_s3_prefix_may_contain_reserved_service_names(prefix):
    async def scenario():
        nodes = [
            make_node(kind="map_node", size=0),
            make_node(path=SOURCE + "/child", size=3),
        ]
        yt, storage, _ = setup(nodes=nodes)
        exported = await export(yt=yt, storage=storage, prefix=prefix)
        assert exported.completed == 2 and exported.failed == 0

        restored = await restore(yt=yt, storage=storage, prefix=prefix)
        assert restored.completed == 2 and restored.failed == 0
        assert yt.restored[DESTINATION + "/child"][1] == [b"xxx"]

    asyncio.run(scenario())


@pytest.mark.parametrize("child_name", ["old", "001"])
def test_restore_ignores_stale_children_of_directory_replaced_by_file(child_name):
    async def scenario():
        replaced = SOURCE + "/changed"
        stale = replaced + "/" + child_name
        sibling = SOURCE + "/changed-other"
        nodes = [
            make_node(kind="map_node", size=0),
            make_node(path=replaced, kind="map_node", size=0),
            make_node(path=stale, size=3),
            make_node(path=sibling, size=2),
        ]
        yt, storage, _ = setup(nodes=nodes)
        await export(yt=yt, storage=storage)
        yt.nodes[replaced] = make_node(path=replaced, kind="file", size=1, revision=2)
        del yt.nodes[stale]
        updated = await export(yt=yt, storage=storage)
        assert updated.completed == 1 and updated.failed == 0
        stale_manifest = manifest_key(path=stale)
        assert stale_manifest in storage.objects
        storage.reads.clear()

        result = await restore(yt=yt, storage=storage)
        assert result.failed == 0
        assert result.completed == 3
        assert yt.restored[DESTINATION + "/changed"][1] == [b"x"]
        assert DESTINATION + "/changed/" + child_name not in yt.restored
        assert DESTINATION + "/changed-other" in yt.restored
        assert stale_manifest not in storage.reads

    asyncio.run(scenario())


def test_many_nodes_reuse_bounded_workers_and_separate_traversal_backend():
    """Reuse a fixed number of backends across many independent nodes."""

    async def scenario():
        """Copy many nodes and inspect factory leases and operation ownership."""
        nodes = [make_node(path=SOURCE + f"/{index}", size=1) for index in range(40)]
        yt, storage, _ = setup(nodes=nodes)
        result = await export(
            yt=yt, storage=storage, copy_options=options(concurrency=3, queue_size=2)
        )
        assert (result.completed, result.failed) == (40, 0)
        assert 5 <= len(yt.workers) <= 7
        assert all(worker.closed for worker in yt.workers)
        traversal = [
            worker
            for worker in yt.workers
            if any(method == "walk" for method, _ in worker.calls)
        ]
        assert len(traversal) == 1
        assert traversal[0].calls == [("walk", SOURCE)]
        copying = [worker for worker in yt.workers if worker is not traversal[0]]
        snapshots = [
            path
            for worker in copying
            for method, path in worker.calls
            if method == "snapshot"
        ]
        assert sorted(snapshots) == sorted(node.path for node in nodes)
        assert any(len(worker.calls) > 1 for worker in copying)

    asyncio.run(scenario())


def test_restore_directory_preparation_has_its_own_backend():
    """Keep directory transactions separate from file-copy worker contexts."""

    async def scenario():
        """Restore directories and files and check backend separation."""
        nodes = [
            make_node(kind="map_node", size=0),
            make_node(path=SOURCE + "/directory", kind="map_node", size=0),
            make_node(path=SOURCE + "/directory/file", size=1),
            make_node(path=SOURCE + "/second", size=1),
        ]
        yt, storage, _ = setup(nodes=nodes)
        await export(yt=yt, storage=storage)
        first_restore_worker = len(yt.workers)
        result = await restore(yt=yt, storage=storage)
        assert (result.completed, result.failed) == (4, 0)
        workers = yt.workers[first_restore_worker:]
        assert len(workers) == 2 and all(worker.closed for worker in workers)
        directories = [
            worker for worker in workers if "map_node" in worker.restore_kinds.values()
        ]
        files = [
            worker for worker in workers if "file" in worker.restore_kinds.values()
        ]
        assert len(directories) == len(files) == 1
        assert directories[0] is not files[0]
        assert len(directories[0].restore_kinds) == len(files[0].restore_kinds) == 2
        for worker in workers:
            for path in worker.restore_kinds:
                assert ("matches", path) in worker.calls

    asyncio.run(scenario())


def test_worker_acquisition_failure_cancels_blocked_discovery():
    """Do not hang on a full queue when its consumer cannot start."""

    async def scenario():
        """Fail worker acquisition only after the producer fills its queue."""
        nodes = [make_node(path=SOURCE + f"/{index}") for index in range(10)]
        yt, storage, _ = setup(nodes=nodes)
        yt.fail_acquire_index = 0
        yt.acquire_gate = yt.discovery_waiting
        with pytest.raises(OSError, match="worker acquisition failed"):
            await asyncio.wait_for(export(yt=yt, storage=storage), timeout=2)
        assert len(yt.discovered) == 2
        assert not yt.discovery_done
        assert len(yt.workers) == 2
        assert all(worker.closed for worker in yt.workers)
        assert yt.active == 0

    asyncio.run(scenario())


def test_producer_cleanup_failure_cancels_blocked_copy_worker():
    """Propagate lifecycle failure and close a worker blocked in data transfer."""

    async def scenario():
        """Fail producer cleanup while the copy worker awaits more data."""
        yt, storage, _ = setup()
        yt.release_read = asyncio.Event()
        yt.fail_producer_cleanup = True
        yt.cleanup_gate = yt.read_started
        with pytest.raises(OSError, match="producer cleanup failed"):
            await asyncio.wait_for(export(yt=yt, storage=storage), timeout=2)
        assert yt.read_started.is_set()
        assert all(worker.closed for worker in yt.workers)
        assert yt.active == 0
        assert manifest_key() not in storage.objects

    asyncio.run(scenario())


@pytest.mark.parametrize("repeated", [False, True])
def test_single_cancellation_waits_for_slow_worker_cleanup(repeated):
    """Do not cancel a worker's asynchronous cleanup a second time."""

    async def scenario():
        """Cancel one pipeline while fast and gated worker exits overlap."""
        nodes = [make_node(path=SOURCE + f"/{index}") for index in range(30)]
        yt, storage, _ = setup(nodes=nodes)
        yt.release_read = asyncio.Event()
        cleanup_started = asyncio.Event()
        release_cleanup = asyncio.Event()
        cleanup_finished = asyncio.Event()
        cleanup_interrupted = asyncio.Event()
        fast_closed = asyncio.Event()
        original_worker = yt.worker

        @asynccontextmanager
        async def leased_worker():
            """Make one backend's cleanup slower than the other backends."""
            async with original_worker() as backend:
                index = yt.workers.index(backend)
                try:
                    yield backend
                finally:
                    if index == 0:
                        cleanup_started.set()
                        try:
                            await release_cleanup.wait()
                        except asyncio.CancelledError:
                            cleanup_interrupted.set()
                            raise
                        cleanup_finished.set()
                    else:
                        fast_closed.set()

        yt.worker = leased_worker
        task = asyncio.create_task(
            export(
                yt=yt,
                storage=storage,
                copy_options=options(concurrency=2, queue_size=1),
            )
        )
        await asyncio.wait_for(yt.read_started.wait(), timeout=2)
        task.cancel()
        try:
            await asyncio.wait_for(cleanup_started.wait(), timeout=2)
            await asyncio.wait_for(fast_closed.wait(), timeout=2)
            for _ in range(10):
                if repeated:
                    task.cancel()
                await asyncio.sleep(0)

            assert not cleanup_interrupted.is_set()
            assert not cleanup_finished.is_set()
            assert not task.done()
        finally:
            release_cleanup.set()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, timeout=2)

        assert cleanup_finished.is_set()
        assert all(worker.closed for worker in yt.workers)
        assert yt.active == 0

    asyncio.run(scenario())


def test_multiple_tables_finish_with_one_shared_data_worker():
    """Snapshot owners must not occupy the only slot their ranges require."""

    async def scenario():
        nodes = [
            make_node(path=f"{SOURCE}/{index}", kind="table") for index in range(5)
        ]
        yt, storage, _ = setup(nodes=nodes)
        result = await asyncio.wait_for(
            export(
                yt=yt,
                storage=storage,
                copy_options=CopyOptions(concurrency=3, max_streams=1, part_size=4),
            ),
            timeout=2,
        )
        assert result.completed == 5 and result.failed == 0
        data_workers = [
            worker
            for worker in yt.workers
            if any(method == "read_part" for method, _ in worker.calls)
        ]
        assert len(data_workers) == 1
        assert len(data_workers[0].calls) == 15
        assert yt.max_active > 1 and yt.active == 0
        assert all(manifest_key(path=node.path) in storage.objects for node in nodes)

    asyncio.run(scenario())


def test_parallel_table_orders_manifest_and_credits_completed_range_sizes():
    """Finishing the final row range first must not report the entire table done."""

    async def scenario():
        node = make_node(kind="table", disk_size=240)
        yt, storage, events = setup(nodes=[node])
        progress = Progress()
        started = asyncio.Queue()
        releases = [asyncio.Event() for _ in range(3)]
        uploaded = [asyncio.Event() for _ in range(3)]
        original_upload = storage.upload

        async def upload(*, key, chunks):
            index = int(key.split("part-")[1].split(".")[0])
            started.put_nowait(index)
            await releases[index].wait()
            ref = await original_upload(key=key, chunks=chunks)
            uploaded[index].set()
            return ref

        storage.upload = upload
        task = asyncio.create_task(
            export(
                yt=yt,
                storage=storage,
                progress=progress,
                copy_options=CopyOptions(concurrency=1, max_streams=3, part_size=4),
            )
        )
        try:
            for index in range(3):
                assert await asyncio.wait_for(started.get(), timeout=2) == index
            for completed, index in enumerate((2, 0, 1), start=1):
                releases[index].set()
                await asyncio.wait_for(uploaded[index].wait(), timeout=2)
                assert progress.source_copied_bytes == 80 * completed
                if completed < 3:
                    assert progress.source_partial_nodes == pytest.approx(completed / 3)
                    assert manifest_key() not in storage.objects
        finally:
            for release in releases:
                release.set()
            outcome = await asyncio.wait_for(task, timeout=2)
        assert outcome.completed == 1 and outcome.failed == 0
        manifest = Manifest.from_bytes(storage.objects[manifest_key()])
        assert [(part.start, part.stop) for part in manifest.parts] == [
            (0, 4),
            (4, 8),
            (8, 12),
        ]
        assert events.index(("snapshot_exit", SOURCE)) < events.index(
            ("put", manifest_key())
        )
        assert progress.source_partial_nodes == 0

    asyncio.run(scenario())


def test_data_worker_failure_stops_before_replacing_other_node_archives():
    """A broken reader pool is fatal rather than a failure of every queued node."""

    async def scenario():
        nodes = [
            make_node(path=f"{SOURCE}/{index}", kind="table") for index in range(4)
        ]
        yt, storage, events = setup(nodes=nodes)
        assert (await export(yt=yt, storage=storage)).completed == 4
        originals = dict(storage.objects)
        yt.nodes = {node.path: replace(node, content_revision=2) for node in nodes}
        # One owner and one traversal client are acquired before the lazy reader.
        yt.fail_acquire_index = len(yt.workers) + 2
        events.clear()
        with pytest.raises(OSError, match="worker acquisition failed"):
            await asyncio.wait_for(export(yt=yt, storage=storage), timeout=2)
        assert [(kind, path) for kind, path in events if kind == "snapshot_enter"] == [
            ("snapshot_enter", nodes[0].path)
        ]
        for node in nodes[1:]:
            key = manifest_key(path=node.path)
            assert storage.objects[key] == originals[key]
        assert all(worker.closed for worker in yt.workers)
        assert yt.active == 0

    asyncio.run(scenario())


def test_progress_distinguishes_copied_unchanged_dynamic_and_failed_nodes():
    """Separate unchanged copies from policy skips without counting metadata bytes."""

    async def scenario():
        """Export mixed nodes twice and compare progress with final results."""
        nodes = [
            make_node(kind="map_node", size=0),
            make_node(path=SOURCE + "/file", size=6),
            make_node(path=SOURCE + "/empty", size=0),
            make_node(path=SOURCE + "/dynamic", kind="table", dynamic=True),
            make_node(path=SOURCE + "/link", kind="link"),
        ]
        yt, storage, _ = setup(nodes=nodes)
        first = Progress(estimated_total=100)
        result = await export(yt=yt, storage=storage, progress=first)
        assert first.discovered == 5 and first.estimated_total == 100
        assert first.completed == result.completed == 3
        assert first.skipped == result.skipped == 1
        assert first.unchanged == 0 and first.failed == result.failed == 1
        assert first.completed_bytes == 6
        assert first.active == first.queued == 0

        repeated = Progress()
        result = await export(yt=yt, storage=storage, progress=repeated)
        assert repeated.discovered == 5
        assert repeated.completed == result.completed == 0
        assert repeated.unchanged == 3 and repeated.skipped == 1
        assert result.skipped == repeated.unchanged + repeated.skipped
        assert repeated.failed == result.failed == 1
        assert repeated.completed_bytes == 0
        assert repeated.active == repeated.queued == 0

    asyncio.run(scenario())


def test_progress_bytes_are_completed_only_after_manifest_publication():
    """Exclude uploaded data when publication fails, then count a successful retry."""

    async def scenario():
        """Fail the final manifest write after all data parts were uploaded."""
        yt, storage, _ = setup()
        original_put = storage.put_bytes
        failed = Progress()

        async def fail_manifest(*, key, data):
            """Reject publication after asserting no bytes are completed yet."""
            if key == manifest_key():
                assert failed.completed_bytes == 0
                raise OSError("manifest publication failed")
            return await original_put(key=key, data=data)

        storage.put_bytes = fail_manifest
        result = await export(yt=yt, storage=storage, progress=failed)
        assert failed.discovered == 1
        assert failed.failed == result.failed == 1
        assert failed.completed == failed.completed_bytes == 0
        assert len([key for key in storage.objects if "/@data/" in key]) == 3

        storage.put_bytes = original_put
        retried = Progress()
        result = await export(yt=yt, storage=storage, progress=retried)
        assert retried.completed == result.completed == 1
        assert retried.failed == 0 and retried.completed_bytes == 12

    asyncio.run(scenario())


def test_restore_progress_counts_directories_once_and_ignores_virtual_parents():
    """Count source directories even when descendants precede their S3 keys."""

    async def scenario():
        """Restore a real tree twice and a separate virtual-root archive."""
        nodes = [
            make_node(kind="map_node", size=0),
            make_node(path=SOURCE + "/001", kind="map_node", size=0),
            make_node(path=SOURCE + "/001/file", size=6),
            make_node(path=SOURCE + "/empty", kind="map_node", size=0),
        ]
        yt, storage, _ = setup(nodes=nodes)
        await export(yt=yt, storage=storage)
        progress = Progress()
        result = await restore(yt=yt, storage=storage, progress=progress)
        assert progress.discovered == 4
        assert progress.completed == result.completed == 4
        assert progress.failed == progress.skipped == progress.unchanged == 0
        assert progress.completed_bytes == 6
        assert progress.active == progress.queued == 0

        repeated = Progress()
        result = await restore(yt=yt, storage=storage, progress=repeated)
        assert repeated.discovered == repeated.unchanged == result.skipped == 4
        assert repeated.completed == repeated.completed_bytes == repeated.skipped == 0

        yt, storage, _ = setup(nodes=[make_node(path=SOURCE + "/child", size=2)])
        await export(yt=yt, storage=storage)
        virtual = Progress()
        result = await restore(yt=yt, storage=storage, progress=virtual)
        assert virtual.discovered == virtual.completed == result.completed == 1
        assert virtual.failed == 0 and virtual.completed_bytes == 2
        assert DESTINATION in yt.restored

    asyncio.run(scenario())


def test_restore_progress_does_not_complete_bytes_of_a_corrupt_node():
    """Keep completed bytes at zero when a later part aborts the whole node."""

    async def scenario():
        """Corrupt the second part so some data transfers before restore fails."""
        yt, storage, _ = setup()
        await export(yt=yt, storage=storage)
        manifest = Manifest.from_bytes(storage.objects[manifest_key()])
        second = manifest.parts[1].object
        storage.objects[second.key] = b"y" * second.size
        progress = Progress()

        result = await restore(yt=yt, storage=storage, progress=progress)
        assert progress.discovered == progress.failed == result.failed == 1
        assert progress.completed == progress.completed_bytes == 0
        assert storage.downloaded == [part.object.key for part in manifest.parts[:2]]
        assert progress.active == progress.queued == 0

    asyncio.run(scenario())


def test_restore_progress_reports_incomplete_nodes_without_duplicate_directories():
    """Include unfinished source nodes while preserving successful sibling counts."""

    async def scenario():
        """Restore one valid file and one node missing its completion manifest."""
        nodes = [
            make_node(kind="map_node", size=0),
            make_node(path=SOURCE + "/001", size=3),
            make_node(path=SOURCE + "/002", size=4),
        ]
        yt, storage, _ = setup(nodes=nodes)
        await export(yt=yt, storage=storage)
        del storage.objects[manifest_key(path=SOURCE + "/002")]
        progress = Progress()

        result = await restore(yt=yt, storage=storage, progress=progress)
        assert progress.discovered == 3
        assert progress.completed == result.completed == 2
        assert progress.failed == result.failed == 1
        assert progress.completed_bytes == 3
        assert progress.active == progress.queued == 0

    asyncio.run(scenario())


@pytest.mark.parametrize("kind", ["table", "file"])
def test_source_progress_tracks_uploaded_ranges_before_node_publication(kind):
    """Report table rows or file byte ranges while a node is still copying."""

    async def scenario():
        """Observe partial source weights between uploads and at publication."""
        yt, storage, _ = setup(nodes=[make_node(kind=kind, size=12, disk_size=240)])
        progress = Progress()
        original_upload = storage.upload
        original_put = storage.put_bytes
        observations = []

        async def check_upload(*, key, chunks):
            """Capture only already completed parts before sending the next."""
            observations.append(
                (progress.source_copied_bytes, progress.source_partial_nodes)
            )
            assert progress.source_observed_bytes == 240
            assert progress.completed == progress.completed_bytes == 0
            return await original_upload(key=key, chunks=chunks)

        async def check_publication(*, key, data):
            """Keep the full partial credit until the manifest is accepted."""
            if key == manifest_key():
                assert progress.source_copied_bytes == 240
                assert progress.source_partial_nodes == 1
                assert progress.completed == progress.completed_bytes == 0
            return await original_put(key=key, data=data)

        storage.upload = check_upload
        storage.put_bytes = check_publication
        result = await export(yt=yt, storage=storage, progress=progress)
        assert observations == pytest.approx([(0, 0), (80, 1 / 3), (160, 2 / 3)])
        assert result.completed == progress.completed == 1
        assert progress.source_copied_bytes == 240
        assert progress.source_partial_nodes == progress.source_skipped_bytes == 0
        assert progress.source_observed_bytes == 240
        assert progress.discovery_complete
        assert not progress.source_size_missing

    asyncio.run(scenario())


@pytest.mark.parametrize("failure", ["upload", "snapshot_exit", "manifest"])
def test_failed_export_rolls_back_source_progress(failure):
    """Source bytes from an incomplete node never count as completed work."""

    async def scenario():
        """Fail after at least one archive part has gained partial credit."""
        yt, storage, _ = setup(nodes=[make_node(disk_size=240)])
        progress = Progress()
        original_upload = storage.upload
        original_put = storage.put_bytes

        async def fail_upload(*, key, chunks):
            """Reject the second part after the first part earns source credit."""
            if "part-000001" in key:
                assert progress.source_copied_bytes == 80
                assert progress.source_partial_nodes == pytest.approx(1 / 3)
                raise OSError("second part failed")
            return await original_upload(key=key, chunks=chunks)

        async def fail_manifest(*, key, data):
            """Reject final publication after the entire source was uploaded."""
            if key == manifest_key():
                assert progress.source_copied_bytes == 240
                assert progress.source_partial_nodes == 1
                raise OSError("publication failed")
            return await original_put(key=key, data=data)

        if failure == "upload":
            storage.upload = fail_upload
        elif failure == "snapshot_exit":
            yt.fail_snapshot_exit.add(SOURCE)
        elif failure == "manifest":
            storage.put_bytes = fail_manifest

        result = await export(yt=yt, storage=storage, progress=progress)
        assert result.failed == progress.failed == 1
        assert progress.source_observed_bytes == 240
        assert progress.source_copied_bytes == progress.source_partial_nodes == 0
        assert progress.completed == progress.completed_bytes == 0
        assert progress.source_skipped_bytes == 0
        assert manifest_key() not in storage.objects

    asyncio.run(scenario())


def test_cancelled_export_rolls_back_uploaded_source_progress():
    """Discard partial source weight when cancellation interrupts the next part."""

    async def scenario():
        """Cancel a real worker blocked after successfully uploading one part."""
        yt, storage, _ = setup(nodes=[make_node(disk_size=240)])
        progress = Progress()
        original_upload = storage.upload
        second_started = asyncio.Event()
        release_second = asyncio.Event()

        async def block_second(*, key, chunks):
            """Leave one credited part before blocking the next transfer."""
            if "part-000001" in key:
                second_started.set()
                await release_second.wait()
            return await original_upload(key=key, chunks=chunks)

        storage.upload = block_second
        task = asyncio.create_task(export(yt=yt, storage=storage, progress=progress))
        await asyncio.wait_for(second_started.wait(), timeout=2)
        assert progress.source_copied_bytes == 80
        assert progress.source_partial_nodes == pytest.approx(1 / 3)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=2)

        assert progress.source_copied_bytes == progress.source_partial_nodes == 0
        assert progress.source_observed_bytes == 240
        assert progress.source_skipped_bytes == progress.completed_bytes == 0
        assert progress.active == 0
        assert manifest_key() not in storage.objects

    asyncio.run(scenario())


def test_source_progress_excludes_skips_from_copy_speed_and_counts_directories_once():
    """Deduct unchanged and dynamic data without inflating measured copy speed."""

    async def scenario():
        """Copy a weighted tree twice, including directories and an empty file."""
        nodes = [
            make_node(kind="map_node", size=0, disk_size=0),
            make_node(path=SOURCE + "/nested", kind="map_node", size=0, disk_size=0),
            make_node(path=SOURCE + "/nested/file", disk_size=240),
            make_node(path=SOURCE + "/table", kind="table", size=6, disk_size=60),
            make_node(path=SOURCE + "/empty", size=0, disk_size=5),
            make_node(
                path=SOURCE + "/dynamic", kind="table", dynamic=True, disk_size=90
            ),
        ]
        yt, storage, _ = setup(nodes=nodes)
        first = Progress()
        result = await export(yt=yt, storage=storage, progress=first)
        assert result.completed == first.completed == 5
        assert first.discovered == 6
        assert first.source_observed_bytes == 395
        assert first.source_copied_bytes == 305
        assert first.source_skipped_bytes == 90
        assert first.source_partial_nodes == 0
        assert first.discovery_complete and not first.source_size_missing

        repeated = Progress()
        result = await export(yt=yt, storage=storage, progress=repeated)
        assert result.skipped == 6
        assert repeated.unchanged == 5 and repeated.skipped == 1
        assert repeated.source_observed_bytes == 395
        assert repeated.source_skipped_bytes == 395
        assert repeated.source_copied_bytes == repeated.source_partial_nodes == 0
        assert repeated.discovery_complete and not repeated.source_size_missing

    asyncio.run(scenario())


def test_source_progress_marks_unavailable_node_size():
    """Allow copying without size metadata while disabling the byte estimate."""

    async def scenario():
        """Complete an unknown-size file without inventing its storage weight."""
        yt, storage, _ = setup(nodes=[make_node(disk_size=None)])
        progress = Progress()
        result = await export(yt=yt, storage=storage, progress=progress)
        assert result.completed == 1
        assert progress.source_size_missing
        assert progress.source_observed_bytes == progress.source_copied_bytes == 0
        assert progress.source_partial_nodes == 0
        assert progress.discovery_complete

    asyncio.run(scenario())


def test_failed_source_progress_preserves_other_workers_credit():
    """Reverting one node leaves a concurrent successful node's progress intact."""

    async def scenario():
        """Overlap a failed second part with another node's publication."""
        nodes = [
            make_node(path=SOURCE + "/success", disk_size=120),
            make_node(path=SOURCE + "/failure", disk_size=240),
        ]
        yt, storage, _ = setup(nodes=nodes)
        storage.upload_failures.add("archive/source/failure/@data/part-000001.bin.zst")
        progress = Progress()
        result = await export(
            yt=yt,
            storage=storage,
            copy_options=options(concurrency=2),
            progress=progress,
        )
        assert result.completed == result.failed == 1
        assert progress.source_observed_bytes == 360
        assert progress.source_copied_bytes == 120
        assert progress.source_partial_nodes == progress.source_skipped_bytes == 0
        assert progress.completed_bytes == 12

    asyncio.run(scenario())


@pytest.mark.parametrize("dynamic", [False, True])
def test_failed_skip_snapshot_does_not_deduct_source_bytes(dynamic):
    """Only successful unchanged or dynamic skips reduce remaining source work."""

    async def scenario():
        """Fail snapshot exit after selecting an unchanged or dynamic skip."""
        yt, storage, _ = setup(
            nodes=[make_node(kind="table", disk_size=240, dynamic=dynamic)]
        )
        await export(yt=yt, storage=storage)
        yt.fail_snapshot_exit.add(SOURCE)
        yt.discovery_overrides[SOURCE] = replace(
            discovered_node(yt.nodes[SOURCE]), attribute_revision=None, dynamic=None
        )
        progress = Progress()
        result = await export(yt=yt, storage=storage, progress=progress)
        assert result.failed == 1
        assert result.skipped == 0
        assert progress.source_observed_bytes == 240
        assert progress.source_copied_bytes == progress.source_skipped_bytes == 0
        assert progress.source_partial_nodes == 0

    asyncio.run(scenario())


@pytest.mark.parametrize("kind", ["table", "file", "map_node"])
def test_complete_discovery_match_skips_without_any_snapshot(kind):
    """A completed matching node costs one manifest read and existence checks."""

    async def scenario():
        """Repeat an export and count the avoided YT operations and S3 writes."""
        node = make_node(kind=kind, size=0 if kind == "map_node" else 12, disk_size=240)
        yt, storage, events = setup(nodes=[node])
        await export(yt=yt, storage=storage)
        manifest = Manifest.from_bytes(storage.objects[manifest_key()])
        events.clear()
        yt.reads.clear()
        storage.reads.clear()
        storage.checked.clear()
        progress = Progress()

        result = await export(yt=yt, storage=storage, progress=progress)
        assert (result.completed, result.skipped, result.failed) == (0, 1, 0)
        assert events == [] and yt.reads == []
        assert storage.reads == [manifest_key()]
        assert storage.checked == [
            manifest.attributes.key,
            *(part.object.key for part in manifest.parts),
        ]
        assert progress.unchanged == 1
        assert progress.source_observed_bytes == progress.source_skipped_bytes == 240
        assert progress.source_copied_bytes == progress.completed_bytes == 0

    asyncio.run(scenario())


def test_dynamic_discovery_skips_without_snapshot_or_s3_requests():
    """A known dynamic table needs no snapshot or destination inspection."""

    async def scenario():
        """Skip directly from discovery and retain its source weight in progress."""
        yt, storage, events = setup(
            nodes=[make_node(kind="table", dynamic=True, disk_size=240)]
        )
        progress = Progress()
        result = await export(yt=yt, storage=storage, progress=progress)
        assert (result.completed, result.skipped, result.failed) == (0, 1, 0)
        assert events == storage.reads == storage.checked == yt.reads == []
        assert progress.skipped == 1 and progress.unchanged == 0
        assert progress.source_observed_bytes == progress.source_skipped_bytes == 240
        assert progress.source_copied_bytes == 0

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "missing", ["content_revision", "attribute_revision", "dynamic"]
)
def test_incomplete_discovery_metadata_falls_back_to_snapshot(missing):
    """Unknown discovery fields cannot establish that a static table is unchanged."""

    async def scenario():
        """Remove one list field while leaving the persisted copy fully valid."""
        yt, storage, events = setup(nodes=[make_node(kind="table", disk_size=240)])
        await export(yt=yt, storage=storage)
        yt.discovery_overrides[SOURCE] = replace(
            discovered_node(yt.nodes[SOURCE]), **{missing: None}
        )
        events.clear()
        yt.reads.clear()
        storage.reads.clear()
        progress = Progress()

        result = await export(yt=yt, storage=storage, progress=progress)
        assert (result.completed, result.skipped, result.failed) == (0, 1, 0)
        assert ("snapshot_enter", SOURCE) in events
        assert yt.reads == []
        assert storage.reads == [manifest_key()]
        assert progress.source_observed_bytes == progress.source_skipped_bytes == 240

    asyncio.run(scenario())


@pytest.mark.parametrize("field", ["compression_level", "part_size"])
def test_changed_copy_settings_require_snapshot_and_new_data(field):
    """Matching source revisions do not hide changes to the archive encoding."""

    async def scenario():
        """Change only one copy option between otherwise identical exports."""
        yt, storage, events = setup()
        await export(yt=yt, storage=storage)
        previous = storage.objects[manifest_key()]
        events.clear()
        yt.reads.clear()
        storage.reads.clear()
        changed_options = replace(options(), **{field: 2})

        result = await export(yt=yt, storage=storage, copy_options=changed_options)
        assert (result.completed, result.skipped, result.failed) == (1, 0, 0)
        assert ("snapshot_enter", SOURCE) in events
        assert yt.reads
        assert storage.reads == [manifest_key()]
        assert storage.objects[manifest_key()] != previous

    asyncio.run(scenario())


@pytest.mark.parametrize("change", ["identity", "content", "attributes", "kind"])
def test_changed_discovery_state_uses_current_snapshot(change):
    """ID, content, attributes and type changes are never treated as unchanged."""

    async def scenario():
        """Change one source property and require a newly published copy."""
        yt, storage, events = setup()
        await export(yt=yt, storage=storage)
        node = yt.nodes[SOURCE]
        if change == "identity":
            changed = replace(node, node_id="replacement-id")
        elif change == "content":
            changed = replace(node, content_revision=2)
        elif change == "attributes":
            changed = replace(
                node,
                attribute_revision=2,
                attributes=canonical_attributes({b"description": b"changed"}),
            )
        else:
            changed = make_node(kind="table")
        yt.nodes[SOURCE] = changed
        events.clear()
        yt.reads.clear()
        storage.reads.clear()

        result = await export(yt=yt, storage=storage)
        assert (result.completed, result.skipped, result.failed) == (1, 0, 0)
        assert ("snapshot_enter", SOURCE) in events
        assert yt.reads
        assert storage.reads == [manifest_key()]
        manifest = Manifest.from_bytes(storage.objects[manifest_key()])
        assert manifest.kind == changed.kind
        assert manifest.source["node_id"] == changed.node_id
        assert manifest.source["content_revision"] == changed.content_revision
        assert manifest.source["attribute_revision"] == changed.attribute_revision

    asyncio.run(scenario())


@pytest.mark.parametrize("missing", ["attributes", "data"])
def test_incomplete_fast_match_reuses_manifest_and_existence_results(missing):
    """Missing objects trigger repair without repeating already completed checks."""

    async def scenario():
        """Delete a referenced object and ensure the fallback costs no extra reads."""
        yt, storage, events = setup(nodes=[make_node(disk_size=240)])
        await export(yt=yt, storage=storage)
        manifest = Manifest.from_bytes(storage.objects[manifest_key()])
        lost = (
            manifest.attributes if missing == "attributes" else manifest.parts[0].object
        )
        del storage.objects[lost.key]
        events.clear()
        storage.reads.clear()
        storage.checked.clear()
        progress = Progress()

        result = await export(yt=yt, storage=storage, progress=progress)
        assert (result.completed, result.skipped, result.failed) == (1, 0, 0)
        assert ("snapshot_enter", SOURCE) in events
        assert storage.reads == [manifest_key()]
        assert storage.checked.count(lost.key) == 1
        assert len(storage.checked) == len(set(storage.checked))
        repaired = Manifest.from_bytes(storage.objects[manifest_key()])
        assert repaired.generation != manifest.generation
        assert lost.key not in storage.objects
        assert all(
            ref.key in storage.objects
            for ref in (repaired.attributes, *(part.object for part in repaired.parts))
        )
        assert progress.source_observed_bytes == progress.source_copied_bytes == 240
        assert progress.source_skipped_bytes == 0

    asyncio.run(scenario())


@pytest.mark.parametrize("legacy", [True, False])
def test_full_match_refreshes_revision_only_after_snapshot_exit(legacy):
    """Unchanged data gains a fresh revision guard with one manifest-only write."""

    async def scenario():
        """Migrate an old manifest or refresh attributes outside the saved subset."""
        yt, storage, events = setup(nodes=[make_node(disk_size=240)])
        await export(yt=yt, storage=storage)
        if legacy:
            raw = json.loads(storage.objects[manifest_key()])
            del raw["source"]["attribute_revision"]
            storage.objects[manifest_key()] = json.dumps(raw).encode()
        else:
            yt.nodes[SOURCE] = replace(yt.nodes[SOURCE], attribute_revision=3)
            yt.discovery_overrides[SOURCE] = replace(
                discovered_node(yt.nodes[SOURCE]), attribute_revision=2
            )
        before = dict(storage.objects)
        events.clear()
        yt.reads.clear()
        storage.reads.clear()
        progress = Progress()

        result = await export(yt=yt, storage=storage, progress=progress)
        assert (result.completed, result.skipped, result.failed) == (0, 1, 0)
        assert yt.reads == []
        assert storage.reads == [manifest_key()]
        assert [
            event for event in events if event[0] in {"put", "delete", "upload"}
        ] == [("put", manifest_key())]
        assert events.index(("snapshot_exit", SOURCE)) < events.index(
            ("put", manifest_key())
        )
        assert {
            key: value
            for key, value in storage.objects.items()
            if key != manifest_key()
        } == {key: value for key, value in before.items() if key != manifest_key()}
        manifest = Manifest.from_bytes(storage.objects[manifest_key()])
        assert (
            manifest.source["attribute_revision"] == yt.nodes[SOURCE].attribute_revision
        )
        assert progress.source_observed_bytes == progress.source_skipped_bytes == 240
        assert progress.source_copied_bytes == 0

        yt.discovery_overrides.clear()
        events.clear()
        storage.reads.clear()
        result = await export(yt=yt, storage=storage)
        assert result.skipped == 1 and result.failed == 0
        assert events == []
        assert storage.reads == [manifest_key()]

    asyncio.run(scenario())


@pytest.mark.parametrize("failure", ["snapshot_exit", "cancel", "manifest"])
def test_failed_revision_refresh_preserves_previous_manifest(failure):
    """Migration cannot publish or report success before its snapshot succeeds."""

    async def scenario():
        """Interrupt each migration boundary while retaining a restorable archive."""
        yt, storage, events = setup(nodes=[make_node(disk_size=240)])
        await export(yt=yt, storage=storage)
        raw = json.loads(storage.objects[manifest_key()])
        del raw["source"]["attribute_revision"]
        storage.objects[manifest_key()] = json.dumps(raw).encode()
        before = dict(storage.objects)
        events.clear()
        yt.reads.clear()
        progress = Progress()
        exiting = asyncio.Event()
        release_exit = asyncio.Event()
        original_snapshot = yt.snapshot

        @asynccontextmanager
        async def blocked_snapshot(*, path):
            """Expose a cancellation point after selecting unchanged data."""
            async with original_snapshot(path=path) as snapshot:
                yield snapshot
                exiting.set()
                await release_exit.wait()

        async def fail_manifest(*, key, data):
            """Fail only the migration's final publication."""
            assert key == manifest_key()
            raise OSError("manifest update failed")

        if failure == "snapshot_exit":
            yt.fail_snapshot_exit.add(SOURCE)
        elif failure == "cancel":
            yt.snapshot = blocked_snapshot
        else:
            storage.put_bytes = fail_manifest

        if failure == "cancel":
            task = asyncio.create_task(
                export(yt=yt, storage=storage, progress=progress)
            )
            await asyncio.wait_for(exiting.wait(), timeout=2)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, timeout=2)
        else:
            result = await export(yt=yt, storage=storage, progress=progress)
            assert (result.completed, result.skipped, result.failed) == (0, 0, 1)

        assert storage.objects == before
        assert yt.reads == []
        assert all(action not in {"put", "delete", "upload"} for action, _ in events)
        assert progress.source_skipped_bytes == progress.source_copied_bytes == 0
        assert progress.unchanged == progress.completed == 0
        assert yt.active == 0

    asyncio.run(scenario())


def test_copy_publishes_snapshot_revisions_instead_of_discovery_revisions():
    """The copied manifest describes the actual snapshot, not its earlier list hint."""

    async def scenario():
        """Move the source forward between directory listing and snapshot opening."""
        node = make_node(revision=3, attribute_revision=4, disk_size=240)
        yt, storage, _ = setup(nodes=[node])
        yt.discovery_overrides[SOURCE] = replace(
            discovered_node(node),
            content_revision=1,
            attribute_revision=2,
            disk_size=100,
        )
        progress = Progress()
        result = await export(yt=yt, storage=storage, progress=progress)
        assert result.completed == 1 and result.failed == 0
        manifest = Manifest.from_bytes(storage.objects[manifest_key()])
        assert manifest.source["content_revision"] == 3
        assert manifest.source["attribute_revision"] == 4
        assert progress.source_observed_bytes == progress.source_copied_bytes == 240

    asyncio.run(scenario())


def test_unchanged_directory_still_discovers_changed_children():
    """Skipping directory metadata must not suppress traversal of its descendants."""

    async def scenario():
        """Keep the parent's revisions stable while replacing one child's content."""
        nodes = [
            make_node(kind="map_node", size=0),
            make_node(path=SOURCE + "/child"),
        ]
        yt, storage, events = setup(nodes=nodes)
        await export(yt=yt, storage=storage)
        child = SOURCE + "/child"
        yt.nodes[child] = replace(yt.nodes[child], content_revision=2)
        events.clear()
        result = await export(yt=yt, storage=storage)
        assert (result.completed, result.skipped, result.failed) == (1, 1, 0)
        assert ("snapshot_enter", SOURCE) not in events
        assert ("snapshot_enter", child) in events
        assert len(yt.discovered) == 4

    asyncio.run(scenario())


def test_fast_skip_with_unknown_size_disables_source_byte_estimate():
    """Missing discovery size affects ETA but does not require a metadata snapshot."""

    async def scenario():
        """Lose optional storage weight after producing a complete archive."""
        yt, storage, events = setup(nodes=[make_node(disk_size=240)])
        await export(yt=yt, storage=storage)
        yt.discovery_overrides[SOURCE] = replace(
            discovered_node(yt.nodes[SOURCE]), disk_size=None
        )
        events.clear()
        progress = Progress()
        result = await export(yt=yt, storage=storage, progress=progress)
        assert result.skipped == 1 and result.failed == 0
        assert events == []
        assert progress.source_size_missing
        assert progress.source_observed_bytes == progress.source_skipped_bytes == 0

    asyncio.run(scenario())


def test_direct_export_without_discovery_still_uses_snapshot():
    """Callers copying an individual path need no prebuilt discovery descriptor."""

    async def scenario():
        """Repeat a direct copy and preserve its snapshot-based contract."""
        yt, storage, events = setup()
        await export(yt=yt, storage=storage)
        events.clear()
        async with (
            yt.worker() as backend,
            PartPool(yt=yt, concurrency=1) as pool,
        ):
            result = await export_node(
                pool=pool,
                yt=backend,
                storage=storage,
                path=SOURCE,
                prefix=PREFIX,
                cluster="local-test",
                options=options(),
            )
        assert result == ("skipped", "unchanged")
        assert ("snapshot_enter", SOURCE) in events

    asyncio.run(scenario())


def test_fast_skip_uses_discovery_state_and_next_run_catches_later_changes():
    """A change after listing is picked up by the next copy, not a forced snapshot."""

    async def scenario():
        """Retain the listed revision while advancing the live source in the fake."""
        yt, storage, events = setup()
        await export(yt=yt, storage=storage)
        listed = discovered_node(yt.nodes[SOURCE])
        yt.nodes[SOURCE] = replace(yt.nodes[SOURCE], content_revision=2)
        yt.discovery_overrides[SOURCE] = listed
        events.clear()
        result = await export(yt=yt, storage=storage)
        assert result.skipped == 1 and result.failed == 0
        assert events == []
        previous = Manifest.from_bytes(storage.objects[manifest_key()])
        assert previous.source["content_revision"] == 1

        yt.discovery_overrides.clear()
        result = await export(yt=yt, storage=storage)
        assert result.completed == 1 and result.failed == 0
        current = Manifest.from_bytes(storage.objects[manifest_key()])
        assert current.source["content_revision"] == 2

    asyncio.run(scenario())


def test_copy_rejects_link_replacing_source_after_discovery():
    """The snapshot still validates a source that has changed since listing."""

    async def scenario():
        """Queue a file descriptor but open a replacement link when copying."""
        original = make_node()
        yt, storage, events = setup(nodes=[make_node(kind="link")])
        yt.discovery_overrides[SOURCE] = discovered_node(original)
        result = await export(yt=yt, storage=storage)
        assert (result.completed, result.skipped, result.failed) == (0, 0, 1)
        assert ("snapshot_enter", SOURCE) in events
        assert yt.reads == [] and storage.objects == {}
        assert not any(action in {"put", "delete", "upload"} for action, _ in events)

    asyncio.run(scenario())
