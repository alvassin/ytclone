"""Offline checks for concurrent discovery, metadata sizing, and cleanup."""

import asyncio
import contextlib

import pytest
from yt import yson

from ytclone.format import DiscoveredNode
from ytclone.parallel.discovery import DiscoveryError, discover
from ytclone.yt import _discovered_node


def run(coro):
    """Bound async tests so a recursive-queue deadlock fails promptly."""
    return asyncio.run(asyncio.wait_for(fut=coro, timeout=5))


def node(*, path: str, kind: str = "map_node") -> DiscoveredNode:
    """Create a distinct discovered source node."""
    return DiscoveredNode(path=path, kind=kind, node_id=path)


class FakeWorker:
    def __init__(self, *, adapter):
        """Track concurrent client use and listing lifecycle."""
        self.adapter = adapter
        self.busy = False
        self.closed = False

    async def inspect_node(self, *, path):
        """Return root metadata without enumerating any descendants."""
        self.adapter.inspected.append(path)
        return self.adapter.root

    async def list_children(self, *, node):
        """Yield control during each request to exercise actual scanner overlap."""
        assert not self.busy and not self.closed
        self.busy = True
        self.adapter.active += 1
        self.adapter.peak = max(self.adapter.peak, self.adapter.active)
        self.adapter.listed.append(node.path)
        self.adapter.entered.put_nowait(node.path)

        try:
            await asyncio.sleep(0)
            if node.path in self.adapter.blocked:
                await self.adapter.release.wait()
            if node.path == self.adapter.fail_path:
                raise ValueError("incomplete listing")
            return self.adapter.tree[node.path]
        finally:
            self.busy = False
            self.adapter.active -= 1


class FakeAdapter:
    def __init__(self, *, tree, root=None, blocked=(), fail_path=None):
        """Configure directory responses and optional failures."""
        self.tree = tree
        self.root = root or node(path="//root")
        self.blocked = set(blocked)
        self.fail_path = fail_path
        self.release = asyncio.Event()
        self.entered = asyncio.Queue()
        self.inspected = []
        self.listed = []
        self.workers = []
        self.active = 0
        self.peak = 0

    @contextlib.asynccontextmanager
    async def worker(self):
        """Require every opened persistent client to be closed after use."""
        worker = FakeWorker(adapter=self)
        self.workers.append(worker)
        try:
            yield worker
        finally:
            assert not worker.busy
            worker.closed = True


def test_scanners_overlap_with_bounded_persistent_clients():
    """Wide trees use all scanners without duplicate emissions or client sharing."""

    async def main():
        """Block three child listings to measure their simultaneous activity."""
        folders = [node(path=f"//root/{index}") for index in range(40)]
        tree = {"//root": folders}
        tree.update(
            {
                folder.path: [node(path=folder.path + "/table", kind="table")]
                for folder in folders
            }
        )
        adapter = FakeAdapter(tree=tree, blocked=[folder.path for folder in folders])
        observed = []

        async def emit(entry):
            """Record discoveries while requests are still running."""
            observed.append(entry.path)

        task = asyncio.create_task(
            discover(yt=adapter, source="//root", concurrency=3, emit=emit)
        )
        for _ in range(4):
            await adapter.entered.get()

        assert adapter.active == adapter.peak == 3
        assert observed[0] == "//root"
        assert not task.done()
        adapter.release.set()
        await task

        assert len(observed) == len(set(observed)) == 81
        assert len(adapter.workers) == 3
        assert all(worker.closed for worker in adapter.workers)
        assert adapter.inspected == ["//root"]
        assert sorted(adapter.listed) == sorted(tree)

    run(main())


@pytest.mark.parametrize("kind", ["map_node", "table", "file", "link"])
def test_empty_roots_and_leaf_roots_are_emitted(kind):
    """Empty directories are preserved and links are never followed by discovery."""

    async def main():
        """Collect a root without children."""
        adapter = FakeAdapter(tree={"//root": []}, root=node(path="//root", kind=kind))
        observed = []

        async def emit(entry):
            """Record the root task."""
            observed.append(entry)

        await discover(yt=adapter, source="//root", concurrency=3, emit=emit)
        assert observed == [adapter.root]
        assert adapter.listed == (["//root"] if kind == "map_node" else [])
        assert all(worker.closed for worker in adapter.workers)
        if kind != "map_node":
            assert len(adapter.workers) == 1

    run(main())


def test_listing_failure_finishes_unrelated_subtrees_before_raising():
    """An unavailable directory must not prevent discovery of healthy siblings."""

    async def main():
        """Let a healthy sibling finish after another directory listing failed."""
        adapter = FakeAdapter(
            tree={
                "//root": [node(path="//root/a"), node(path="//root/b")],
                "//root/b": [node(path="//root/b/child")],
                "//root/b/child": [node(path="//root/b/child/table", kind="table")],
            },
            blocked=["//root/b"],
            fail_path="//root/a",
        )
        observed = []

        async def emit(entry):
            """Record healthy sibling discoveries despite an earlier error."""
            observed.append(entry.path)

        task = asyncio.create_task(
            discover(yt=adapter, source="//root", concurrency=2, emit=emit)
        )
        for _ in range(3):
            await adapter.entered.get()
        assert not task.done()

        adapter.release.set()
        with pytest.raises(DiscoveryError, match="1 source directories") as error:
            await task

        assert error.value.failed_directories == 1
        assert "//root/b/child/table" in observed
        assert adapter.listed == ["//root", "//root/a", "//root/b", "//root/b/child"]
        assert adapter.active == 0
        assert all(worker.closed for worker in adapter.workers)

    run(main())


def test_all_failed_listings_finish_without_queue_deadlock():
    """Failed directory tasks are acknowledged even when no descendants can be read."""

    async def main():
        """Missing fake directory responses make both child requests fail."""
        adapter = FakeAdapter(
            tree={"//root": [node(path="//root/a"), node(path="//root/b")]},
        )

        async def emit(entry):
            """Accept directory metadata without hiding missing descendants."""

        with pytest.raises(DiscoveryError) as error:
            await discover(yt=adapter, source="//root", concurrency=2, emit=emit)

        assert error.value.failed_directories == 2
        assert adapter.active == 0
        assert all(worker.closed for worker in adapter.workers)

    run(main())


def test_cancellation_closes_all_scanners_without_further_listings():
    """Cancellation reaches every active request and releases all clients."""

    async def main():
        """Cancel after both scanners have begun child requests."""
        adapter = FakeAdapter(
            tree={"//root": [node(path="//root/a"), node(path="//root/b")]},
            blocked=["//root/a", "//root/b"],
        )

        async def emit(entry):
            """Accept nodes without delaying discovery."""

        task = asyncio.create_task(
            discover(yt=adapter, source="//root", concurrency=2, emit=emit)
        )
        for _ in range(3):
            await adapter.entered.get()
        before = list(adapter.listed)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert adapter.listed == before
        assert adapter.active == 0
        assert all(worker.closed for worker in adapter.workers)

    run(main())


def test_emit_failure_closes_clients():
    """A coordinator refusing a task fails discovery and closes its clients."""

    async def main():
        """Reject the first leaf after accepting the source directory."""
        adapter = FakeAdapter(
            tree={"//root": [node(path="//root/table", kind="table")]}
        )

        async def emit(entry):
            """Simulate a scheduler failure."""
            if entry.path != "//root":
                raise RuntimeError("scheduler failed")

        with pytest.raises(RuntimeError, match="scheduler failed"):
            await discover(yt=adapter, source="//root", concurrency=2, emit=emit)

        assert all(worker.closed for worker in adapter.workers)

    run(main())


@pytest.mark.parametrize(
    ("children", "message"),
    [
        ([node(path="//root/a"), node(path="//root/a")], "duplicate path"),
        ([node(path="//outside/table", kind="table")], "outside its directory"),
        ([node(path="//root/a/table", kind="table")], "outside its directory"),
    ],
)
def test_invalid_listing_paths_fail(children, message):
    """Duplicate tasks and accidental subtree escapes must never be accepted."""

    async def main():
        """Return an invalid root listing."""
        adapter = FakeAdapter(tree={"//root": children})

        async def emit(entry):
            """Accept valid discoveries before encountering the invalid node."""

        with pytest.raises(ValueError, match=message):
            await discover(yt=adapter, source="//root", concurrency=1, emit=emit)

    run(main())


@pytest.mark.parametrize("concurrency", [0, -1, True, 1.5])
def test_invalid_concurrency_opens_no_clients(concurrency):
    """Reject invalid limits before contacting the source."""

    async def main():
        """Pass an invalid scanner count."""
        adapter = FakeAdapter(tree={})

        async def emit(entry):
            """Never called for invalid configuration."""
            pytest.fail("Invalid discovery emitted a node")

        with pytest.raises(ValueError, match="positive integer"):
            await discover(
                yt=adapter, source="//root", concurrency=concurrency, emit=emit
            )

        assert not adapter.workers

    run(main())


@pytest.mark.parametrize(
    ("kind", "expected"),
    [(b"table", 20), (b"file", 30), (b"map_node", 0), (b"link", None)],
)
def test_discovery_size_uses_logical_bytes(kind, expected):
    """Table and file scheduling ignores replicated disk-space estimates."""
    discovered = _discovered_node(
        path="//root/node",
        attrs={
            b"id": b"node-id",
            b"type": kind,
            b"data_weight": yson.YsonUint64(20),
            b"uncompressed_data_size": yson.YsonInt64(30),
            b"resource_usage": {b"disk_space": 999},
        },
    )
    assert discovered.estimated_size == expected


@pytest.mark.parametrize("kind", [b"table", b"file"])
@pytest.mark.parametrize(
    "value", [None, -1, True, "2", 1.5, yson.YsonEntity(), yson.YsonBoolean(True)]
)
def test_missing_or_malformed_size_remains_unknown(kind, value):
    """An unknown size cannot be accidentally treated as an empty table."""
    attrs = {b"id": b"node-id", b"type": kind}
    if value is not None:
        attrs[b"data_weight"] = value
        attrs[b"uncompressed_data_size"] = value

    assert _discovered_node(path="//root/node", attrs=attrs).estimated_size is None
