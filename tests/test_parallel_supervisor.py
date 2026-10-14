"""Coordinator integration checks with real local subprocesses and fake copies."""

import asyncio
import json
import sys
import textwrap
from contextlib import asynccontextmanager

import pytest

from ytclone.format import DiscoveredNode
from ytclone.parallel import supervisor
from ytclone.progress import Progress

FAKE_WORKER = r"""
import json
import os
import select
import sys
import time
from pathlib import Path

config = json.loads(sys.stdin.readline())["config"]
root = Path(config["fake_root"])
mode = config.get("fake_mode", "normal")
log = root / "events.jsonl"


def record(event, **values):
    with log.open("a") as stream:
        stream.write(json.dumps(dict(event=event, pid=os.getpid(), **values)) + "\n")


def send(message):
    print(json.dumps(message), flush=True)


record("launched")
send({"type": "ready"})
buffer = b""
pending = []
uploaded = published = 0
if config.get("announce_ready"):
    uploaded = 1
    send({"type": "progress", "uploaded": uploaded, "published": 0})
while True:
    readable, _, _ = select.select([sys.stdin.fileno()], [], [], 0.005)
    if readable:
        data = os.read(sys.stdin.fileno(), 65536)
        if not data:
            record("eof")
            break
        buffer += data
        while b"\n" in buffer:
            line, buffer = buffer.split(b"\n", 1)
            message = json.loads(line)
            if message["type"] == "stop":
                record("stopped")
                sys.exit(0)
            assert message["type"] == "job"
            job_id = message["id"]
            path = message["node"]["path"]
            record("started", job_id=job_id, path=path)
            if mode == "crash_always":
                record("crashed", job_id=job_id)
                os._exit(7)
            if mode == "crash_once":
                marker = root / "crashed.pid"
                try:
                    with marker.open("x") as stream:
                        stream.write(str(os.getpid()))
                except FileExistsError:
                    old_pid = int(marker.read_text())
                    try:
                        os.kill(old_pid, 0)
                        old_alive = True
                    except ProcessLookupError:
                        old_alive = False
                    record("retry_started", job_id=job_id, old_alive=old_alive)
                else:
                    send({"type": "progress", "uploaded": 5, "published": 0})
                    record("crashed", job_id=job_id)
                    os._exit(7)
            if mode == "spam":
                while True:
                    uploaded += 1
                    send({"type": "progress", "uploaded": uploaded,
                          "published": 0, "padding": "x" * 32000})
            pending.append((job_id, path))

    for job_id, path in list(pending):
        if mode == "hold" and "/large" in path and not (root / "release").exists():
            continue
        if mode == "busy":
            continue
        pending.remove((job_id, path))
        name = path.rsplit("/", 1)[-1]
        status, reason = {
            "bad": ("failed", "fake error"),
            "unchanged": ("skipped", "unchanged"),
            "dynamic": ("skipped", "dynamic"),
        }.get(name, ("completed", ""))
        if status == "completed":
            uploaded += 13
            published += 7
        send({"type": "progress", "uploaded": uploaded, "published": published})
        result = {"type": "result", "id": job_id, "status": status, "reason": reason}
        send(result)
        record("finished", job_id=job_id, path=path)
        if mode == "duplicate":
            send(result)
"""


def make_node(*, name: str, size: int = 1) -> DiscoveredNode:
    """Create source metadata for a fake transfer."""
    return DiscoveredNode(
        path=f"//source/{name}", kind="table", node_id=name, estimated_size=size
    )


def read_events(root) -> list[dict]:
    """Read complete records from the fake worker's append-only event file."""
    path = root / "events.jsonl"
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line]


async def until(predicate, *, timeout: float = 3) -> None:
    """Wait briefly for an observable effect rather than sleeping for a race."""

    async def poll():
        while not predicate():
            await asyncio.sleep(0.005)

    await asyncio.wait_for(poll(), timeout=timeout)


async def finish(task, *, timeout: float = 5):
    """Bound a test even if cancellation cannot finish subprocess cleanup."""
    done, _ = await asyncio.wait({task}, timeout=timeout)
    if task not in done:
        task.cancel()
        raise AssertionError("Supervisor did not finish within the test deadline")
    return task.result()


@asynccontextmanager
async def tracked_processes(monkeypatch):
    """Track real child handles and forcibly close them if a test fails."""
    original = asyncio.create_subprocess_exec
    processes = []

    async def spawn(*args, **kwargs):
        process = await original(*args, **kwargs)
        processes.append(process)
        return process

    with monkeypatch.context() as patch:
        patch.setattr(supervisor.asyncio, "create_subprocess_exec", spawn)
        try:
            yield processes
        finally:
            for process in processes:
                if process.returncode is None:
                    try:
                        process.kill()
                    except ProcessLookupError:
                        pass
                # Closing pipes unblocks Process.wait even if stdout was flooded.
                process._transport.close()
            if processes:
                await asyncio.wait_for(
                    asyncio.gather(*(process.wait() for process in processes)),
                    timeout=2,
                )


@pytest.fixture
def fake_worker(tmp_path):
    """Provide a standalone Python worker implementing only the wire protocol."""
    script = tmp_path / "worker.py"
    script.write_text(textwrap.dedent(FAKE_WORKER))
    return [sys.executable, "-u", str(script)]


def arguments(
    *, tmp_path, fake_worker, discover, progress, mode="normal", concurrency=1
):
    """Keep fake-process configuration isolated from real backup credentials."""
    return {
        "discover": discover,
        "config": {
            "concurrency": concurrency,
            "queue_size": 1,
            "fake_root": str(tmp_path),
            "fake_mode": mode,
        },
        "processes": 1,
        "small_size": 10,
        "progress": progress,
        "restart_delay": 0.001,
        "command": fake_worker,
    }


def test_copies_while_discovery_is_still_running(monkeypatch, tmp_path, fake_worker):
    """The first small node completes before the source scan has finished."""

    async def check():
        release = asyncio.Event()
        progress = Progress()

        async def discover(*, emit):
            await emit(make_node(name="first"))
            await release.wait()
            await emit(make_node(name="second"))

        async with tracked_processes(monkeypatch) as children:
            task = asyncio.create_task(
                supervisor.supervise(
                    **arguments(
                        tmp_path=tmp_path,
                        fake_worker=fake_worker,
                        discover=discover,
                        progress=progress,
                    )
                )
            )
            try:
                await until(lambda: progress.completed == 1)
                assert not progress.discovery_complete
                release.set()
                await finish(task)
            finally:
                release.set()
                if not task.done():
                    task.cancel()

            assert progress.completed == progress.discovered == 2
            assert progress.failed == progress.active == progress.queued == 0
            assert progress.transferred_bytes == 26
            assert progress.completed_bytes == 14
            assert all(child.returncode == 0 for child in children)

    asyncio.run(check())


def test_reserved_capacity_accepts_later_small_node(monkeypatch, tmp_path, fake_worker):
    """A newly discovered small table starts while a large transfer is active."""

    async def check():
        progress = Progress()

        def started(name):
            return any(
                e.get("path") == f"//source/{name}" and e["event"] == "started"
                for e in read_events(tmp_path)
            )

        async def discover(*, emit):
            await emit(make_node(name="large-first", size=100))
            await until(lambda: started("large-first"))
            await emit(make_node(name="large-second", size=200))
            await emit(make_node(name="small", size=1))
            await until(lambda: progress.completed == 1)
            assert started("small")
            assert not started("large-second")
            (tmp_path / "release").touch()

        async with tracked_processes(monkeypatch):
            task = asyncio.create_task(
                supervisor.supervise(
                    **arguments(
                        tmp_path=tmp_path,
                        fake_worker=fake_worker,
                        discover=discover,
                        progress=progress,
                        mode="hold",
                        concurrency=2,
                    )
                )
            )
            await finish(task)

        starts = [e["path"] for e in read_events(tmp_path) if e["event"] == "started"]
        assert starts == [
            "//source/large-first",
            "//source/small",
            "//source/large-second",
        ]
        assert progress.completed == 3
        assert progress.failed == 0
        assert progress.completed_bytes == 21

    asyncio.run(check())


def test_node_failure_does_not_restart_or_stop_other_jobs(
    monkeypatch, tmp_path, fake_worker
):
    """A node failure is counted while subsequent nodes still finish normally."""

    async def check():
        progress = Progress()

        async def discover(*, emit):
            for name in ("bad", "good", "unchanged", "dynamic"):
                await emit(make_node(name=name))

        async with tracked_processes(monkeypatch) as children:
            task = asyncio.create_task(
                supervisor.supervise(
                    **arguments(
                        tmp_path=tmp_path,
                        fake_worker=fake_worker,
                        discover=discover,
                        progress=progress,
                    )
                )
            )
            await finish(task)
            assert len(children) == 1

        assert progress.discovered == 4
        assert (
            progress.failed
            == progress.completed
            == progress.unchanged
            == progress.skipped
            == 1
        )
        assert progress.transferred_bytes == 13
        assert progress.completed_bytes == 7

    asyncio.run(check())


def test_crashed_job_is_requeued_only_after_previous_process_exits(
    monkeypatch, tmp_path, fake_worker
):
    """A restarted worker gets the same job ID without overlapping its old owner."""

    async def check():
        progress = Progress()

        async def discover(*, emit):
            await emit(make_node(name="first"))

        async with tracked_processes(monkeypatch) as children:
            task = asyncio.create_task(
                supervisor.supervise(
                    **arguments(
                        tmp_path=tmp_path,
                        fake_worker=fake_worker,
                        discover=discover,
                        progress=progress,
                        mode="crash_once",
                    )
                )
            )
            await finish(task)
            assert len(children) == 2
            assert children[0].returncode == 7
            assert children[1].returncode == 0

        events = read_events(tmp_path)
        assignments = [e for e in events if e["event"] == "started"]
        assert len(assignments) == 2
        assert assignments[0]["job_id"] == assignments[1]["job_id"]
        retry = next(e for e in events if e["event"] == "retry_started")
        assert not retry["old_alive"]
        assert progress.completed == progress.discovered == 1
        assert progress.failed == 0
        assert progress.transferred_bytes == 18
        assert progress.completed_bytes == 7

    asyncio.run(check())


def test_restart_budget_is_bounded(monkeypatch, tmp_path, fake_worker):
    """Repeated process crashes cannot cause an infinite restart loop."""

    async def check():
        progress = Progress()

        async def discover(*, emit):
            await emit(make_node(name="first"))

        async with tracked_processes(monkeypatch) as children:
            task = asyncio.create_task(
                supervisor.supervise(
                    **arguments(
                        tmp_path=tmp_path,
                        fake_worker=fake_worker,
                        discover=discover,
                        progress=progress,
                        mode="crash_always",
                    ),
                    worker_restarts=1,
                )
            )
            with pytest.raises(RuntimeError, match="restart budget"):
                await finish(task)
            assert len(children) == 2
            assert all(child.returncode is not None for child in children)
            assert progress.completed == 0

    asyncio.run(check())


def test_duplicate_acknowledgement_fails_without_double_counting(
    monkeypatch, tmp_path, fake_worker
):
    """A child cannot acknowledge another assignment by replaying an old result."""

    async def check():
        progress = Progress()

        async def discover(*, emit):
            await emit(make_node(name="first"))
            await asyncio.Event().wait()

        async with tracked_processes(monkeypatch) as children:
            task = asyncio.create_task(
                supervisor.supervise(
                    **arguments(
                        tmp_path=tmp_path,
                        fake_worker=fake_worker,
                        discover=discover,
                        progress=progress,
                        mode="duplicate",
                    )
                )
            )
            with pytest.raises(ValueError, match="unassigned"):
                await finish(task)
            assert progress.completed == 1
            assert all(child.returncode is not None for child in children)

    asyncio.run(check())


def test_discovery_failure_drains_discovered_jobs(monkeypatch, tmp_path, fake_worker):
    """A listing error releases reserved slots and preserves already found work."""

    async def check():
        progress = Progress()

        async def discover(*, emit):
            await emit(make_node(name="large", size=100))
            raise OSError("fake listing failure")

        async with tracked_processes(monkeypatch):
            task = asyncio.create_task(
                supervisor.supervise(
                    **arguments(
                        tmp_path=tmp_path,
                        fake_worker=fake_worker,
                        discover=discover,
                        progress=progress,
                    )
                )
            )
            await finish(task)

        assert progress.discovered == progress.completed == 1
        assert progress.failed == 1
        assert progress.discovery_complete
        assert progress.transferred_bytes == 13
        assert progress.completed_bytes == 7

    asyncio.run(check())


@pytest.mark.parametrize("mode", ["busy", "spam"])
def test_cancellation_terminates_busy_children_without_pipe_deadlock(
    monkeypatch, tmp_path, fake_worker, mode
):
    """Cancellation reaps children even when stdout floods a bounded event queue."""

    async def check():
        progress = Progress()

        async def discover(*, emit):
            await emit(make_node(name="first"))
            await asyncio.Event().wait()

        async with tracked_processes(monkeypatch) as children:
            task = asyncio.create_task(
                supervisor.supervise(
                    **arguments(
                        tmp_path=tmp_path,
                        fake_worker=fake_worker,
                        discover=discover,
                        progress=progress,
                        mode=mode,
                    )
                )
            )
            try:
                await until(
                    lambda: any(e["event"] == "started" for e in read_events(tmp_path))
                )
                if mode == "spam":
                    await until(lambda: progress.transferred_bytes > 10)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await finish(task, timeout=3)
                assert all(child.returncode is not None for child in children)
                assert progress.active == 0
            finally:
                if not task.done():
                    task.cancel()

    asyncio.run(check())


@pytest.mark.parametrize("failure", ["cancelled", "protocol_error"])
def test_worker_stops_before_discovery_cleanup_finishes(
    monkeypatch, tmp_path, fake_worker, failure
):
    """A slow scanner cleanup must not delay stopping a real child process."""

    async def check():
        progress = Progress()
        cleanup_started, release_cleanup = asyncio.Event(), asyncio.Event()

        async def discover(*, emit):
            await emit(make_node(name="first"))
            try:
                await asyncio.Event().wait()
            finally:
                cleanup_started.set()
                await release_cleanup.wait()

        async with tracked_processes(monkeypatch) as children:
            task = asyncio.create_task(
                supervisor.supervise(
                    **arguments(
                        tmp_path=tmp_path,
                        fake_worker=fake_worker,
                        discover=discover,
                        progress=progress,
                        mode="busy" if failure == "cancelled" else "duplicate",
                    )
                )
            )
            try:
                if failure == "cancelled":
                    await until(
                        lambda: any(
                            e["event"] == "started" for e in read_events(tmp_path)
                        )
                    )
                    task.cancel()
                await asyncio.wait_for(cleanup_started.wait(), timeout=3)
                assert len(children) == 1
                await asyncio.wait_for(children[0].wait(), timeout=3)
                assert not release_cleanup.is_set()
                assert not task.done()
            finally:
                release_cleanup.set()
                outcome = await asyncio.wait_for(
                    asyncio.gather(task, return_exceptions=True), timeout=5
                )

            if failure == "cancelled":
                assert isinstance(outcome[0], asyncio.CancelledError)
            else:
                assert isinstance(outcome[0], ValueError)
                assert "unassigned" in str(outcome[0])
            assert children[0].returncode is not None
            assert progress.active == 0

    asyncio.run(check())


def test_known_batch_is_prioritized_before_dispatch(monkeypatch, tmp_path, fake_worker):
    """A ready pool chooses a known small item before earlier large discoveries."""

    async def check():
        progress = Progress()

        async def discover(*, emit):
            await until(lambda: progress.transferred_bytes == 1)
            await emit(make_node(name="large-first", size=100))
            await emit(make_node(name="large-second", size=200))
            await emit(make_node(name="small", size=1))

        options = arguments(
            tmp_path=tmp_path,
            fake_worker=fake_worker,
            discover=discover,
            progress=progress,
            concurrency=3,
        )
        options["config"].update(announce_ready=True, queue_size=16)
        async with tracked_processes(monkeypatch):
            task = asyncio.create_task(supervisor.supervise(**options))
            await finish(task)

        starts = [e["path"] for e in read_events(tmp_path) if e["event"] == "started"]
        assert starts == [
            "//source/small",
            "//source/large-first",
            "//source/large-second",
        ]
        assert progress.completed == 3
        assert progress.transferred_bytes == 40
        assert progress.completed_bytes == 21

    asyncio.run(check())


def test_pool_assigns_only_available_slots_and_aggregates_processes(
    monkeypatch, tmp_path, fake_worker
):
    """Two children receive no prefetch beyond their two slots, then drain work."""

    async def check():
        progress = Progress()

        async def discover(*, emit):
            for index in range(7):
                await emit(make_node(name=f"large-{index}", size=100 + index))

        options = arguments(
            tmp_path=tmp_path,
            fake_worker=fake_worker,
            discover=discover,
            progress=progress,
            mode="hold",
            concurrency=2,
        )
        options["processes"] = 2
        async with tracked_processes(monkeypatch) as children:
            task = asyncio.create_task(supervisor.supervise(**options))
            try:
                await until(
                    lambda: (
                        len(
                            [
                                e
                                for e in read_events(tmp_path)
                                if e["event"] == "started"
                            ]
                        )
                        == 4
                    )
                )
                starts = [e for e in read_events(tmp_path) if e["event"] == "started"]
                pids = {e["pid"] for e in starts}
                assert len(pids) == 2
                assert all(sum(e["pid"] == pid for e in starts) == 2 for pid in pids)
                assert progress.active == 4
                assert progress.queued == 3
                (tmp_path / "release").touch()
                await finish(task)
                assert all(child.returncode == 0 for child in children)
            finally:
                if not task.done():
                    task.cancel()

        assert progress.completed == progress.discovered == 7
        assert progress.failed == 0
        assert progress.transferred_bytes == 91
        assert progress.completed_bytes == 49

    asyncio.run(check())
