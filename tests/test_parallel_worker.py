"""Private subprocess transfers and cancellation without YT or S3 access."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import asdict
from importlib.metadata import version
from types import SimpleNamespace

import pytest

from ytclone import storage as archive_storage
from ytclone.engine import CopyOptions
from ytclone.format import DiscoveredNode
from ytclone.parallel import worker
from ytclone.parallel.protocol import decode_message, encode_message
from ytclone.progress import Progress


class MemoryWriter:
    """Collect JSON lines while allowing concurrent producers to interleave."""

    def __init__(self):
        """Start with no protocol messages."""
        self.lines = []

    def write(self, data):
        """Record one complete serialized message."""
        self.lines.append(data)

    async def drain(self):
        """Yield to expose unsynchronized output writes."""
        await asyncio.sleep(0)

    @property
    def messages(self):
        """Decode output using the coordinator's protocol."""
        return [decode_message(line=line) for line in self.lines]


class FakeYt:
    """Track persistent backend lifetimes without opening SDK connections."""

    def __init__(self):
        """Start with no clients opened or closed."""
        self.opened = []
        self.closed = []

    @asynccontextmanager
    async def worker(self):
        """Return a distinct backend and record its eventual cleanup."""
        backend = object()
        self.opened.append(backend)
        try:
            yield backend
        finally:
            self.closed.append(backend)


def feed(*, reader, job_id=None, message_type="job"):
    """Send a serialized job or control message to the in-memory input pipe."""
    message = {"type": message_type}
    if message_type == "job":
        message.update(
            id=job_id,
            node=asdict(
                DiscoveredNode(
                    path=f"//source/{job_id}",
                    kind="table",
                    node_id=f"id-{job_id}",
                    estimated_size=1024,
                )
            ),
        )
    reader.feed_data(encode_message(message=message))


def serve(*, reader, writer, yt, concurrency=1, progress=None, interval=100):
    """Configure a worker loop with no network adapters."""
    return worker.serve_jobs(
        reader=reader,
        writer=writer,
        yt=yt,
        storage=object(),
        prefix="archive",
        cluster="https://yt.test",
        options=CopyOptions(concurrency=concurrency),
        progress=Progress() if progress is None else progress,
        progress_interval=interval,
    )


def test_failure_does_not_stop_later_jobs(monkeypatch):
    """A failed node returns only its error type and leaves the client reusable."""
    backends = []

    async def transfer(**kwargs):
        """Fail the first node and complete the next using the same client."""
        backends.append(kwargs["yt"])
        if kwargs["path"].endswith("/1"):
            raise OSError("A diagnostic that must not enter the IPC result")
        kwargs["progress"].transferred_bytes += 100
        kwargs["progress"].completed_bytes += 100
        return "completed", "copied"

    monkeypatch.setattr(worker, "export_node", transfer)

    async def scenario():
        """Drain two jobs and compare publication counters with result order."""
        reader, writer, yt = asyncio.StreamReader(), MemoryWriter(), FakeYt()
        feed(reader=reader, job_id=1)
        feed(reader=reader, job_id=2)
        feed(reader=reader, message_type="stop")
        await serve(reader=reader, writer=writer, yt=yt)

        assert writer.messages == [
            {"type": "ready"},
            {"type": "progress", "uploaded": 0, "published": 0},
            {"type": "result", "id": 1, "status": "failed", "reason": "OSError"},
            {"type": "progress", "uploaded": 100, "published": 100},
            {"type": "result", "id": 2, "status": "completed", "reason": "copied"},
        ]
        assert backends[0] is backends[1]
        assert yt.opened == yt.closed == [backends[0]]

    asyncio.run(scenario())


def test_concurrency_is_bounded_and_clients_persist(monkeypatch):
    """More assignments reuse C backends without creating C clients per node."""

    async def scenario():
        """Hold two transfers and verify that a third cannot start yet."""
        reader, writer, yt = asyncio.StreamReader(), MemoryWriter(), FakeYt()
        released, two_started = asyncio.Event(), asyncio.Event()
        backends = []
        active = peak = 0

        async def transfer(**kwargs):
            """Record active slots and block them until the test releases both."""
            nonlocal active, peak
            backends.append(kwargs["yt"])
            active += 1
            peak = max(peak, active)
            if active == 2:
                two_started.set()
            await released.wait()
            active -= 1
            return "skipped", "unchanged"

        monkeypatch.setattr(worker, "export_node", transfer)
        for job_id in range(3):
            feed(reader=reader, job_id=job_id)
        feed(reader=reader, message_type="stop")
        task = asyncio.create_task(
            serve(reader=reader, writer=writer, yt=yt, concurrency=2)
        )
        await asyncio.wait_for(two_started.wait(), timeout=1)
        assert len(backends) == peak == 2
        released.set()
        await asyncio.wait_for(task, timeout=1)

        assert len(backends) == 3
        assert len(set(backends)) == len(yt.opened) == len(yt.closed) == 2
        assert peak == 2
        results = [item for item in writer.messages if item["type"] == "result"]
        assert len(results) == 3
        assert all(item["status"] == "skipped" for item in results)

    asyncio.run(scenario())


@pytest.mark.parametrize("parent_eof", [True, False])
def test_disconnect_and_cancellation_close_active_transfers(monkeypatch, parent_eof):
    """Parent disappearance or process shutdown cancels the copy and its client."""

    async def scenario():
        """Wait for an active copy before disconnecting its coordinator."""
        reader, writer, yt = asyncio.StreamReader(), MemoryWriter(), FakeYt()
        started, cancelled = asyncio.Event(), asyncio.Event()

        async def transfer(**kwargs):
            """Expose cancellation while simulating a long-running upload."""
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        monkeypatch.setattr(worker, "export_node", transfer)
        feed(reader=reader, job_id=1)
        task = asyncio.create_task(serve(reader=reader, writer=writer, yt=yt))
        await asyncio.wait_for(started.wait(), timeout=1)
        if parent_eof:
            reader.feed_eof()
        else:
            task.cancel()

        expected = worker.ParentDisconnected if parent_eof else asyncio.CancelledError
        with pytest.raises(expected):
            await asyncio.wait_for(task, timeout=1)
        assert cancelled.is_set()
        assert yt.opened == yt.closed
        assert not any(item["type"] == "result" for item in writer.messages)

    asyncio.run(scenario())


def test_periodic_progress_while_node_is_active(monkeypatch):
    """Large active uploads report bytes before publishing a result."""

    async def scenario():
        """Hold the copy until periodic progress appears on its output pipe."""
        reader, writer, yt = asyncio.StreamReader(), MemoryWriter(), FakeYt()
        released = asyncio.Event()
        progress = Progress()

        async def transfer(**kwargs):
            """Simulate uploaded parts of an unfinished table."""
            progress.transferred_bytes = 1234
            await released.wait()
            return "completed", "copied"

        monkeypatch.setattr(worker, "export_node", transfer)
        feed(reader=reader, job_id=1)
        task = asyncio.create_task(
            serve(
                reader=reader,
                writer=writer,
                yt=yt,
                progress=progress,
                interval=0.01,
            )
        )
        try:

            async def wait_for_report():
                """Wait for a cumulative byte sample without completing the copy."""
                while not any(item.get("uploaded") == 1234 for item in writer.messages):
                    await asyncio.sleep(0)

            await asyncio.wait_for(wait_for_report(), timeout=1)
            assert not any(item["type"] == "result" for item in writer.messages)
            feed(reader=reader, message_type="stop")
            released.set()
            await asyncio.wait_for(task, timeout=1)
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    asyncio.run(scenario())


def test_explicit_stop_without_jobs_is_clean():
    """Empty assignments shut down without fabricating completed work."""

    async def scenario():
        """Open and close the worker pool without copying a node."""
        reader, writer, yt = asyncio.StreamReader(), MemoryWriter(), FakeYt()
        feed(reader=reader, message_type="stop")
        await serve(reader=reader, writer=writer, yt=yt, concurrency=2)
        assert writer.messages == [{"type": "ready"}]
        assert len(yt.opened) == len(yt.closed) == 2

    asyncio.run(scenario())


@pytest.mark.parametrize("message_type", ["config", "unknown"])
def test_unexpected_messages_stop_the_pool(message_type):
    """Protocol mistakes are fatal rather than silently leaving queued work."""

    async def scenario():
        """Send an unsupported control message after worker startup."""
        reader, writer, yt = asyncio.StreamReader(), MemoryWriter(), FakeYt()
        feed(reader=reader, message_type=message_type)
        with pytest.raises(ValueError, match="Expected a job or stop"):
            await serve(reader=reader, writer=writer, yt=yt)
        assert yt.opened == yt.closed

    asyncio.run(scenario())


def test_run_worker_builds_process_local_resources(monkeypatch):
    """Resource wiring uses explicit endpoint settings and inherited credentials."""
    seen = {}

    class Session:
        """Record the HTTP session lifetime without creating connections."""

        def __init__(self, **kwargs):
            """Record supplied session options."""
            seen["session_options"] = kwargs

        async def __aenter__(self):
            """Make this session available to the S3 client."""
            return self

        async def __aexit__(self, *args):
            """Record resource cleanup."""
            seen["session_closed"] = True

    def adapter(**kwargs):
        """Build a fake process-local YT factory."""
        seen["yt_options"] = kwargs
        return FakeYt()

    def s3_client(**kwargs):
        """Capture endpoint and region without reading any credentials."""
        seen["s3_options"] = kwargs
        return object()

    async def serve_fake(**kwargs):
        """Inspect transfer settings instead of consuming network data."""
        seen["serve"] = kwargs

    monkeypatch.setattr(archive_storage.aiohttp, "ClientSession", Session)
    monkeypatch.setattr(worker, "YtAdapter", adapter)
    monkeypatch.setattr(archive_storage, "S3Client", s3_client)
    monkeypatch.setattr(
        archive_storage, "S3Storage", lambda **kwargs: SimpleNamespace(**kwargs)
    )
    monkeypatch.setattr(worker, "serve_jobs", serve_fake)

    async def scenario():
        """Pass a full coordinator configuration through the real wiring."""
        config = {
            "yt_proxy": "https://yt.test/",
            "proxy_discovery": False,
            "endpoint": "https://s3.test/",
            "region": "region-test",
            "storage": {
                "kind": "s3",
                "bucket": "archive-bucket",
                "prefix": "snapshots",
            },
            "concurrency": 3,
            "part_size": 256 * 1024**2,
            "chunk_size": 32 * 1024**2,
            "compression_level": 3,
            "progress_interval": 7,
        }
        await worker.run_worker(
            reader=asyncio.StreamReader(), writer=MemoryWriter(), config=config
        )

    asyncio.run(scenario())
    assert seen["session_closed"]
    assert seen["s3_options"]["url"] == "https://s3.test/archive-bucket"
    assert seen["s3_options"]["region"] == "region-test"
    assert set(seen["s3_options"]) == {"session", "url", "region"}
    assert seen["serve"]["options"].concurrency == 3
    assert seen["serve"]["prefix"] == "snapshots"
    assert seen["serve"]["cluster"] == "https://yt.test"
    assert seen["yt_options"]["proxy_discovery"] is False


def test_local_worker_opens_resolved_root_without_initializing_it(
    monkeypatch, tmp_path
):
    """Workers use the coordinator's root and never create S3 resources or markers."""
    seen = {}

    def unexpected(*args, **kwargs):
        pytest.fail("A local worker must only open its already resolved archive")

    async def serve_fake(**kwargs):
        seen.update(kwargs)

    monkeypatch.setattr(archive_storage.aiohttp, "ClientSession", unexpected)
    monkeypatch.setattr(archive_storage, "S3Client", unexpected)
    monkeypatch.setattr(archive_storage, "resolve_local_archive", unexpected)
    monkeypatch.setattr(
        archive_storage, "LocalStorage", lambda **kwargs: SimpleNamespace(**kwargs)
    )
    monkeypatch.setattr(worker, "YtAdapter", lambda **kwargs: FakeYt())
    monkeypatch.setattr(worker, "serve_jobs", serve_fake)

    async def scenario():
        await worker.run_worker(
            reader=asyncio.StreamReader(),
            writer=MemoryWriter(),
            config={
                "yt_proxy": "https://yt.test/",
                "proxy_discovery": False,
                "endpoint": "https://unused.test",
                "region": "unused",
                "storage": {"kind": "local", "root": str(tmp_path), "prefix": ""},
                "concurrency": 2,
                "part_size": 256 * 1024**2,
                "chunk_size": 8 * 1024**2,
                "compression_level": 3,
                "progress_interval": 5,
            },
        )

    asyncio.run(scenario())
    assert seen["storage"].root == str(tmp_path)
    assert seen["storage"].chunk_size == 8 * 1024**2
    assert seen["prefix"] == ""
    assert list(tmp_path.iterdir()) == []


def test_entrypoint_stdout_contains_only_protocol(tmp_path):
    """Real subprocess pipes isolate logs and accidental prints from JSON output."""
    import os
    import subprocess
    import sys
    from pathlib import Path

    script = tmp_path / "fake_worker.py"
    script.write_text(
        """from contextlib import asynccontextmanager
from ytclone.parallel import worker
from ytclone import storage as archive_storage

class FakeYt:
    def __init__(self, **kwargs):
        pass

    @asynccontextmanager
    async def worker(self):
        yield object()

async def transfer(**kwargs):
    print("diagnostic on stderr", flush=True)
    kwargs["progress"].transferred_bytes = 10
    kwargs["progress"].completed_bytes = 10
    return "completed", "copied"

worker.YtAdapter = FakeYt
archive_storage.S3Client = lambda **kwargs: object()
archive_storage.S3Storage = lambda **kwargs: object()
worker.export_node = transfer
raise SystemExit(worker.main())
"""
    )
    config = {
        "yt_proxy": "https://unused.test",
        "proxy_discovery": False,
        "endpoint": "https://unused.test",
        "region": "region-test",
        "storage": {"kind": "s3", "bucket": "bucket", "prefix": "archive"},
        "concurrency": 1,
        "part_size": 256 * 1024**2,
        "chunk_size": 32 * 1024**2,
        "compression_level": 3,
        "progress_interval": 100,
        "log_file": None,
        "log_level": "info",
        "log_format": "stream",
    }
    job = {
        "type": "job",
        "id": 1,
        "node": asdict(
            DiscoveredNode(path="//source/table", kind="table", node_id="id-1")
        ),
    }
    payload = b"".join(
        encode_message(message=message)
        for message in ({"type": "config", "config": config}, job, {"type": "stop"})
    )
    result = subprocess.run(
        check=False,
        args=[sys.executable, str(script)],
        input=payload,
        capture_output=True,
        timeout=10,
        env={
            "PATH": os.defpath,
            "PYTHONPATH": str(Path(worker.__file__).parents[2]),
        },
    )
    assert result.returncode == 0, result.stderr.decode()
    assert b"diagnostic on stderr" in result.stderr
    assert b"node_completed" in result.stderr
    messages = [
        decode_message(line=line) for line in result.stdout.splitlines(keepends=True)
    ]
    assert messages == [
        {"type": "ready"},
        {"type": "progress", "uploaded": 10, "published": 10},
        {"type": "result", "id": 1, "status": "completed", "reason": "copied"},
    ]


@pytest.mark.parametrize("signal_name", ["SIGTERM", "SIGINT"])
def test_entrypoint_signals_cancel_active_work(tmp_path, signal_name):
    """OS shutdown signals reach coroutine cleanup before the process exits."""
    import os
    import signal
    import sys
    from pathlib import Path

    script = tmp_path / "signal_worker.py"
    script.write_text(
        """import asyncio
from ytclone.parallel import worker

async def wait_for_shutdown(**kwargs):
    await worker.write_message(writer=kwargs["writer"], message={"type": "ready"})
    try:
        await asyncio.Event().wait()
    finally:
        print("active_copy_cleanup", flush=True)

worker.run_worker = wait_for_shutdown
raise SystemExit(worker.main())
"""
    )

    async def scenario():
        """Wait for worker startup, signal it and collect its termination output."""
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            str(script),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={
                "PATH": os.defpath,
                "PYTHONPATH": str(Path(worker.__file__).parents[2]),
            },
        )
        try:
            process.stdin.write(
                encode_message(
                    message={
                        "type": "config",
                        "config": {
                            "log_file": None,
                            "log_level": "info",
                            "log_format": "stream",
                        },
                    }
                )
            )
            await process.stdin.drain()
            ready = await asyncio.wait_for(process.stdout.readline(), timeout=5)
            assert decode_message(line=ready) == {"type": "ready"}
            process.send_signal(signal=getattr(signal, signal_name))
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=5)
            assert b"active_copy_cleanup" in stderr
            assert not stdout
            dependency_version = version("aiomisc")
            # aiomisc 18.0.33 cancels its own shutdown task in Runner.close()
            # and reports a false 60-second timeout after cleanup already ran.
            # Keep cleanup, protocol and termination checks mandatory; only
            # this known dependency diagnostic is an expected failure.
            if (
                dependency_version == "18.0.33"
                and process.returncode == 70
                and b"aiomisc.entrypoint:Shutdown did not happen in 60.0 seconds, aborting."
                in stderr
                and b"Traceback (most recent call last)" not in stderr
            ):
                pytest.xfail(
                    "aiomisc 18.0.33 reports a false shutdown timeout after cleanup"
                )
            # On Linux/Python 3.10, aiomisc 17.10.3 can stop the loop
            # while its context manager is still awaiting shutdown.
            if (
                dependency_version == "17.10.3"
                and sys.platform == "linux"
                and sys.version_info[:2] == (3, 10)
                and signal_name == "SIGINT"
                and process.returncode == 1
                and b"/aiomisc/entrypoint.py" in stderr
                and b"loop.run_until_complete(self.__aexit__(exc_type, exc_val, exc_tb))"
                in stderr
                and stderr.rstrip().endswith(
                    b"RuntimeError: Event loop stopped before Future completed."
                )
            ):
                pytest.xfail(
                    "aiomisc 17.10.3 stops its loop during SIGINT shutdown on Python 3.10"
                )
            assert process.returncode == 130, stderr.decode(errors="replace")
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()

    asyncio.run(scenario())
