"""Own assignments and restart exited workers without overlapping writers."""

import asyncio
import logging
import os
import signal
import sys
from contextlib import suppress
from dataclasses import asdict, dataclass, field
from pathlib import Path

from ..format import DiscoveredNode
from ..progress import Progress
from .protocol import MAX_MESSAGE_SIZE, read_message, write_message
from .scheduler import Job, Scheduler

log = logging.getLogger(__name__)


@dataclass
class _Process:
    index: int
    process: asyncio.subprocess.Process | None = None
    monitor: asyncio.Task | None = None
    ready: bool = False
    restarts: int = 0
    jobs: dict[int, Job] = field(default_factory=dict)
    uploaded: int = 0
    published: int = 0


def worker_command() -> list[str]:
    """Launch fresh interpreters, including a future bundled entry point."""
    if getattr(sys, "frozen", False):
        return [sys.executable, "--parallel-worker"]
    return [sys.executable, "-m", "ytclone.parallel.worker"]


async def _stop_process(*, slot: _Process, graceful: bool) -> None:
    """Wait for termination before allowing any assignment to be reused."""
    process = slot.process
    if process is None:
        return

    # The event queue is no longer consumed during shutdown. Keep pipe drainage
    # independent of the monitor so even a noisy child can finish and be reaped.
    if slot.monitor is not None:
        slot.monitor.cancel()
        await asyncio.gather(slot.monitor, return_exceptions=True)

    async def drain() -> None:
        """Discard remaining child output while waiting for process exit."""
        while await process.stdout.read(64 * 1024):
            pass

    drainage = asyncio.create_task(drain())
    try:
        await _finish_process(process=process, graceful=graceful)
    finally:
        if process.stdin is not None:
            process.stdin.close()
        drainage.cancel()
        await asyncio.gather(drainage, return_exceptions=True)


def _signal_process(*, process: asyncio.subprocess.Process, signum: int) -> None:
    """Signal only the independent session created for this worker."""
    with suppress(ProcessLookupError):
        if os.name == "posix":
            os.killpg(process.pid, signum)
        else:
            process.send_signal(signum)


async def _finish_process(
    *, process: asyncio.subprocess.Process, graceful: bool
) -> None:
    """Stop or terminate a child and wait for its actual exit."""
    if process.returncode is None:
        if graceful:
            try:
                await asyncio.wait_for(
                    fut=write_message(writer=process.stdin, message={"type": "stop"}),
                    timeout=5,
                )
            except (ConnectionError, BrokenPipeError, asyncio.TimeoutError):
                _signal_process(process=process, signum=signal.SIGTERM)
        else:
            _signal_process(process=process, signum=signal.SIGTERM)
        try:
            await asyncio.wait_for(fut=process.wait(), timeout=15)
        except asyncio.TimeoutError:
            _signal_process(process=process, signum=signal.SIGKILL)
            await process.wait()


async def supervise(
    *,
    discover,
    config: dict,
    processes: int,
    small_size: int,
    progress: Progress,
    worker_restarts: int = 3,
    restart_delay: float = 1.0,
    command: list[str] | None = None,
) -> None:
    """Stream discoveries into a small-first queue and supervise child lifetimes."""
    if processes < 1 or config["concurrency"] < 1 or worker_restarts < 0:
        raise ValueError("Invalid process, concurrency or restart count")
    if restart_delay < 0:
        raise ValueError("Restart delay must not be negative")

    scheduler = Scheduler(small_size=small_size)
    events: asyncio.Queue = asyncio.Queue(
        maxsize=config.get("queue_size", 16) * processes
    )
    slots = [_Process(index=index) for index in range(1, processes + 1)]
    retry_tasks: set[asyncio.Task] = set()
    discovery_complete = False
    success = False
    command = command if command is not None else worker_command()

    async def emit(node: DiscoveredNode) -> None:
        """Backpressure discovery event delivery, never table contents."""
        # All callbacks run on this event loop. Make metadata visible to the
        # priority queue immediately, before dispatch can overtake its wake event.
        scheduler.add(node=node)
        progress.discovered += 1
        await events.put(("discovered", None, None))

    async def scan() -> None:
        """Report discovery failure while preserving already queued work."""
        try:
            await discover(emit=emit)
        except Exception:
            log.exception("parallel_discovery_failed")
            await events.put(("discovery_failed", None, None))
        else:
            await events.put(("discovery_done", None, None))

    async def monitor(slot: _Process) -> None:
        """Deliver messages before the confirmed-exit event for this process."""
        process = slot.process
        failure = None
        try:
            ready = False
            while True:
                message = (
                    await read_message(process.stdout)
                    if ready
                    else await asyncio.wait_for(
                        fut=read_message(process.stdout), timeout=60
                    )
                )
                if message is None:
                    break
                if message.get("type") == "ready":
                    ready = True
                await events.put(("message", slot, message))
            if process.returncode is None:
                # Closing the control channel means this process cannot work anymore.
                with suppress(ProcessLookupError):
                    process.terminate()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            failure = type(error).__name__
            log.exception("worker_protocol_failed worker=%d", slot.index)
            if process.returncode is None:
                with suppress(ProcessLookupError):
                    process.terminate()
        try:
            code = await asyncio.wait_for(fut=process.wait(), timeout=15)
        except asyncio.TimeoutError:
            with suppress(ProcessLookupError):
                process.kill()
            code = await process.wait()
        await events.put(("exit", slot, (code, failure)))

    async def launch(slot: _Process) -> None:
        """Start an isolated client process and send nonsecret configuration."""
        worker_config = dict(config)
        if config.get("log_file"):
            path = Path(config["log_file"])
            worker_config["log_file"] = str(
                path.with_name(
                    f"{path.stem}-worker-{slot.index}{path.suffix or '.log'}"
                )
            )
        slot.ready = False
        slot.uploaded = slot.published = 0
        slot.process = await asyncio.create_subprocess_exec(
            *command,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            limit=MAX_MESSAGE_SIZE,
            start_new_session=True,
        )
        slot.monitor = asyncio.create_task(monitor(slot))
        with suppress(ConnectionError, BrokenPipeError):
            await write_message(
                writer=slot.process.stdin,
                message={"type": "config", "config": worker_config},
            )
        log.info("worker_started worker=%d pid=%d", slot.index, slot.process.pid)

    async def request_restart(slot: _Process) -> None:
        """Delay a replacement without blocking healthy workers or discovery."""
        await asyncio.sleep(min(30.0, restart_delay * 2 ** (slot.restarts - 1)))
        await events.put(("restart", slot, None))

    async def dispatch() -> None:
        """Fill free slots without preassigning work to busy processes."""
        capacity = sum(config["concurrency"] for slot in slots if slot.ready)
        for slot in slots:
            if not slot.ready:
                continue
            while len(slot.jobs) < config["concurrency"]:
                job = scheduler.take(
                    active=scheduler.inflight_count,
                    capacity=capacity,
                    discovery_complete=discovery_complete,
                )
                if job is None:
                    break
                slot.jobs[job.id] = job
                try:
                    await write_message(
                        writer=slot.process.stdin,
                        message={"type": "job", "id": job.id, "node": asdict(job.node)},
                    )
                except (ConnectionError, BrokenPipeError):
                    # Only the monitor may requeue this job, after process exit.
                    slot.ready = False
                    if slot.process.returncode is None:
                        with suppress(ProcessLookupError):
                            slot.process.terminate()
                    break
                log.debug(
                    "job_assigned worker=%d job=%d path=%s",
                    slot.index,
                    job.id,
                    job.node.path,
                )

    scanner = asyncio.create_task(scan())
    try:
        for slot in slots:
            await launch(slot)

        while True:
            await dispatch()
            progress.queued = scheduler.pending_count
            progress.active = scheduler.inflight_count
            if discovery_complete and not progress.queued and not progress.active:
                success = True
                break

            kind, slot, value = await events.get()
            if kind == "discovered":
                pass
            elif kind in ("discovery_done", "discovery_failed"):
                discovery_complete = True
                progress.discovery_complete = True
                if kind == "discovery_failed":
                    progress.failed += 1
            elif kind == "restart":
                await launch(slot)
            elif kind == "exit":
                slot.ready = False
                code, failure = value
                for job in slot.jobs.values():
                    scheduler.retry(job=job)
                slot.jobs.clear()
                if slot.process.stdin is not None:
                    slot.process.stdin.close()
                slot.process = None
                log.warning(
                    "worker_exited worker=%d code=%s protocol_error=%s",
                    slot.index,
                    code,
                    failure,
                )
                if slot.restarts >= worker_restarts:
                    raise RuntimeError(f"Worker {slot.index} exhausted restart budget")
                slot.restarts += 1
                task = asyncio.create_task(request_restart(slot))
                retry_tasks.add(task)
                task.add_done_callback(retry_tasks.discard)
            elif kind == "message":
                message_type = value.get("type")
                if message_type == "ready":
                    if slot.ready:
                        raise ValueError("Worker sent duplicate ready")
                    slot.ready = True
                elif message_type == "result":
                    job_id = value.get("id")
                    if type(job_id) is not int or job_id not in slot.jobs:
                        raise ValueError("Worker acknowledged an unassigned job")
                    status, reason = value.get("status"), value.get("reason")
                    if status not in (
                        "completed",
                        "skipped",
                        "failed",
                    ) or not isinstance(reason, str):
                        raise ValueError("Invalid worker result")
                    job = scheduler.complete(job_id=job_id)
                    del slot.jobs[job_id]
                    progress.record_result(status=status, reason=reason)
                    log.info(
                        "node_%s worker=%d path=%s reason=%s",
                        status,
                        slot.index,
                        job.node.path,
                        reason,
                    )
                elif message_type == "progress":
                    uploaded, published = value.get("uploaded"), value.get("published")
                    if any(type(v) is not int or v < 0 for v in (uploaded, published)):
                        raise ValueError("Invalid worker counters")
                    if uploaded < slot.uploaded or published < slot.published:
                        raise ValueError("Worker counters decreased")
                    progress.transferred_bytes += uploaded - slot.uploaded
                    progress.completed_bytes += published - slot.published
                    slot.uploaded, slot.published = uploaded, published
                elif message_type == "fatal":
                    log.error(
                        "worker_fatal worker=%d error_type=%s",
                        slot.index,
                        value.get("error_type"),
                    )
                else:
                    raise ValueError("Unknown worker message type")
    finally:
        scanner.cancel()
        for task in retry_tasks:
            task.cancel()
        # Discovery cleanup may wait for a blocking YT request. Start stopping
        # workers immediately so they do not keep copying while it finishes.
        worker_cleanup = asyncio.gather(
            *(_stop_process(slot=slot, graceful=success) for slot in slots)
        )
        try:
            await asyncio.gather(scanner, *retry_tasks, return_exceptions=True)
        finally:
            await worker_cleanup
        progress.active = 0
