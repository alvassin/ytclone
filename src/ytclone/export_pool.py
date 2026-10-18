"""One fixed pool of range readers shared by every admitted export node."""

import asyncio
import logging
from collections import deque
from collections.abc import Awaitable, Callable, Iterator
from contextlib import AsyncExitStack
from dataclasses import dataclass, field

from .format import Part, Range
from .interfaces import WorkerLifecycleError, YtBackend, YtWorkerFactory

Transfer = Callable[..., Awaitable[Part]]
log = logging.getLogger(__name__)


@dataclass(eq=False)
class _Job:
    parts: Iterator[Range]
    next_part: Range
    transfer: Transfer
    parallel: bool
    done: asyncio.Future[None]
    active: int = 0
    exhausted: bool = False
    error: BaseException | None = None
    completed: list[tuple[int, Part]] = field(default_factory=list)


class PartPool:
    """Schedule lazy node ranges without making node owners occupy reader slots.

    State changes contain no awaits. Results go directly into node state, so a
    full work queue can never prevent readers from reporting their completion.
    """

    def __init__(self, *, yt: YtWorkerFactory, concurrency: int):
        if concurrency < 1:
            raise ValueError("Part pool concurrency must be positive")
        self.yt = yt
        self.concurrency = concurrency
        self._ready: deque[_Job] = deque()
        self._jobs: set[_Job] = set()
        self._wake = asyncio.Event()
        self._workers: list[asyncio.Task] = []
        self._entered = False
        self._stopping = False
        self._failure: BaseException | None = None
        self._close_task: asyncio.Task | None = None

    async def __aenter__(self):
        if self._entered or self._stopping:
            raise RuntimeError("Part pool cannot be reopened")
        self._entered = True
        self._workers = [
            asyncio.create_task(self._worker()) for _ in range(self.concurrency)
        ]
        return self

    async def __aexit__(self, *exc):
        await self.close()

    def check(self) -> None:
        """Reject new node work after a reader lifecycle failure or shutdown."""
        if not self._entered or self._stopping:
            if self._failure is not None:
                raise self._failure
            raise RuntimeError("Part pool is not running")

    async def copy(
        self, *, parts: Iterator[Range], transfer: Transfer, parallel: bool
    ) -> list[Part]:
        """Return ordered results only after all assigned reads have been drained."""
        self.check()
        parts = iter(parts)
        try:
            first = next(parts)
        except StopIteration:
            return []
        job = _Job(
            parts=parts,
            next_part=first,
            transfer=transfer,
            parallel=parallel,
            done=asyncio.get_running_loop().create_future(),
        )
        self._jobs.add(job)
        self._ready.append(job)
        self._wake.set()
        try:
            try:
                await asyncio.shield(job.done)
            except asyncio.CancelledError as error:
                self._stop_job(job=job, error=error)
                # A cancelled owner must retain its snapshot until every reader
                # has closed, including when shutdown sends another cancellation.
                while not job.done.done():
                    try:
                        await asyncio.shield(job.done)
                    except asyncio.CancelledError:
                        pass
                raise
            if job.error is not None:
                raise job.error
            return [part for _, part in sorted(job.completed, key=lambda item: item[0])]
        finally:
            self._jobs.discard(job)

    def _finish(self, *, job: _Job) -> None:
        if (
            (job.exhausted or job.error is not None)
            and job.active == 0
            and not job.done.done()
        ):
            job.done.set_result(None)

    def _stop_job(self, *, job: _Job, error: BaseException) -> None:
        if job.error is None:
            job.error = error
        self._finish(job=job)
        self._wake.set()

    def _take(self) -> tuple[_Job, Range] | None:
        """Give each ready node a turn; files/documents have one active range."""
        for _ in range(len(self._ready)):
            job = self._ready.popleft()
            if job.error is not None or job.exhausted:
                self._finish(job=job)
                continue
            if not job.parallel and job.active:
                self._ready.append(job)
                continue
            part = job.next_part
            try:
                job.next_part = next(job.parts)
            except StopIteration:
                job.exhausted = True
            except Exception as error:
                log.exception("export_range_planning_failed")
                self._stop_job(job=job, error=error)
                continue
            job.active += 1
            if not job.exhausted:
                self._ready.append(job)
            return job, part
        return None

    async def _next(self) -> tuple[_Job, Range] | None:
        while not self._stopping:
            assignment = self._take()
            if assignment is not None:
                return assignment
            self._wake.clear()
            await self._wake.wait()
        return None

    async def _worker(self) -> None:
        """Keep one backend for consecutive ranges, opening it only when needed."""
        try:
            async with AsyncExitStack() as stack:
                reader: YtBackend | None = None
                while (assignment := await self._next()) is not None:
                    job, part = assignment
                    try:
                        if reader is None:
                            reader = await stack.enter_async_context(self.yt.worker())
                        try:
                            result = await job.transfer(reader=reader, part=part)
                        except WorkerLifecycleError:
                            raise
                        except Exception as error:
                            log.exception("export_range_failed part=%d", part.index)
                            self._stop_job(job=job, error=error)
                        else:
                            job.completed.append((part.index, result))
                    except BaseException as error:
                        self._stop_job(job=job, error=error)
                        if not self._stopping:
                            self._failure = error
                            self._abort(error=error)
                        raise
                    finally:
                        job.active -= 1
                        self._finish(job=job)
                        self._wake.set()
        except BaseException as error:
            if not self._stopping:
                self._failure = error
                self._abort(error=error)
            raise

    def _abort(self, *, error: BaseException) -> None:
        """Stop issuing work before cancelling readers, exactly once."""
        if self._stopping:
            return
        self._stopping = True
        for job in self._jobs:
            self._stop_job(job=job, error=error)
        self._ready.clear()
        current = asyncio.current_task()
        for worker in self._workers:
            if worker is not current and not worker.done():
                worker.cancel()
        self._wake.set()

    async def _close(self) -> None:
        self._abort(error=RuntimeError("Part pool stopped before export completed"))
        results = await asyncio.gather(*self._workers, return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException) and not isinstance(
                result, asyncio.CancelledError
            ):
                raise result

    async def close(self) -> None:
        """Drain readers before callers close snapshots; repeated calls are safe."""
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close())
        try:
            await asyncio.shield(self._close_task)
        except asyncio.CancelledError:
            while not self._close_task.done():
                try:
                    await asyncio.shield(self._close_task)
                except asyncio.CancelledError:
                    pass
            # Retrieve any cleanup failure even if the caller was cancelled.
            self._close_task.result()
            raise
