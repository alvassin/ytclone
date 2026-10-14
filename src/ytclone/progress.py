"""Copy counters and periodic console summaries, independent of the adapters."""

import asyncio
import logging
import math
import time
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field
from datetime import datetime
from typing import TextIO

log = logging.getLogger(__name__)


@dataclass
class Timing:
    count: int = 0
    seconds: float = 0.0
    errors: int = 0


@dataclass
class Progress:
    estimated_total: int | None = None
    estimated_source_bytes: int | None = None
    source_observed_bytes: int = 0
    source_copied_bytes: float = 0.0
    source_skipped_bytes: int = 0
    source_partial_nodes: float = 0.0
    source_size_missing: bool = False
    discovery_complete: bool = False
    discovered: int = 0
    completed: int = 0
    unchanged: int = 0
    skipped: int = 0
    failed: int = 0
    active: int = 0
    queued: int = 0
    transferred_bytes: int = 0
    completed_bytes: int = 0
    timings: dict[str, Timing] = field(default_factory=dict)

    def record_result(self, *, status: str, reason: str) -> None:
        """Count one finished node, separating unchanged from other skips."""
        if status == "completed":
            self.completed += 1
        elif status == "skipped" and reason == "unchanged":
            self.unchanged += 1
        elif status == "skipped":
            self.skipped += 1
        elif status == "failed":
            self.failed += 1
        else:
            raise ValueError(f"Unknown copy status: {status}")

    @contextmanager
    def measure(self, *, operation: str) -> Iterator[None]:
        """Measure one operation on the event loop, including failed attempts."""
        started = time.monotonic()
        timing = self.timings.setdefault(operation, Timing())
        try:
            yield
        except BaseException:
            timing.errors += 1
            raise
        finally:
            timing.count += 1
            timing.seconds += time.monotonic() - started


def format_bytes(size: float) -> str:
    """Format a byte count using binary units."""
    for unit in ("B", "KiB", "MiB", "GiB", "TiB", "PiB"):
        if size < 1024 or unit == "PiB":
            return f"{size:.1f} {unit}"
        size /= 1024
    raise AssertionError("Unreachable size unit")


def estimate_eta(*, progress: Progress, elapsed: float, state: str) -> float | None:
    """Estimate remaining time from source weights and node throughput."""
    if state == "finished" and not progress.failed:
        return 0.0
    total_nodes = (
        progress.discovered if progress.discovery_complete else progress.estimated_total
    )
    if (
        state != "running"
        or elapsed < 10
        or progress.failed
        or progress.source_size_missing
        or total_nodes is None
        or progress.estimated_source_bytes is None
        or (
            progress.estimated_total is not None
            and progress.discovered > progress.estimated_total
        )
        or progress.source_observed_bytes > progress.estimated_source_bytes
    ):
        return None

    done_nodes = (
        progress.completed
        + progress.unchanged
        + progress.skipped
        + progress.source_partial_nodes
    )
    remaining_nodes = max(0.0, total_nodes - done_nodes)
    remaining_bytes = max(
        0.0,
        progress.estimated_source_bytes
        - progress.source_copied_bytes
        - progress.source_skipped_bytes,
    )
    # Fractional weights can leave sub-byte rounding residue after completion.
    if remaining_bytes < 1:
        remaining_bytes = 0.0
    if done_nodes <= 0 or (remaining_nodes == 0 and remaining_bytes == 0):
        # Even the last uploaded part still needs its manifest and final scan.
        return None
    node_seconds = remaining_nodes * elapsed / done_nodes
    if remaining_bytes == 0:
        return node_seconds
    if progress.source_copied_bytes <= 0:
        # Skipping existing data says nothing about transfer throughput.
        return None
    byte_seconds = remaining_bytes * elapsed / progress.source_copied_bytes
    return byte_seconds


def format_eta(seconds: float | None) -> str:
    """Show unknown, finished, or an explicitly approximate duration."""
    if seconds is None:
        return "?"
    if seconds == 0:
        return "0s"
    remaining = max(1, math.ceil(seconds))
    hours, remaining = divmod(remaining, 3600)
    minutes, remaining = divmod(remaining, 60)
    if hours:
        return f"~{hours}h{minutes:02d}m"
    if minutes:
        return f"~{minutes}m{remaining:02d}s"
    return f"~{remaining}s"


def format_progress(
    *, progress: Progress, concurrency: int, elapsed: float, state: str, export: bool
) -> str:
    """Render node counts, compressed data volumes and cumulative timings."""
    total = "?" if progress.estimated_total is None else f"~{progress.estimated_total}"
    transferred = "uploaded" if export else "downloaded"
    completed = "published" if export else "restored"
    rate = progress.transferred_bytes / elapsed if elapsed > 0 else 0
    fields = [
        state,
        f"elapsed={elapsed:.0f}s",
        f"discovered={progress.discovered}/{total}",
        f"copied={progress.completed}",
        f"unchanged={progress.unchanged}",
        f"skipped={progress.skipped}",
        f"errors={progress.failed}",
        f"queued={progress.queued}",
        f"active={progress.active}/{concurrency}",
        f"{transferred}={format_bytes(progress.transferred_bytes)}",
        f"{completed}={format_bytes(progress.completed_bytes)}",
        f"avg_rate={format_bytes(rate)}/s",
    ]
    if export:
        source_size = (
            "?"
            if progress.estimated_source_bytes is None
            else "~" + format_bytes(progress.estimated_source_bytes)
        )
        fields.extend(
            (
                f"source_size={source_size}",
                f"source_processed={format_bytes(progress.source_copied_bytes)}",
                f"source_skipped={format_bytes(progress.source_skipped_bytes)}",
                f"eta={format_eta(estimate_eta(progress=progress, elapsed=elapsed, state=state))}",
            )
        )

    for name in ("yt_metadata", "yt_read", "yt_write", "s3", "s3_download"):
        timing = progress.timings.get(name)
        if timing is not None and timing.count:
            if name in ("yt_read", "yt_write", "s3_download"):
                fields.append(f"{name}={timing.seconds:.1f}s({timing.errors}errors)")
            else:
                average_ms = timing.seconds * 1000 / timing.count
                unit = "req" if name == "s3" else "op"
                fields.append(
                    f"{name}={average_ms:.0f}ms/{unit}"
                    f"({timing.count}{unit},{timing.errors}errors)"
                )
    return " ".join(fields)


class ProgressReporter:
    def __init__(
        self,
        *,
        progress: Progress,
        concurrency: int,
        interval: float,
        export: bool,
        stream: TextIO,
    ):
        """Bind a progress view to a console stream."""
        self.progress = progress
        self.concurrency = concurrency
        self.interval = interval
        self.export = export
        self.stream = stream
        self.started = time.monotonic()
        self.task: asyncio.Task | None = None

    def write(self, *, state: str) -> None:
        """Write a dated summary and flush redirected stdout as well."""
        timestamp = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %z")
        summary = format_progress(
            progress=self.progress,
            concurrency=self.concurrency,
            elapsed=time.monotonic() - self.started,
            state=state,
            export=self.export,
        )
        self.stream.write(f"{timestamp} {summary}\n")
        self.stream.flush()
        log.info("copy_progress %s", summary)

    async def _report(self) -> None:
        """Report periodically without blocking copy workers."""
        while True:
            await asyncio.sleep(self.interval)
            self.write(state="running")

    async def __aenter__(self):
        """Start with an immediate summary, then report periodically."""
        self.write(state="running")
        self.task = asyncio.create_task(self._report())
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        """Stop the reporter and always print the final copy state."""
        self.task.cancel()
        with suppress(asyncio.CancelledError):
            await self.task
        if isinstance(exc, (asyncio.CancelledError, KeyboardInterrupt)):
            state = "cancelled"
        elif exc is not None or self.progress.failed:
            state = "failed"
        else:
            state = "finished"
        self.write(state=state)
