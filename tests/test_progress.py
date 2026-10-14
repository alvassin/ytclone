"""Counters, timing boundaries and summaries without network access."""

import asyncio
import io

import pytest

from ytclone import progress as module
from ytclone.progress import (
    Progress,
    ProgressReporter,
    Timing,
    estimate_eta,
    format_bytes,
    format_eta,
    format_progress,
)


def test_results_keep_unchanged_and_other_skips_separate():
    """Count outcomes without presenting dynamic tables as existing copies."""
    progress = Progress()
    for status, reason in (
        ("completed", "copied"),
        ("skipped", "unchanged"),
        ("skipped", "dynamic"),
        ("failed", "node"),
    ):
        progress.record_result(status=status, reason=reason)
    assert (
        progress.completed,
        progress.unchanged,
        progress.skipped,
        progress.failed,
    ) == (1, 1, 1, 1)
    with pytest.raises(ValueError):
        progress.record_result(status="typo", reason="")


def test_measure_keeps_failed_and_cancelled_operation_times(monkeypatch):
    """Failed work contributes time and errors without swallowing exceptions."""
    clock = iter((0, 2, 4, 7, 8, 9))
    monkeypatch.setattr(module.time, "monotonic", lambda: next(clock))
    progress = Progress()
    with progress.measure(operation="s3"):
        pass
    with pytest.raises(OSError), progress.measure(operation="s3"):
        raise OSError("network")
    with pytest.raises(asyncio.CancelledError), progress.measure(operation="s3"):
        raise asyncio.CancelledError
    assert progress.timings["s3"] == Timing(count=3, seconds=6, errors=2)


@pytest.mark.parametrize(
    "size,expected",
    [
        (0, "0.0 B"),
        (1024, "1.0 KiB"),
        (2**20, "1.0 MiB"),
        (3 * 2**30, "3.0 GiB"),
        (2**40, "1.0 TiB"),
    ],
)
def test_human_volumes(size, expected):
    """Use consistent units for small files and terabyte copies."""
    assert format_bytes(size) == expected


def test_summary_distinguishes_estimate_transfer_and_published_data():
    """Partial uploads must not look like completed recoverable tables."""
    progress = Progress(
        estimated_total=5,
        discovered=7,
        completed=2,
        unchanged=1,
        skipped=1,
        queued=1,
        active=2,
        transferred_bytes=2**30,
        completed_bytes=2**29,
    )
    progress.timings["yt_metadata"] = Timing(count=4, seconds=2)
    progress.timings["s3_download"] = Timing(count=3, seconds=1)
    value = format_progress(
        progress=progress, concurrency=4, elapsed=10, state="running", export=True
    )
    assert "discovered=7/~5" in value
    assert "unchanged=1 skipped=1" in value
    assert "queued=1 active=2/4" in value
    assert "uploaded=1.0 GiB published=512.0 MiB" in value
    assert "yt_metadata=500ms/op" in value
    assert "s3_download=1.0s" in value
    assert "%" not in value  # A stale estimate cannot be a reliable percentage.


def test_unknown_total_and_restore_use_source_archive_units():
    """Restores do not estimate the source from the destination YT tree."""
    value = format_progress(
        progress=Progress(), concurrency=2, elapsed=0, state="running", export=False
    )
    assert "discovered=0/?" in value
    assert "downloaded=0.0 B restored=0.0 B" in value
    assert "avg_rate=0.0 B/s" in value


@pytest.mark.parametrize(
    "error,state",
    [
        (None, "finished"),
        (OSError("failed"), "failed"),
        (asyncio.CancelledError(), "cancelled"),
    ],
)
def test_reporter_prints_periodically_and_always_finishes(error, state):
    """A final summary survives failure and cancellation of the copy."""

    async def exercise():
        """Wait for a periodic output rather than relying on sleep timing."""
        ready = asyncio.Event()

        class Output(io.StringIO):
            def write(self, value):
                """Signal when the initial and first periodic rows are visible."""
                result = super().write(value)
                if self.getvalue().count("running") >= 2:
                    ready.set()
                return result

        stream = Output()
        reporter = ProgressReporter(
            progress=Progress(),
            concurrency=2,
            interval=0.001,
            export=True,
            stream=stream,
        )
        try:
            async with reporter:
                await asyncio.wait_for(ready.wait(), timeout=2)
                if error is not None:
                    raise error
        except (OSError, asyncio.CancelledError) as exc:
            assert exc is error
        assert f" {state} " in stream.getvalue().splitlines()[-1]
        assert reporter.task.done()

    asyncio.run(exercise())


@pytest.mark.parametrize(
    "changes,expected",
    [
        ({}, 60.0),
        ({"source_skipped_bytes": 500}, 20.0),
        ({"transferred_bytes": 999999999}, 60.0),
        ({"source_copied_bytes": 0}, None),
        ({"estimated_source_bytes": None}, None),
        ({"estimated_total": None}, None),
        ({"source_size_missing": True}, None),
        ({"failed": 1}, None),
        ({"discovered": 11}, None),
        ({"source_observed_bytes": 1001}, None),
    ],
)
def test_eta_uses_source_weights_and_excludes_skips_from_speed(changes, expected):
    """S3 compression and skipped bytes cannot inflate source throughput."""
    progress = Progress(
        estimated_total=10,
        estimated_source_bytes=1000,
        discovered=5,
        completed=2,
        source_observed_bytes=500,
        source_copied_bytes=250,
    )
    for name, value in changes.items():
        setattr(progress, name, value)
    assert estimate_eta(progress=progress, elapsed=20, state="running") == expected


def test_eta_handles_one_large_active_table_and_uneven_node_sizes():
    """A partial table provides throughput without extrapolating huge-node latency."""
    progress = Progress(
        estimated_total=1000,
        estimated_source_bytes=1000,
        discovered=1,
        source_observed_bytes=1000,
        source_copied_bytes=250,
        source_partial_nodes=0.25,
    )
    assert estimate_eta(progress=progress, elapsed=20, state="running") == 60


@pytest.mark.parametrize("remaining_bytes", [0, 100])
def test_eta_for_skips_requires_no_remaining_transfer_work(remaining_bytes):
    """Existing copies provide node throughput, but no data transfer sample."""
    progress = Progress(
        estimated_total=10,
        estimated_source_bytes=500 + remaining_bytes,
        discovered=5,
        unchanged=5,
        source_observed_bytes=500,
        source_skipped_bytes=500,
    )
    expected = None if remaining_bytes else 20
    assert estimate_eta(progress=progress, elapsed=20, state="running") == expected


def test_eta_uses_completed_discovery_to_refine_remaining_empty_nodes():
    """Completed discovery replaces an overestimated count without guessing bytes."""
    progress = Progress(
        estimated_total=100,
        estimated_source_bytes=0,
        discovered=10,
        completed=5,
        discovery_complete=True,
    )
    assert estimate_eta(progress=progress, elapsed=20, state="running") == 20


@pytest.mark.parametrize(
    "state,expected", [("finished", 0), ("failed", None), ("cancelled", None)]
)
def test_eta_terminal_states_do_not_extrapolate_stale_estimates(state, expected):
    """Finishing is known even without source statistics; interruption is not."""
    assert estimate_eta(progress=Progress(), elapsed=30, state=state) == expected


def test_eta_waits_for_warmup_and_does_not_promise_completion_before_publication():
    """No initial or zero-second running estimate before manifest publication."""
    progress = Progress(
        estimated_total=1,
        estimated_source_bytes=100,
        discovered=1,
        source_observed_bytes=100,
        source_copied_bytes=50,
        source_partial_nodes=0.5,
    )
    assert estimate_eta(progress=progress, elapsed=9, state="running") is None
    assert estimate_eta(progress=progress, elapsed=10, state="running") == 10
    progress.source_copied_bytes = 100
    progress.source_partial_nodes = 1
    assert estimate_eta(progress=progress, elapsed=20, state="running") is None


@pytest.mark.parametrize(
    "seconds,expected",
    [
        (None, "?"),
        (0, "0s"),
        (0.1, "~1s"),
        (30, "~30s"),
        (61, "~1m01s"),
        (3661, "~1h01m"),
    ],
)
def test_eta_format_marks_estimates_and_keeps_completion_distinct(seconds, expected):
    """Unknown and exact completion differ from approximate running durations."""
    assert format_eta(seconds) == expected


def test_summary_separates_source_storage_from_s3_bytes():
    """Output labels prevent confusing replicated YT usage with compressed uploads."""
    progress = Progress(
        estimated_total=4,
        estimated_source_bytes=4096,
        discovered=3,
        completed=1,
        unchanged=1,
        source_observed_bytes=3072,
        source_copied_bytes=1024,
        source_skipped_bytes=1024,
        transferred_bytes=100,
    )
    value = format_progress(
        progress=progress, concurrency=2, elapsed=20, state="running", export=True
    )
    assert "source_size=~4.0 KiB" in value
    assert "source_processed=1.0 KiB source_skipped=1.0 KiB eta=~40s" in value
    assert "uploaded=100.0 B" in value
    restored = format_progress(
        progress=progress, concurrency=2, elapsed=20, state="running", export=False
    )
    assert "source_size=" not in restored
    assert "eta=" not in restored


def test_eta_ignores_sub_byte_rounding_when_only_metadata_work_remains():
    """Floating-point part weights must not hide remaining directory traversal."""
    progress = Progress(
        estimated_total=100,
        estimated_source_bytes=1000,
        discovered=10,
        completed=10,
        source_observed_bytes=1000,
        source_copied_bytes=1000 - 1e-9,
    )
    assert estimate_eta(progress=progress, elapsed=20, state="running") == 180
