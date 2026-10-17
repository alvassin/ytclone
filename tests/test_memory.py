"""Planning-budget behavior without allocating transfer buffers or contexts."""

import pytest

from ytclone.engine import CopyOptions
from ytclone.format import MIB
from ytclone.memory import estimate_memory_mib


@pytest.mark.parametrize("export", [False, True])
def test_memory_budget_is_positive_and_rounded_up_to_64_mib(export):
    """Reserve shared and transfer-buffer memory in whole planning increments."""
    value = estimate_memory_mib(
        concurrency=4,
        chunk_size=32 * MIB,
        compression_level=3,
        export=export,
    )
    buffer_count = 6 if export else 2
    assert isinstance(value, int)
    assert value % 64 == 0
    assert value >= 128 + 4 * buffer_count * 32


@pytest.mark.parametrize("export", [False, True])
def test_memory_budget_grows_with_concurrency_and_buffer_size(export):
    """Reflect independently growing worker counts and per-worker buffers."""
    by_workers = [
        estimate_memory_mib(
            concurrency=concurrency,
            chunk_size=32 * MIB,
            compression_level=3,
            export=export,
        )
        for concurrency in (1, 2, 4)
    ]
    by_buffers = [
        estimate_memory_mib(
            concurrency=4,
            chunk_size=buffer_size * MIB,
            compression_level=3,
            export=export,
        )
        for buffer_size in (5, 32, 128)
    ]
    assert by_workers[0] < by_workers[1] < by_workers[2]
    assert by_buffers[0] < by_buffers[1] < by_buffers[2]


def test_export_budget_includes_compression_level_cost():
    """Account for larger compressor contexts and windows at high levels."""
    low = estimate_memory_mib(
        concurrency=2, chunk_size=32 * MIB, compression_level=1, export=True
    )
    high = estimate_memory_mib(
        concurrency=2, chunk_size=32 * MIB, compression_level=22, export=True
    )
    assert high > low


def test_restore_budget_does_not_assume_cli_compression_level_matches_archive():
    """Allow the largest supported archive window regardless of CLI level."""
    estimates = {
        estimate_memory_mib(
            concurrency=2,
            chunk_size=32 * MIB,
            compression_level=level,
            export=False,
        )
        for level in (-7, 1, 3, 22)
    }
    assert len(estimates) == 1
    assert estimates.pop() >= 128 + 2 * (2 * 32 + 128)


def test_export_budget_accounts_for_idle_readers_without_full_transfer_buffers():
    """Multiple tables retain SDK clients after releasing their shared stream slots."""
    options = CopyOptions(concurrency=4, part_concurrency=3, max_streams=2)
    assert options.reader_limit == 8

    def budget(*, streams, readers=None, export=True):
        return estimate_memory_mib(
            concurrency=streams,
            reader_concurrency=readers,
            chunk_size=32 * MIB,
            compression_level=3,
            export=export,
        )

    active_only = budget(streams=options.stream_limit)
    with_idle = budget(streams=options.stream_limit, readers=options.reader_limit)
    all_active = budget(streams=options.reader_limit)
    assert active_only < with_idle < all_active
    assert budget(streams=options.stream_limit, readers=1) == active_only
    assert budget(streams=4, readers=options.reader_limit, export=False) == budget(
        streams=4, export=False
    )
