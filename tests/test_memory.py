"""Planning-budget behavior without allocating transfer buffers or contexts."""

import pytest

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


def test_snapshot_owners_add_headroom_without_multiplying_data_buffers():
    """More admitted nodes retain metadata clients while the data pool stays fixed."""
    one_owner = estimate_memory_mib(
        concurrency=4,
        snapshot_concurrency=1,
        chunk_size=32 * MIB,
        compression_level=3,
        export=True,
    )
    many_owners = estimate_memory_mib(
        concurrency=4,
        snapshot_concurrency=9,
        chunk_size=32 * MIB,
        compression_level=3,
        export=True,
    )
    many_streams = estimate_memory_mib(
        concurrency=12,
        snapshot_concurrency=1,
        chunk_size=32 * MIB,
        compression_level=3,
        export=True,
    )
    assert 0 < many_owners - one_owner < many_streams - one_owner
    assert many_owners - one_owner < 8 * 32


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
