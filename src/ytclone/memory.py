"""Planning estimates for copy memory; these do not impose an RSS limit."""

import zstandard

from .format import MIB

# Heuristic allowances for Python, metadata, SDK/HTTP buffers and allocations.
PROCESS_HEADROOM = 128 * MIB
WORKER_HEADROOM = 16 * MIB
BUDGET_STEP = 64 * MIB


def estimate_memory_mib(
    *,
    concurrency: int,
    chunk_size: int,
    compression_level: int,
    export: bool,
    snapshot_concurrency: int = 0,
) -> int:
    """Estimate a rounded RAM budget for the configured copy direction."""
    if export:
        parameters = zstandard.ZstdCompressionParameters.from_level(compression_level)
        codec_memory = parameters.estimated_compression_context_size()
        window = 1 << parameters.window_log
        # Current input, prefetch, old/new multipart payloads, assembly and slack.
        buffers = 6 * chunk_size
    else:
        # Restore uses the archive's window, not the CLI compression level.
        # Budget for the largest window produced by our supported levels (<=22).
        parameters = zstandard.ZstdCompressionParameters.from_level(22)
        codec_memory = zstandard.estimate_decompression_context_size()
        window = 1 << parameters.window_log
        # The previous input may still be referenced while fetching the next.
        buffers = 2 * chunk_size

    # CCtx/DCtx estimates exclude streaming window and input/output buffers.
    worker_memory = buffers + codec_memory + window + MIB + WORKER_HEADROOM
    total = PROCESS_HEADROOM + concurrency * worker_memory
    if export:
        # Snapshot owners retain metadata clients, without extra data buffers.
        total += snapshot_concurrency * WORKER_HEADROOM
    return ((total + BUDGET_STEP - 1) // BUDGET_STEP) * (BUDGET_STEP // MIB)
