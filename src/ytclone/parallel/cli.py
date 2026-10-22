"""Copy a YT prefix to S3 with concurrent discovery and small-first processes."""

import asyncio
import logging
import math
import sys
from contextlib import nullcontext
from pathlib import Path

import aiomisc
import argclass
from aiomisc.log import LogFormat

from ..cli import LOG_DATE_FORMAT, direction, load_encryption
from ..cli import Arguments as CopyArguments
from ..engine import CopyOptions
from ..format import MIB
from ..logging import file_logging
from ..memory import estimate_memory_mib
from ..progress import Progress, ProgressReporter
from ..yt import YtAdapter
from .discovery import discover
from .supervisor import supervise

log = logging.getLogger(__name__)


class Arguments(CopyArguments):
    processes: int = argclass.Argument(default=2, help="Persistent copy processes")
    concurrency: int = argclass.Argument(
        default=4, help="Concurrent node lifecycles per copy process"
    )
    discovery_concurrency: int = argclass.Argument(
        default=4, help="Concurrent YT directory listings in the coordinator"
    )
    small_table_mib: int = argclass.Argument(
        default=64,
        help="Reserve one transfer slot for nodes up to this size while scanning",
    )
    worker_restarts: int = argclass.Argument(
        default=3, help="Maximum crash restarts per process, with exponential delay"
    )
    queue_size: int = argclass.Argument(
        default=16,
        help="Queued control messages per process; discovered metadata stays in RAM",
    )
    progress_interval: float = argclass.Argument(
        default=5.0, help="Seconds between aggregate console summaries"
    )


def validate(arguments: Arguments) -> CopyOptions:
    """Reject invalid settings before starting clients or child processes."""
    export, _, _, _ = direction(
        source=arguments.source, destination=arguments.destination
    )
    if not export:
        raise ValueError("Parallel copying currently supports YT to S3 only")
    if min(arguments.processes, arguments.discovery_concurrency) < 1:
        raise ValueError("Process and discovery counts must be positive")
    if min(arguments.worker_restarts, arguments.small_table_mib) < 0:
        raise ValueError("Restart count and small-node threshold must not be negative")
    if (
        not math.isfinite(arguments.progress_interval)
        or arguments.progress_interval <= 0
    ):
        raise ValueError("Progress interval must be positive and finite")
    if arguments.encryption_password_file is not None:
        arguments.encryption_password_file = str(
            Path(arguments.encryption_password_file).resolve()
        )
    return CopyOptions(
        concurrency=arguments.concurrency,
        max_streams=arguments.max_streams,
        queue_size=arguments.queue_size,
        part_size=arguments.part_size_mib * MIB,
        chunk_size=arguments.buffer_size_mib * MIB,
        compression_level=arguments.compression_level,
        table_format=arguments.table_format,
        compression=arguments.compression,
        encryption=load_encryption(
            enabled=arguments.encrypt,
            password_file=arguments.encryption_password_file,
        ),
    )


async def run(*, arguments: Arguments, options: CopyOptions) -> int:
    """Own discovery, aggregate progress and the bounded process supervisor."""
    _, source, bucket, prefix = direction(
        source=arguments.source, destination=arguments.destination
    )
    progress = Progress(source_size_missing=True)
    yt = YtAdapter(
        proxy=arguments.yt_proxy,
        proxy_discovery=not arguments.no_proxy_discovery,
        progress=progress,
    )
    config = {
        "yt_proxy": arguments.yt_proxy,
        "proxy_discovery": not arguments.no_proxy_discovery,
        "table_format": options.table_format,
        "compression": options.compression,
        "encrypt": arguments.encrypt,
        "encryption_password_file": arguments.encryption_password_file,
        "endpoint": arguments.endpoint,
        "region": arguments.region,
        "bucket": bucket,
        "prefix": prefix,
        "concurrency": options.concurrency,
        "max_streams": options.max_streams,
        "queue_size": options.queue_size,
        "part_size": options.part_size,
        "chunk_size": options.chunk_size,
        "compression_level": options.compression_level,
        "log_level": arguments.log_level,
        "log_format": arguments.log_format,
        "log_file": arguments.log_file,
        "progress_interval": arguments.progress_interval,
    }
    if options.encryption is not None:
        config["encryption_key_id"] = options.encryption.policy["key_id"]
    log.info(
        "table_compression format=%s mode=yt-zstd-passthrough level=%d",
        options.table_format,
        options.compression_level,
    )
    budget = arguments.processes * estimate_memory_mib(
        concurrency=options.stream_limit,
        snapshot_concurrency=options.concurrency,
        chunk_size=options.chunk_size,
        compression_level=options.compression_level,
        export=True,
    )
    print(
        f"Parallel export: {arguments.processes} processes x "
        f"{options.concurrency} node slots / {options.stream_limit} data workers; "
        f"discovery={arguments.discovery_concurrency}; small-first <= {arguments.small_table_mib} MiB. "
        f"RAM estimate: {budget} MiB plus discovery metadata (planning budget, not a limit)",
        flush=True,
    )
    if arguments.log_file:
        print(
            f"Detailed log: {arguments.log_file}; worker logs: <name>-worker-N.log",
            flush=True,
        )

    async def scan(*, emit):
        """Discover current source metadata using dedicated scanner clients."""
        await discover(
            yt=yt,
            source=source,
            concurrency=arguments.discovery_concurrency,
            emit=emit,
        )

    async with ProgressReporter(
        progress=progress,
        concurrency=arguments.processes * options.concurrency,
        interval=arguments.progress_interval,
        export=True,
        stream=sys.stdout,
    ):
        await supervise(
            discover=scan,
            config=config,
            processes=arguments.processes,
            small_size=arguments.small_table_mib * MIB,
            progress=progress,
            worker_restarts=arguments.worker_restarts,
        )
    return 1 if progress.failed else 0


def main() -> int:
    """Parse the public CLI and configure aiomisc logging and shutdown."""
    if sys.argv[1:] == ["--parallel-worker"]:
        from .worker import main as worker_main

        return worker_main()

    arguments = Arguments(description=__doc__)
    arguments.parse_args()
    try:
        options = validate(arguments)
    except ValueError as error:
        arguments.create_parser().error(str(error))

    log_format = arguments.log_format
    if log_format == LogFormat.plain.name:
        log_format = LogFormat.stream.name
    context = (
        file_logging(
            filename=arguments.log_file,
            level=arguments.log_level,
            log_format=log_format,
            date_format=LOG_DATE_FORMAT,
        )
        if arguments.log_file
        else nullcontext()
    )
    try:
        with context:
            return aiomisc.run(
                coro=run(arguments=arguments, options=options),
                pool_size=max(4, arguments.discovery_concurrency + 1),
                log_level=arguments.log_level,
                log_format=log_format,
                log_date_format=LOG_DATE_FORMAT,
                log_config=not bool(arguments.log_file),
            )
    except (KeyboardInterrupt, asyncio.CancelledError):
        log.warning("parallel_copy_cancelled")
        return 130
    except Exception:
        log.exception("parallel_copy_failed")
        return 1
