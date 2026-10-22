"""Copy YTsaurus static tables, files, documents and directories to or from an S3 archive."""

import asyncio
import logging
import math
import os
import sys
import time
from contextlib import nullcontext, suppress
from pathlib import Path

import aiohttp
import aiomisc
import argclass
from aiohttp_s3_client import S3Client
from aiomisc.log import LogFormat

from .encryption import ArchiveEncryption
from .engine import CopyOptions, export_tree, restore_tree
from .format import MIB, parse_s3_url, path_components
from .logging import file_logging
from .memory import estimate_memory_mib
from .progress import Progress, ProgressReporter
from .s3 import S3Storage
from .yt import YtAdapter

log = logging.getLogger(__name__)
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S %z"


class Arguments(argclass.Parser):
    source: str = argclass.Argument(
        "source", help="Source //YT/path or s3://bucket/prefix"
    )
    destination: str = argclass.Argument(
        "destination", help="Destination in the other storage"
    )
    yt_proxy: str = argclass.Argument(env_var="YT_PROXY", required=True)
    no_proxy_discovery: bool = argclass.Argument(
        default=False, help="Use only the supplied YT proxy (for local Docker)"
    )
    endpoint: str = argclass.Argument(
        env_var="S3_ENDPOINT", default="https://s3.amazonaws.com"
    )
    region: str = argclass.Argument(env_var="AWS_DEFAULT_REGION", default="us-east-1")
    concurrency: int = argclass.Argument(
        default=4, help="Maximum concurrent node lifecycles"
    )
    max_streams: int = argclass.Argument(
        default=0,
        help="Shared export data workers per process; 0 uses concurrency",
    )
    queue_size: int = argclass.Argument(default=16, help="Maximum queued node paths")
    part_size_mib: int = argclass.Argument(
        default=256,
        help="Target uncompressed size per archive object; approximate for tables",
    )
    buffer_size_mib: int = argclass.Argument(
        default=32, help="S3 multipart buffer per transfer, 5..512 MiB"
    )
    compression_level: int = argclass.Argument(
        default=3, help="Zstandard level, -7..22"
    )
    table_format: str = argclass.Argument(
        default="yson",
        choices=("yson", "skiff"),
        help="Export table format; restore uses the archived format and schema",
    )
    compression: str = argclass.Argument(
        default="zstd",
        choices=("zstd",),
        help="Archive compression; tables use YT compression without local transcoding",
    )
    yt_zstd_passthrough: bool = argclass.Argument(
        default=False,
        help="Deprecated export alias: table Zstandard passthrough is always enabled",
    )
    encrypt: bool = argclass.Argument(
        default=False,
        help="Encrypt exported contents or unlock an encrypted archive on restore",
    )
    encryption_password_file: str | None = argclass.Argument(
        default=None,
        type=str,
        help="Read the encryption password from this file; otherwise use YTCLONE_ENCRYPTION_PASSWORD",
    )
    log_file: str | None = argclass.Argument(
        default=None,
        type=str,
        help="Append detailed logs to this file and show progress on stdout",
    )
    progress_interval: float = argclass.Argument(
        default=5.0,
        help="Seconds between console summaries with --log-file",
    )
    log_level: str = argclass.Argument(
        default="info", choices=("debug", "info", "warning", "error")
    )
    log_format: str = argclass.Argument(
        default=LogFormat.default(),
        choices=LogFormat.choices(),
        env_var="AIOMISC_LOG_FORMAT",
    )


def load_encryption(
    *, enabled: bool, password_file: str | None
) -> ArchiveEncryption | None:
    """Read an explicitly requested secret without exposing it in parser state."""
    if not enabled:
        if password_file is not None:
            raise ValueError("--encryption-password-file requires --encrypt")
        return None
    if password_file is not None:
        try:
            password = Path(password_file).read_bytes()
        except OSError:
            raise ValueError("Cannot read encryption password file") from None
        if password.endswith(b"\r\n"):
            password = password[:-2]
        elif password.endswith(b"\n"):
            password = password[:-1]
    else:
        value = os.environ.get("YTCLONE_ENCRYPTION_PASSWORD")
        if value is None:
            raise ValueError(
                "--encrypt requires --encryption-password-file or "
                "YTCLONE_ENCRYPTION_PASSWORD"
            )
        try:
            password = value.encode("utf-8")
        except UnicodeError:
            raise ValueError("Encryption password must be valid UTF-8") from None
    return ArchiveEncryption(password=password)


def direction(*, source: str, destination: str) -> tuple[bool, str, str, str]:
    """Validate paths and resolve direction, YT path, bucket and prefix."""
    export = source.startswith("//") and destination.startswith("s3://")
    restore = source.startswith("s3://") and destination.startswith("//")
    if not (export or restore):
        raise ValueError("Specify one YT path and one S3 URL; source comes first")
    yt_path = source if export else destination
    path_components(yt_path)
    if any(c in yt_path for c in "\\[]{}<>&*@"):
        raise ValueError(
            "YT selectors, links and escaped/special node names are not supported"
        )
    bucket, prefix = parse_s3_url(destination if export else source)
    return export, yt_path, bucket, prefix


async def run(*, arguments: Arguments, options: CopyOptions) -> int:
    """Create adapters, run the copy and return its exit code."""
    export, yt_path, bucket, prefix = direction(
        source=arguments.source, destination=arguments.destination
    )

    memory_budget = estimate_memory_mib(
        concurrency=options.stream_limit if export else options.concurrency,
        snapshot_concurrency=options.concurrency if export else 0,
        chunk_size=options.chunk_size,
        compression_level=options.compression_level,
        export=export,
    )
    log.info(
        "copy_started direction=%s concurrency=%d buffer_size_mib=%d "
        "memory_estimate_mib=%d",
        "yt-to-s3" if export else "s3-to-yt",
        options.concurrency,
        options.chunk_size // MIB,
        memory_budget,
    )
    log.info(
        "Memory estimate is a planning budget; large rows, metadata and "
        "allocator overhead can exceed it."
    )
    if export:
        log.info("export_parallelism max_streams=%d", options.stream_limit)
        log.info(
            "table_compression format=%s mode=yt-zstd-passthrough level=%d",
            options.table_format,
            options.compression_level,
        )

    progress = Progress() if arguments.log_file else None
    if progress is not None:
        print(
            f"Detailed log: {arguments.log_file}; "
            f"RAM estimate: {memory_budget} MiB (planning budget, not a limit)",
            flush=True,
        )
    reporter = (
        ProgressReporter(
            progress=progress,
            concurrency=options.concurrency,
            interval=arguments.progress_interval,
            export=export,
            stream=sys.stdout,
        )
        if progress is not None
        else nullcontext()
    )
    started = time.monotonic()

    async with reporter:
        yt = YtAdapter(
            proxy=arguments.yt_proxy,
            proxy_discovery=not arguments.no_proxy_discovery,
            progress=progress,
            table_format=options.table_format,
        )
        estimate_task = (
            asyncio.create_task(estimate_total(yt=yt, path=yt_path, progress=progress))
            if export and progress is not None
            else None
        )
        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(
                    total=None, sock_connect=30, sock_read=300
                )
            ) as session:
                client = S3Client(
                    session=session,
                    url=arguments.endpoint.rstrip("/") + "/" + bucket,
                    region=arguments.region,
                )
                storage = S3Storage(
                    client=client,
                    chunk_size=options.chunk_size,
                    progress=progress,
                )
                if export:
                    result = await export_tree(
                        yt=yt,
                        storage=storage,
                        source=yt_path,
                        prefix=prefix,
                        cluster=arguments.yt_proxy.rstrip("/"),
                        options=options,
                        progress=progress,
                    )
                else:
                    result = await restore_tree(
                        yt=yt,
                        storage=storage,
                        source_prefix=prefix,
                        destination=yt_path,
                        options=options,
                        progress=progress,
                    )
        finally:
            if estimate_task is not None:
                estimate_task.cancel()
                with suppress(asyncio.CancelledError):
                    await estimate_task
    log.info(
        "copy_finished completed=%d skipped=%d failed=%d seconds=%.3f",
        result.completed,
        result.skipped,
        result.failed,
        time.monotonic() - started,
    )
    return 1 if result.failed else 0


async def estimate_total(*, yt: YtAdapter, path: str, progress: Progress) -> None:
    """Fetch optional subtree counts and storage size alongside discovery."""
    try:
        async with yt.worker() as worker:
            estimate = await worker.estimate_source(path=path)
            progress.estimated_total = estimate.node_count
            progress.estimated_source_bytes = estimate.disk_size
        log.info(
            "source_estimate path=%s nodes=%s disk_space=%s",
            path,
            estimate.node_count,
            estimate.disk_size,
        )
    except Exception:
        log.warning("source_estimate_unavailable path=%s", path, exc_info=True)


def main() -> int:
    """Parse arguments and run the CLI with configured logging."""
    arguments = Arguments(description=__doc__)
    arguments.parse_args()
    try:
        if (
            not math.isfinite(arguments.progress_interval)
            or arguments.progress_interval <= 0
        ):
            raise ValueError("Progress interval must be positive and finite")
        export, _, _, _ = direction(
            source=arguments.source, destination=arguments.destination
        )
        if not export and arguments.max_streams != 0:
            raise ValueError("Maximum streams applies to export only")
        if not export and arguments.yt_zstd_passthrough:
            raise ValueError("YT Zstandard passthrough applies to export only")
        if not export and arguments.table_format != "yson":
            raise ValueError(
                "Table format applies to export only; restore uses the manifest"
            )
        options = CopyOptions(
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
    except ValueError as exc:
        arguments.create_parser().error(str(exc))

    # aiomisc's plain formatter ignores dates; stream supports timestamps.
    log_format = arguments.log_format
    if log_format == LogFormat.plain.name:
        log_format = LogFormat.stream.name

    # System log collectors supply their own timestamps.
    log_date_format = (
        None
        if log_format in (LogFormat.syslog.name, LogFormat.journald.name)
        else LOG_DATE_FORMAT
    )

    log_context = (
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
        with log_context:
            try:
                return aiomisc.run(
                    run(arguments=arguments, options=options),
                    pool_size=max(4, arguments.concurrency + 1),
                    log_level=arguments.log_level,
                    log_format=log_format,
                    log_date_format=log_date_format,
                    log_config=not bool(arguments.log_file),
                )
            except (KeyboardInterrupt, asyncio.CancelledError):
                log.warning("copy_cancelled")
                return 130
            except Exception:
                log.exception("copy_failed")
                return 1
    except OSError as exc:
        print(f"Cannot write log file {arguments.log_file}: {exc}", file=sys.stderr)
        return 1
