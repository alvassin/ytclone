"""Persistent copy workers controlled through private JSON-line pipes."""

import asyncio
import logging
import math
import sys
from collections.abc import Iterator
from contextlib import AsyncExitStack, contextmanager, redirect_stdout, suppress

import aiohttp
import aiomisc
from aiochannel import Channel
from aiohttp_s3_client import S3Client
from aiomisc.log import LogFormat
from aiomisc_log import basic_config

from ..engine import CopyOptions, export_node
from ..export_pool import PartPool
from ..format import DiscoveredNode
from ..interfaces import ObjectStorage, YtBackend, YtWorkerFactory
from ..logging import file_logging
from ..progress import Progress
from ..s3 import S3Storage
from ..yt import YtAdapter
from .protocol import MAX_MESSAGE_SIZE, read_message, write_message

log = logging.getLogger(__name__)
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S %z"


class ParentDisconnected(Exception):
    """The coordinator closed its input before requesting a graceful stop."""


async def serve_jobs(
    *,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    yt: YtWorkerFactory,
    storage: ObjectStorage,
    prefix: str,
    cluster: str,
    options: CopyOptions,
    progress: Progress,
    progress_interval: float,
) -> None:
    """Copy assigned nodes with persistent clients; cancel work on parent EOF."""
    if not math.isfinite(progress_interval) or progress_interval <= 0:
        raise ValueError("Progress interval must be positive and finite")

    channel: Channel[tuple[int, DiscoveredNode]] = Channel(maxsize=options.concurrency)
    output_lock = asyncio.Lock()
    pending: set[int] = set()
    stopping = False

    async def send(*, message: dict) -> None:
        """Keep concurrent progress and result messages on separate lines."""
        async with output_lock:
            await write_message(writer=writer, message=message)

    async def report() -> None:
        """Publish cumulative byte counters for this process incarnation."""
        await send(
            message={
                "type": "progress",
                "uploaded": progress.transferred_bytes,
                "published": progress.completed_bytes,
            }
        )

    async def receive() -> None:
        """Validate assignments and close the local queue on an explicit stop."""
        while True:
            message = await read_message(reader=reader)
            if message is None:
                raise ParentDisconnected("Coordinator input closed")
            if message.get("type") == "stop":
                channel.close()
                return
            if message.get("type") != "job":
                raise ValueError("Expected a job or stop message")

            job_id = message.get("id")
            if type(job_id) is not int or job_id < 0 or job_id in pending:
                raise ValueError("Expected a unique nonnegative job ID")
            node = DiscoveredNode(**message["node"])
            pending.add(job_id)
            await channel.put((job_id, node))

    async def consume(*, backend: YtBackend) -> None:
        """Reuse one dedicated YT client for consecutive node assignments."""
        async for job_id, node in channel:
            pool.check()
            try:
                status, reason = await export_node(
                    yt=backend,
                    storage=storage,
                    path=node.path,
                    prefix=prefix,
                    cluster=cluster,
                    options=options,
                    progress=progress,
                    discovered=node,
                    pool=pool,
                )
            except Exception as error:
                if not stopping:
                    pool.check()
                status, reason = "failed", type(error).__name__
                log.exception("node_failed path=%s", node.path)
            else:
                log.info("node_%s path=%s reason=%s", status, node.path, reason)

            if stopping:
                return
            progress.record_result(status=status, reason=reason)
            pending.remove(job_id)
            await report()
            await send(
                message={
                    "type": "result",
                    "id": job_id,
                    "status": status,
                    "reason": reason,
                }
            )

    async def report_periodically() -> None:
        """Show byte progress while a large node is still being copied."""
        while True:
            await asyncio.sleep(progress_interval)
            await report()

    async with AsyncExitStack() as stack:
        backends = [
            await stack.enter_async_context(yt.worker())
            for _ in range(options.concurrency)
        ]
        pool = await stack.enter_async_context(
            PartPool(yt=yt, concurrency=options.stream_limit)
        )
        await send(message={"type": "ready"})
        tasks = [asyncio.create_task(consume(backend=backend)) for backend in backends]
        tasks.append(asyncio.create_task(receive()))
        reporter = asyncio.create_task(report_periodically())
        completion = asyncio.gather(*tasks)

        async def shutdown() -> None:
            """Cancel owners once, drain data, and close clients without interruption."""
            reporter.cancel()
            for task in tasks:
                if not task.done():
                    task.cancel()
            try:
                # Cancelled pool.copy calls retain snapshots until readers drain.
                await pool.close()
            finally:
                try:
                    await asyncio.gather(
                        completion, reporter, *tasks, return_exceptions=True
                    )
                finally:
                    await stack.aclose()

        try:
            done, _ = await asyncio.wait(
                fs=(completion, reporter), return_when=asyncio.FIRST_COMPLETED
            )
            if reporter in done:
                await reporter
            await asyncio.shield(completion)
        finally:
            stopping = True
            channel.close()
            shutdown_task = asyncio.create_task(shutdown())
            try:
                await asyncio.shield(shutdown_task)
            except asyncio.CancelledError:
                while not shutdown_task.done():
                    try:
                        await asyncio.shield(shutdown_task)
                    except asyncio.CancelledError:
                        pass
                shutdown_task.result()
                raise


async def run_worker(
    *, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, config: dict
) -> None:
    """Create process-local YT and S3 clients and serve node assignments."""
    options = CopyOptions(
        concurrency=config["concurrency"],
        max_streams=config["max_streams"],
        queue_size=config["concurrency"],
        part_size=config["part_size"],
        chunk_size=config["chunk_size"],
        compression_level=config["compression_level"],
        table_format=config.get("table_format", "yson"),
        compression=config.get("compression", "zstd"),
    )
    progress = Progress()
    yt = YtAdapter(
        proxy=config["yt_proxy"],
        proxy_discovery=config["proxy_discovery"],
        progress=progress,
        table_format=options.table_format,
    )

    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=300)
    ) as session:
        client = S3Client(
            session=session,
            url=config["endpoint"].rstrip("/") + "/" + config["bucket"],
            region=config["region"],
        )
        storage = S3Storage(
            client=client, chunk_size=options.chunk_size, progress=progress
        )
        await serve_jobs(
            reader=reader,
            writer=writer,
            yt=yt,
            storage=storage,
            prefix=config["prefix"],
            cluster=config["yt_proxy"].rstrip("/"),
            options=options,
            progress=progress,
            progress_interval=config["progress_interval"],
        )


@contextmanager
def worker_logging(*, config: dict) -> Iterator[None]:
    """Send all diagnostics to a file or stderr, keeping stdout for IPC."""
    with redirect_stdout(sys.stderr):
        if config.get("log_file"):
            with file_logging(
                filename=config["log_file"],
                level=config["log_level"],
                log_format=config["log_format"],
                date_format=LOG_DATE_FORMAT,
            ):
                yield
        else:
            log_format = config["log_format"]
            if log_format == LogFormat.plain.name:
                log_format = LogFormat.stream.name
            basic_config(
                level=config["log_level"],
                log_format=LogFormat[log_format],
                date_format=LOG_DATE_FORMAT,
            )
            yield


async def worker_main() -> int:
    """Connect standard pipes, read configuration and report fatal errors safely."""
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader(limit=MAX_MESSAGE_SIZE)
    read_transport, _ = await loop.connect_read_pipe(
        protocol_factory=lambda: asyncio.StreamReaderProtocol(stream_reader=reader),
        pipe=sys.stdin.buffer,
    )
    write_transport, protocol = await loop.connect_write_pipe(
        protocol_factory=lambda: asyncio.streams.FlowControlMixin(loop=loop),
        pipe=sys.stdout.buffer,
    )
    writer = asyncio.StreamWriter(
        transport=write_transport, protocol=protocol, reader=None, loop=loop
    )

    try:
        message = await read_message(reader=reader)
        if message is None:
            return 0
        if message.get("type") != "config" or not isinstance(
            message.get("config"), dict
        ):
            raise ValueError("Expected initial worker configuration")

        with worker_logging(config=message["config"]):
            await run_worker(reader=reader, writer=writer, config=message["config"])
        return 0
    except ParentDisconnected:
        return 1
    except Exception as error:
        log.exception("worker_failed error_type=%s", type(error).__name__)
        with suppress(BrokenPipeError, ConnectionError):
            await write_message(
                writer=writer,
                message={"type": "fatal", "error_type": type(error).__name__},
            )
        return 1
    finally:
        read_transport.close()
        writer.close()


def main() -> int:
    """Run the private worker with aiomisc signal handling and no stdout logger."""
    try:
        return aiomisc.run(coro=worker_main(), log_config=False)
    except (KeyboardInterrupt, asyncio.CancelledError):
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
