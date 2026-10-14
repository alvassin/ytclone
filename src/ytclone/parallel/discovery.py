"""Concurrent metadata discovery with one persistent YT client per scanner."""

import asyncio
import logging
from collections.abc import Awaitable, Callable

from ..format import DiscoveredNode
from ..yt import YtAdapter, YtWorker, path_components

log = logging.getLogger(__name__)


class DiscoveryError(RuntimeError):
    """Some directories could not be listed after the remaining tree was scanned."""

    def __init__(self, *, failed_directories: int) -> None:
        """Retain the number of failed listings for the final copy result."""
        self.failed_directories = failed_directories
        super().__init__(f"Failed to list {failed_directories} source directories")


async def discover(
    *,
    yt: YtAdapter,
    source: str,
    concurrency: int,
    emit: Callable[[DiscoveredNode], Awaitable[None]],
) -> None:
    """Emit the source tree while independent directory listings run concurrently."""
    if type(concurrency) is not int or concurrency < 1:
        raise ValueError("Discovery concurrency must be a positive integer")
    path_components(source)

    # A bounded recursive queue can deadlock when every scanner is producing.
    # This frontier contains metadata only; clients and active RPCs stay bounded.
    pending: asyncio.Queue[DiscoveredNode] = asyncio.Queue()
    seen: set[str] = set()
    failed_directories = 0

    async def enqueue(*, node: DiscoveredNode, parent: str | None = None) -> None:
        """Validate and emit a new node before scheduling its directory listing."""
        path_components(node.path)
        if parent is not None and node.path.rsplit("/", maxsplit=1)[0] != parent:
            raise ValueError(
                f"Listing returned a node outside its directory: {node.path}"
            )
        if node.path in seen:
            raise ValueError(f"Discovery returned a duplicate path: {node.path}")

        seen.add(node.path)
        await emit(node)
        if node.kind == "map_node":
            pending.put_nowait(node)

    async def scan(*, worker: YtWorker) -> None:
        """Drain directory tasks sequentially using this scanner's client."""
        nonlocal failed_directories

        while True:
            node = await pending.get()
            try:
                try:
                    children = await worker.list_children(node=node)
                except Exception:
                    failed_directories += 1
                    log.exception("directory_discovery_failed path=%s", node.path)
                    continue

                for child in children:
                    await enqueue(node=child, parent=node.path)
            finally:
                pending.task_done()

    async def extra_scanner() -> None:
        """Keep a dedicated YT client alive until discovery finishes or fails."""
        async with yt.worker() as worker:
            await scan(worker=worker)

    async with yt.worker() as first_worker:
        root = await first_worker.inspect_node(path=source)
        if root.path != source:
            raise ValueError("Inspection returned a different source path")

        await enqueue(node=root)
        if root.kind != "map_node":
            return

        scanners = [asyncio.create_task(scan(worker=first_worker))]
        scanners.extend(
            asyncio.create_task(extra_scanner()) for _ in range(concurrency - 1)
        )
        finished = asyncio.create_task(pending.join())

        try:
            done, _ = await asyncio.wait(
                fs=[finished, *scanners], return_when=asyncio.FIRST_COMPLETED
            )
            for scanner in scanners:
                if scanner in done:
                    scanner.result()
                    raise RuntimeError("Discovery scanner stopped unexpectedly")

            if failed_directories:
                raise DiscoveryError(failed_directories=failed_directories)
        finally:
            for task in [finished, *scanners]:
                task.cancel()
            await asyncio.gather(finished, *scanners, return_exceptions=True)
