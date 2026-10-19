"""Bounded orchestration; adapters are injected and can be replaced by test doubles."""

import asyncio
import logging
import time
from contextlib import aclosing
from dataclasses import dataclass, replace
from uuid import uuid4

import aiomisc
from aiochannel import Channel
from yt import yson

from .export_pool import PartPool
from .format import (
    KINDS,
    MIB,
    DiscoveredNode,
    Manifest,
    Node,
    ObjectRef,
    Part,
    Range,
    data_key,
    generation_prefix,
    make_fingerprint,
    matches_discovery,
    node_prefix,
    object_key,
    plan_parts,
    restore_path,
    validate_manifest_location,
    verify_object,
)
from .interfaces import (
    ObjectStorage,
    RetryableReadError,
    SourceSnapshot,
    YtBackend,
    YtWorkerFactory,
)
from .progress import Progress
from .restore_retry import retry as retry_restore

log = logging.getLogger(__name__)
PART_READ_ATTEMPTS = 3
PART_RETRY_PAUSE = 1


@dataclass(frozen=True)
class CopyOptions:
    concurrency: int = 4
    max_streams: int = 0
    queue_size: int = 16
    # Target uncompressed size per archive object; approximate for tables.
    part_size: int = 256 * MIB
    # S3 multipart buffer; does not split data into separate objects.
    chunk_size: int = 32 * MIB
    compression_level: int = 3

    def __post_init__(self):
        """Validate concurrency, buffer sizes and compression level."""
        if min(self.concurrency, self.queue_size, self.part_size) < 1:
            raise ValueError(
                "Concurrency, queue and archive part size must be positive"
            )
        if self.max_streams < 0:
            raise ValueError("Maximum streams must be nonnegative")
        if not 5 * MIB <= self.chunk_size <= 512 * MIB:
            raise ValueError("Multipart buffer must be between 5 and 512 MiB")
        if not -7 <= self.compression_level <= 22:
            raise ValueError("Zstandard level must be between -7 and 22")

    @property
    def stream_limit(self) -> int:
        """Use one process-wide reader budget independent of admitted nodes."""
        return self.max_streams or self.concurrency


@dataclass
class CopyResult:
    completed: int = 0
    skipped: int = 0
    failed: int = 0


async def _pipeline(
    *,
    yt: YtWorkerFactory,
    discover,
    transfer,
    options: CopyOptions,
    result: CopyResult,
    progress: Progress,
    pool: PartPool | None = None,
) -> CopyResult:
    """Run discovery and copy workers, each with its own persistent YT client."""
    channel: Channel[str | DiscoveredNode] = Channel(maxsize=options.queue_size)

    async def producer():
        """Discover nodes using a separate client and close the queue at EOF."""
        try:
            async with yt.worker() as backend:
                try:
                    await discover(channel=channel, yt=backend)
                except Exception:
                    result.failed += 1
                    progress.record_result(status="failed", reason="discovery")
                    log.exception("discovery_failed")
        finally:
            channel.close()

    async def worker():
        """Reuse one YT client while transferring and counting queued nodes."""
        async with yt.worker() as backend:
            async for item in channel:
                path = item.path if isinstance(item, DiscoveredNode) else item
                progress.queued = channel.qsize()
                progress.active += 1
                try:
                    status, reason = await transfer(item=item, yt=backend)
                    progress.record_result(status=status, reason=reason)
                    if status == "skipped":
                        result.skipped += 1
                        log.info("node_skipped path=%s reason=%s", path, reason)
                    else:
                        result.completed += 1
                        log.info("node_completed path=%s reason=%s", path, reason)
                except Exception:
                    if pool is not None:
                        pool.check()
                    result.failed += 1
                    progress.record_result(status="failed", reason="node")
                    log.exception("node_failed path=%s", path)
                finally:
                    progress.active -= 1

    tasks = [asyncio.create_task(worker()) for _ in range(options.concurrency)]
    tasks.append(asyncio.create_task(producer()))

    completion = asyncio.gather(*tasks)

    async def shutdown():
        """Cancel owners once; their range waits retain snapshots until drain."""
        channel.close()
        for task in tasks:
            if not task.done():
                task.cancel()
        try:
            if pool is not None:
                await pool.close()
        finally:
            await asyncio.gather(completion, *tasks, return_exceptions=True)
            progress.queued = 0

    try:
        await asyncio.shield(completion)
    finally:
        cleanup = asyncio.create_task(shutdown())
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            while not cleanup.done():
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    pass
            cleanup.result()
            raise

    return result


@dataclass
class _SourceProgress:
    """Track one node's source weight, reverting unfinished work on failure."""

    progress: Progress
    disk_size: int = 0
    total: int = 0
    copied: float = 0
    fraction: float = 0
    skipped: int = 0
    completed_units: int = 0

    def __enter__(self):
        """Keep accounting alive through snapshot exit and publication."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Remove temporary node credit and roll back failed copies."""
        self.progress.source_partial_nodes = max(
            0, self.progress.source_partial_nodes - self.fraction
        )
        if exc_type is not None:
            self.progress.source_copied_bytes = max(
                0, self.progress.source_copied_bytes - self.copied
            )
            self.progress.source_skipped_bytes -= self.skipped

    def observe(self, *, node: Node | DiscoveredNode):
        """Count source weight once from the state used to copy or skip."""
        if node.disk_size is None:
            self.progress.source_size_missing = True
        else:
            self.disk_size = node.disk_size
            self.progress.source_observed_bytes += node.disk_size
        if isinstance(node, Node):
            self.total = node.row_count if node.kind == "table" else node.data_size

    def advance(self, *, part: Range):
        """Credit an uploaded part by its share of source rows or file bytes."""
        if not self.total:
            return
        self.completed_units += part.stop - part.start
        fraction = min(1, self.completed_units / self.total)
        copied = self.disk_size * fraction
        self.progress.source_copied_bytes += copied - self.copied
        self.progress.source_partial_nodes += fraction - self.fraction
        self.copied = copied
        self.fraction = fraction

    def complete(self):
        """Credit any remaining source weight after publishing the manifest."""
        self.progress.source_copied_bytes += self.disk_size - self.copied
        self.copied = self.disk_size

    def skip(self):
        """Exclude skipped source bytes from remaining work, not copy speed."""
        self.skipped = self.disk_size
        self.progress.source_skipped_bytes += self.skipped


async def _export_manifest(
    *, storage: ObjectStorage, prefix: str
) -> tuple[Manifest | None, str | None]:
    """Read the manifest and its publication guard from the same object version."""
    stored = await storage.read_versioned(
        key=object_key(prefix=prefix, name="@manifest.json")
    )
    if stored is None:
        return None, None
    manifest = Manifest.from_bytes(stored.data)
    validate_manifest_location(manifest=manifest, prefix=prefix)
    return manifest, stored.etag


async def _complete(*, storage: ObjectStorage, manifest: Manifest) -> bool:
    """Check each referenced object once before accepting an unchanged archive."""
    for ref in (manifest.attributes, *(part.object for part in manifest.parts)):
        if not await storage.exists(ref=ref):
            return False
    return True


def _source_metadata(*, node: Node, cluster: str) -> dict:
    """Record source identity and revisions from the snapshot actually read."""
    source = {
        "cluster": cluster,
        "path": node.path,
        "node_id": node.node_id,
        "content_revision": node.content_revision,
    }
    if node.attribute_revision is not None:
        source["attribute_revision"] = node.attribute_revision
    return source


async def copy_part(
    *,
    reader: YtBackend,
    snapshot: SourceSnapshot,
    storage: ObjectStorage,
    key: str,
    part: Range,
    compression_level: int,
    chunk_size: int,
) -> ObjectRef:
    """Retry a complete part after temporary source failures in the same snapshot."""
    attempt = 0

    @aiomisc.asyncbackoff(
        attempt_timeout=None,
        deadline=None,
        pause=PART_RETRY_PAUSE,
        max_tries=PART_READ_ATTEMPTS,
        exceptions=(RetryableReadError,),
        # aiomisc also catches TimeoutError; S3 owns its request retries.
        giveup=lambda error: not isinstance(error, RetryableReadError),
    )
    async def transfer() -> ObjectRef:
        """Open a fresh reader and compressor before each upload attempt."""
        nonlocal attempt
        attempt += 1

        try:
            async with aclosing(
                reader.read_part(
                    snapshot=snapshot,
                    part=part,
                    compression_level=compression_level,
                    chunk_size=chunk_size,
                )
            ) as chunks:
                return await storage.upload(key=key, chunks=chunks)
        except RetryableReadError as error:
            log.warning(
                "part_read_failed path=%s part=%d attempt=%d/%d error=%s",
                snapshot.node.path,
                part.index,
                attempt,
                PART_READ_ATTEMPTS,
                error,
            )
            raise

    return await transfer()


async def _export_parts(
    *,
    pool: PartPool,
    snapshot: SourceSnapshot,
    storage: ObjectStorage,
    target: str,
    options: CopyOptions,
    source_progress: _SourceProgress,
) -> list[Part]:
    """Register a node's lazy ranges with the shared data workers."""
    node = snapshot.node

    async def transfer(*, reader: YtBackend, part: Range) -> Part:
        ref = await copy_part(
            reader=reader,
            snapshot=snapshot,
            storage=storage,
            key=data_key(prefix=target, kind=node.kind, index=part.index),
            part=part,
            compression_level=options.compression_level,
            chunk_size=options.chunk_size,
        )
        source_progress.advance(part=part)
        log.info(
            "part_uploaded path=%s part=%d bytes=%d", node.path, part.index, ref.size
        )
        return Part(object=ref, start=part.start, stop=part.stop)

    return await pool.copy(
        parts=plan_parts(node=node, part_size=options.part_size),
        transfer=transfer,
        parallel=node.kind == "table",
    )


async def export_node(
    *,
    pool: PartPool,
    yt: YtBackend,
    storage: ObjectStorage,
    path: str,
    prefix: str,
    cluster: str,
    options: CopyOptions,
    progress: Progress | None = None,
    discovered: DiscoveredNode | None = None,
):
    """Skip a verified listing state or publish one consistent source snapshot."""
    pool.check()
    started = time.monotonic()
    target = node_prefix(prefix=prefix, path=path)
    manifest_key = object_key(prefix=target, name="@manifest.json")

    progress = progress if progress is not None else Progress()
    with _SourceProgress(progress=progress) as source_progress:
        old = None
        etag = None
        complete = None
        if discovered is not None:
            if discovered.kind not in KINDS:
                raise ValueError(f"Unsupported YT node type: {discovered.kind}")
            if discovered.dynamic is True:
                source_progress.observe(node=discovered)
                source_progress.skip()
                return "skipped", "dynamic"

            old, etag = await _export_manifest(storage=storage, prefix=target)
            if old is not None and matches_discovery(
                manifest=old,
                node=discovered,
                cluster=cluster,
                compression_level=options.compression_level,
                part_size=options.part_size,
            ):
                complete = await _complete(storage=storage, manifest=old)
                if complete:
                    source_progress.observe(node=discovered)
                    source_progress.skip()
                    return "skipped", "unchanged"

        unchanged = False
        async with yt.snapshot(path=path) as snapshot:
            node = snapshot.node
            source_progress.observe(node=node)
            if node.dynamic:
                source_progress.skip()
                return "skipped", "dynamic"
            if node.kind not in KINDS:
                raise ValueError(f"Unsupported YT node type: {node.kind}")
            fingerprint = make_fingerprint(
                node=node,
                cluster=cluster,
                compression_level=options.compression_level,
                part_size=options.part_size,
            )
            if discovered is None:
                old, etag = await _export_manifest(storage=storage, prefix=target)
            if old is not None and old.fingerprint == fingerprint:
                if complete is None:
                    complete = await _complete(storage=storage, manifest=old)
                unchanged = complete

            if not unchanged:
                pool.check()
                generation = uuid4().hex
                staged = generation_prefix(prefix=target, generation=generation)
                attributes = await storage.put_bytes(
                    key=object_key(prefix=staged, name="@attributes.yson"),
                    data=node.attributes,
                )
                parts = await _export_parts(
                    pool=pool,
                    snapshot=snapshot,
                    storage=storage,
                    target=staged,
                    options=options,
                    source_progress=source_progress,
                )

        # A refreshed revision is trustworthy only after successful snapshot exit.
        source = _source_metadata(node=node, cluster=cluster)
        if unchanged:
            if node.attribute_revision is not None and old.source != source:
                refreshed = replace(old, source=source)
                await storage.put_conditional(
                    key=manifest_key, data=refreshed.to_bytes(), etag=etag
                )
                log.info("manifest_refreshed path=%s", path)
            source_progress.skip()
            return "skipped", "unchanged"

        # Snapshot exit may fail; publication must happen strictly afterwards.
        manifest = Manifest(
            generation=generation,
            kind=node.kind,
            source=source,
            fingerprint=fingerprint,
            attributes=attributes,
            parts=tuple(parts),
            row_count=node.row_count,
            data_size=node.data_size,
        )
        # Failed/uncertain publication keeps both generations. Our request may
        # already have committed and been superseded before we see its response.
        await storage.put_conditional(
            key=manifest_key, data=manifest.to_bytes(), etag=etag
        )
        progress.completed_bytes += sum(part.object.size for part in parts)
        source_progress.complete()

    # Only our confirmed predecessor is obsolete. Other generations may still be
    # uploading, or may already have replaced ours. Cleanup cannot undo publication.
    if old is not None:
        for ref in (old.attributes, *(part.object for part in old.parts)):
            try:
                await storage.delete(key=ref.key)
            except Exception:
                log.warning("generation_cleanup_failed key=%s", ref.key, exc_info=True)
    log.info("node_timing path=%s seconds=%.3f", path, time.monotonic() - started)
    return "completed", "copied"


async def export_tree(
    *,
    yt: YtWorkerFactory,
    storage: ObjectStorage,
    source: str,
    prefix: str,
    cluster: str,
    options: CopyOptions,
    progress: Progress | None = None,
) -> CopyResult:
    """Walk a YT subtree and copy its nodes to an S3 prefix."""
    progress = progress if progress is not None else Progress()

    async def discover(*, channel: Channel[DiscoveredNode], yt: YtBackend):
        """Queue source identities and revisions from directory listings."""
        async with aclosing(yt.walk(path=source)) as nodes:
            async for node in nodes:
                progress.discovered += 1
                await channel.put(node)
                progress.queued = channel.qsize()
        progress.discovery_complete = True

    async def transfer(*, item: DiscoveredNode, yt: YtBackend):
        """Export or skip one queued source state."""
        return await export_node(
            pool=pool,
            yt=yt,
            storage=storage,
            path=item.path,
            prefix=prefix,
            cluster=cluster,
            options=options,
            progress=progress,
            discovered=item,
        )

    async with PartPool(yt=yt, concurrency=options.stream_limit) as pool:
        return await _pipeline(
            yt=yt,
            discover=discover,
            transfer=transfer,
            options=options,
            result=CopyResult(),
            progress=progress,
            pool=pool,
        )


async def _manifest(*, storage: ObjectStorage, prefix: str) -> Manifest | None:
    """Read and validate a node manifest, or return None if absent."""
    data = await storage.read_bytes(
        key=object_key(prefix=prefix, name="@manifest.json")
    )
    if data is None:
        return None
    manifest = Manifest.from_bytes(data)
    validate_manifest_location(manifest=manifest, prefix=prefix)
    return manifest


@retry_restore
async def _restore_data(
    *,
    yt: YtBackend,
    storage: ObjectStorage,
    destination: str,
    node: Node,
    manifest: Manifest,
) -> None:
    """Retry all parts in a fresh transaction after a confirmed rollback."""
    async with yt.restore(
        path=destination, node=node, fingerprint=manifest.fingerprint
    ) as sink:
        for index, part in enumerate(manifest.parts):
            async with aclosing(storage.download(ref=part.object)) as chunks:
                await sink.write_part(chunks=chunks)
            log.info(
                "part_restored path=%s part=%d bytes=%d",
                destination,
                index,
                part.object.size,
            )


async def restore_node(
    *,
    yt: YtBackend,
    storage: ObjectStorage,
    prefix: str,
    destination: str,
    progress: Progress | None = None,
):
    """Restore or skip one archive node, verifying data before commit."""
    manifest = await _manifest(storage=storage, prefix=prefix)
    if manifest is None:
        raise ValueError(f"Incomplete archive node (missing manifest): {prefix}")
    metadata = await storage.read_bytes(key=manifest.attributes.key)
    if metadata is None:
        raise ValueError(f"Missing attributes: {manifest.attributes.key}")
    verify_object(data=metadata, ref=manifest.attributes)
    if not isinstance(yson.loads(string=metadata, encoding=None), dict):
        raise TypeError("Attributes must be a YSON map")
    if await yt.matches(path=destination, fingerprint=manifest.fingerprint):
        return "skipped", "unchanged"

    node = Node(
        path=destination,
        kind=manifest.kind,
        node_id=manifest.source["node_id"],
        content_revision=manifest.source["content_revision"],
        attributes=metadata,
        row_count=manifest.row_count,
        data_size=manifest.data_size,
    )
    await _restore_data(
        yt=yt,
        storage=storage,
        destination=destination,
        node=node,
        manifest=manifest,
    )
    if progress is not None:
        progress.completed_bytes += sum(part.object.size for part in manifest.parts)
    return "completed", "copied"


def _node_from_key(*, key: str, source_prefix: str) -> str | None:
    """Find the archive node owning an S3 key within the selected prefix."""
    base = source_prefix.rstrip("/") + "/" if source_prefix else ""
    if not key.startswith(base):
        raise ValueError("Listing key escapes selected source prefix")
    components = key[len(base) :].split("/")
    for index, name in enumerate(components):
        if name == "@generations" and index + 1 < len(components):
            return (base + "/".join(components[:index])).rstrip("/")
        if name == "@data" and index + 1 < len(components):
            return (base + "/".join(components[:index])).rstrip("/")
        if (
            name in ("@attributes.yson", "@manifest.json")
            and index == len(components) - 1
        ):
            return (base + "/".join(components[:index])).rstrip("/")
    return None


class _ArchiveLeaf(Exception):
    """Stale descendants of a current table/file are outside its tree."""


async def restore_tree(
    *,
    yt: YtWorkerFactory,
    storage: ObjectStorage,
    source_prefix: str,
    destination: str,
    options: CopyOptions,
    progress: Progress | None = None,
) -> CopyResult:
    """Restore an S3 subtree, preparing directories before their children."""
    source_prefix = source_prefix.rstrip("/")
    result = CopyResult()
    progress = progress if progress is not None else Progress()
    directories: set[str] = set()
    leaves: set[str] = set()
    discovered: set[str] = set()

    def found(prefix: str) -> None:
        """Count source nodes once, including directories prepared before listing."""
        if prefix not in discovered:
            discovered.add(prefix)
            progress.discovered += 1

    async def transfer(*, item: str, yt: YtBackend):
        """Restore one archived node using the current worker's client."""
        target = restore_path(
            source_prefix=source_prefix, node=item, destination=destination
        )
        return await restore_node(
            yt=yt, storage=storage, prefix=item, destination=target, progress=progress
        )

    async def directory(*, prefix: str, yt: YtBackend):
        """Restore directory metadata or create a virtual parent once."""
        if prefix in leaves:
            raise _ArchiveLeaf(prefix)
        if prefix in directories:
            return
        path = restore_path(
            source_prefix=source_prefix, node=prefix, destination=destination
        )
        manifest = await _manifest(storage=storage, prefix=prefix)
        if manifest is not None:
            found(prefix)
            if manifest.kind != "map_node":
                leaves.add(prefix)
                raise _ArchiveLeaf(prefix)
            status, reason = await transfer(item=prefix, yt=yt)
            progress.record_result(status=status, reason=reason)
            if status == "skipped":
                result.skipped += 1
                log.info("node_skipped path=%s reason=%s", path, reason)
            else:
                result.completed += 1
                log.info("node_completed path=%s reason=%s", path, reason)
        else:
            async with aclosing(
                storage.list_keys(
                    prefix=object_key(prefix=prefix, name="@generations/")
                )
            ) as keys:
                async for _ in keys:
                    found(prefix)
                    raise ValueError(f"Incomplete archive directory: {prefix}")
            # A virtual parent is legitimate when only a descendant was exported.
            if (
                await storage.read_bytes(
                    key=object_key(prefix=prefix, name="@attributes.yson")
                )
                is not None
            ):
                found(prefix)
                raise ValueError(f"Incomplete archive directory: {prefix}")
            # Virtual parents have no source metadata to overwrite existing attrs.
            await yt.ensure_directory(path=path)
        directories.add(prefix)

    async def parents(*, prefix: str, yt: YtBackend):
        """Prepare each ancestor directory before restoring a descendant."""
        if prefix == source_prefix:
            return
        await directory(prefix=source_prefix, yt=yt)
        relative = prefix[len(source_prefix) + (1 if source_prefix else 0) :]
        current = source_prefix
        for component in relative.split("/")[:-1]:
            current = object_key(prefix=current, name=component)
            await directory(prefix=current, yt=yt)

    async def discover(*, channel: Channel[str], yt: YtBackend):
        """Scan S3 nodes, prepare directories and queue tables and files."""
        root = await _manifest(storage=storage, prefix=source_prefix)
        if root is not None and root.kind != "map_node":
            found(source_prefix)
            await channel.put(source_prefix)
            progress.queued = channel.qsize()
            return
        seen = False
        previous = None
        listing_prefix = source_prefix + "/" if source_prefix else ""
        async for key in storage.list_keys(prefix=listing_prefix):
            prefix = _node_from_key(key=key, source_prefix=source_prefix)
            if prefix is None or prefix == previous:
                continue
            previous = prefix
            seen = True
            try:
                await parents(prefix=prefix, yt=yt)
                found(prefix)
                manifest = await _manifest(storage=storage, prefix=prefix)
                if manifest is not None and manifest.kind == "map_node":
                    await directory(prefix=prefix, yt=yt)
                else:
                    if manifest is not None:
                        leaves.add(prefix)
                    await channel.put(prefix)
                    progress.queued = channel.qsize()
            except _ArchiveLeaf:
                log.debug("stale_archive_descendant_skipped prefix=%s", prefix)
            except Exception:
                found(prefix)
                result.failed += 1
                progress.record_result(status="failed", reason="archive")
                log.exception("archive_node_failed prefix=%s", prefix)
        if not seen:
            raise ValueError(f"No archive nodes found under {source_prefix}")

    return await _pipeline(
        yt=yt,
        discover=discover,
        transfer=transfer,
        options=options,
        result=result,
        progress=progress,
    )
