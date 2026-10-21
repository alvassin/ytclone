"""Direct exports must choose their publication guard before pinning a snapshot."""

import asyncio
from contextlib import asynccontextmanager

import pytest
from test_engine import (
    PREFIX,
    SOURCE,
    MemoryYt,
    export,
    make_node,
    manifest_key,
    options,
    setup,
)

from ytclone.engine import export_node
from ytclone.export_pool import PartPool
from ytclone.format import Manifest
from ytclone.interfaces import PublicationConflict
from ytclone.progress import Progress


@pytest.mark.parametrize("existing", [False, True], ids=["create", "replace"])
def test_direct_export_cannot_replace_a_newer_snapshot_published_after_it(existing):
    """A pinned revision must not adopt the ETag of a later publication."""

    async def scenario():
        initial, storage, events = setup()
        if existing:
            result = await export(yt=initial, storage=storage)
            assert (result.completed, result.failed) == (1, 0)
        older = MemoryYt(
            nodes=[make_node(revision=2, attribute_revision=2, size=8, disk_size=80)],
            events=events,
        )
        newer = MemoryYt(
            nodes=[make_node(revision=3, attribute_revision=3, size=4)], events=events
        )
        snapshot_ready = asyncio.Event()
        release_snapshot = asyncio.Event()
        snapshot = older.snapshot
        progress = Progress()

        @asynccontextmanager
        async def paused_snapshot(*, path):
            async with snapshot(path=path) as pinned:
                assert pinned.node.content_revision == 2
                snapshot_ready.set()
                await release_snapshot.wait()
                yield pinned

        older.snapshot = paused_snapshot

        async def direct_export():
            async with (
                older.worker() as backend,
                PartPool(yt=older, concurrency=1) as pool,
            ):
                return await export_node(
                    pool=pool,
                    yt=backend,
                    storage=storage,
                    path=SOURCE,
                    prefix=PREFIX,
                    cluster="local-test",
                    options=options(),
                    progress=progress,
                )

        stale_export = asyncio.create_task(direct_export())
        try:
            await snapshot_ready.wait()
            result = await export(yt=newer, storage=storage)
            assert (result.completed, result.failed) == (1, 0)
            winner_bytes = storage.objects[manifest_key()]
            winner = Manifest.from_bytes(winner_bytes)
            assert winner.source["content_revision"] == 3
            winner_keys = {
                winner.attributes.key,
                *(part.object.key for part in winner.parts),
            }
            event_boundary = len(events)
            release_snapshot.set()
            with pytest.raises(PublicationConflict):
                await stale_export

            assert storage.objects[manifest_key()] == winner_bytes
            assert winner_keys <= storage.objects.keys()
            assert not any(
                operation == "delete" or (operation == "put" and key == manifest_key())
                for operation, key in events[event_boundary:]
            )
            assert progress.completed_bytes == 0
            assert progress.source_copied_bytes == 0
        finally:
            release_snapshot.set()
            if not stale_export.done():
                stale_export.cancel()
            await asyncio.gather(stale_export, return_exceptions=True)

    asyncio.run(asyncio.wait_for(scenario(), timeout=5))
