"""Generation publication and cleanup under failed and overlapping operations."""

import asyncio
import json
import logging

import pytest
from test_engine import (
    DESTINATION,
    SOURCE,
    MemoryYt,
    export,
    make_node,
    manifest_key,
    restore,
    setup,
)

from ytclone.engine import restore_node
from ytclone.format import Manifest


def run(scenario):
    """Bound barriers so a broken interleaving fails instead of hanging CI."""

    async def bounded():
        return await asyncio.wait_for(scenario(), timeout=5)

    return asyncio.run(bounded())


def current(storage):
    return Manifest.from_bytes(storage.objects[manifest_key()])


def generation_keys(manifest):
    return {manifest.attributes.key, *(part.object.key for part in manifest.parts)}


def source(*, revision, size, events, attribute_revision=None):
    return MemoryYt(
        nodes=[
            make_node(
                revision=revision,
                size=size,
                attribute_revision=revision
                if attribute_revision is None
                else attribute_revision,
            )
        ],
        events=events,
    )


@pytest.mark.parametrize("failure", ["upload", "snapshot_exit", "publication"])
def test_failed_update_keeps_previous_archive_restorable(failure):
    async def scenario():
        yt, storage, events = setup()
        await export(yt=yt, storage=storage)
        old = current(storage)
        previous = dict(storage.objects)
        changed = source(revision=2, size=8, events=events)

        if failure == "upload":

            async def fail_upload(**kwargs):
                raise OSError("upload interrupted")

            storage.upload = fail_upload
        elif failure == "snapshot_exit":
            changed.fail_snapshot_exit.add(SOURCE)
        else:

            async def fail_publish(**kwargs):
                raise OSError("publication refused")

            storage.put_conditional = fail_publish

        result = await export(yt=changed, storage=storage)

        assert (result.completed, result.failed) == (0, 1)
        assert all(storage.objects[key] == data for key, data in previous.items())
        restored = await restore(yt=yt, storage=storage)
        assert (restored.completed, restored.failed) == (1, 0)
        node, parts = yt.restored[DESTINATION]
        assert node.content_revision == old.source["content_revision"]
        assert b"".join(parts) == b"x" * old.data_size

    run(scenario)


def test_concurrent_writers_publish_once_without_deleting_the_winner():
    async def scenario():
        yt, storage, events = setup()
        await export(yt=yt, storage=storage)
        old = current(storage)
        publish = storage.put_conditional
        both_ready = asyncio.Event()
        attempts = []

        async def competing_publish(*, key, data, etag):
            attempts.append((Manifest.from_bytes(data), etag))
            if len(attempts) == 2:
                both_ready.set()
            await both_ready.wait()
            return await publish(key=key, data=data, etag=etag)

        storage.put_conditional = competing_publish
        events.clear()
        results = await asyncio.gather(
            export(yt=source(revision=2, size=8, events=events), storage=storage),
            export(yt=source(revision=3, size=4, events=events), storage=storage),
        )

        assert sorted((result.completed, result.failed) for result in results) == [
            (0, 1),
            (1, 0),
        ]
        assert len({etag for _, etag in attempts}) == 1
        winner = current(storage)
        assert winner.generation in {manifest.generation for manifest, _ in attempts}
        assert generation_keys(winner) <= storage.objects.keys()
        deleted = {key for operation, key in events if operation == "delete"}
        assert deleted == generation_keys(old)
        for manifest, _ in attempts:
            assert generation_keys(manifest) <= storage.objects.keys()
        restored = await restore(yt=yt, storage=storage)
        assert (restored.completed, restored.failed) == (1, 0)
        assert (
            yt.restored[DESTINATION][0].content_revision
            == winner.source["content_revision"]
        )

    run(scenario)


def test_delayed_cleanup_deletes_only_its_predecessor_after_another_publication():
    async def scenario():
        yt, storage, events = setup()
        await export(yt=yt, storage=storage)
        first = current(storage)
        delete = storage.delete
        cleanup_started = asyncio.Event()
        release_cleanup = asyncio.Event()

        async def delayed_delete(*, key):
            if key == first.attributes.key:
                cleanup_started.set()
                await release_cleanup.wait()
            await delete(key=key)

        storage.delete = delayed_delete
        events.clear()
        first_update = asyncio.create_task(
            export(yt=source(revision=2, size=8, events=events), storage=storage)
        )
        await cleanup_started.wait()
        second = current(storage)
        assert second.generation != first.generation
        result = await export(
            yt=source(revision=3, size=4, events=events), storage=storage
        )
        assert (result.completed, result.failed) == (1, 0)
        third = current(storage)
        assert third.generation not in (first.generation, second.generation)
        release_cleanup.set()
        result = await first_update

        assert (result.completed, result.failed) == (1, 0)
        assert current(storage) == third
        assert generation_keys(third) <= storage.objects.keys()
        deleted = {key for operation, key in events if operation == "delete"}
        assert deleted == generation_keys(first) | generation_keys(second)

    run(scenario)


def test_stale_metadata_refresh_cannot_resurrect_a_removed_generation():
    async def scenario():
        yt, storage, events = setup()
        await export(yt=yt, storage=storage)
        old = current(storage)
        publish = storage.put_conditional
        refresh_ready = asyncio.Event()
        release_refresh = asyncio.Event()

        async def delayed_refresh(*, key, data, etag):
            manifest = Manifest.from_bytes(data)
            if manifest.generation == old.generation:
                assert manifest.source["attribute_revision"] == 2
                refresh_ready.set()
                await release_refresh.wait()
            return await publish(key=key, data=data, etag=etag)

        storage.put_conditional = delayed_refresh
        refresher = source(revision=1, size=12, attribute_revision=2, events=events)
        refresh = asyncio.create_task(export(yt=refresher, storage=storage))
        await refresh_ready.wait()
        updated = await export(
            yt=source(revision=2, size=8, events=events), storage=storage
        )
        assert (updated.completed, updated.failed) == (1, 0)
        replacement = current(storage)
        assert not generation_keys(old) & storage.objects.keys()
        release_refresh.set()
        result = await refresh

        assert (result.completed, result.skipped, result.failed) == (0, 0, 1)
        assert current(storage) == replacement
        assert generation_keys(replacement) <= storage.objects.keys()
        assert refresher.reads == []

    run(scenario)


def test_lost_publication_response_preserves_both_generations():
    async def scenario():
        yt, storage, events = setup()
        await export(yt=yt, storage=storage)
        old = current(storage)
        publish = storage.put_conditional

        async def lose_response(*, key, data, etag):
            await publish(key=key, data=data, etag=etag)
            raise TimeoutError("publication response lost")

        storage.put_conditional = lose_response
        events.clear()
        result = await export(
            yt=source(revision=2, size=8, events=events), storage=storage
        )

        assert (result.completed, result.failed) == (0, 1)
        replacement = current(storage)
        assert replacement.generation != old.generation
        assert (
            generation_keys(old) | generation_keys(replacement)
            <= storage.objects.keys()
        )
        assert not any(operation == "delete" for operation, _ in events)
        restored = await restore(yt=yt, storage=storage)
        assert (restored.completed, restored.failed) == (1, 0)
        assert yt.restored[DESTINATION][0].content_revision == 2

    run(scenario)


def test_cleanup_failure_warns_without_failing_the_published_copy(caplog):
    async def scenario():
        yt, storage, events = setup()
        await export(yt=yt, storage=storage)
        old = current(storage)
        delete = storage.delete

        async def fail_one_delete(*, key):
            if key == old.attributes.key:
                raise OSError("cleanup unavailable")
            await delete(key=key)

        storage.delete = fail_one_delete
        result = await export(
            yt=source(revision=2, size=8, events=events), storage=storage
        )

        assert (result.completed, result.failed) == (1, 0)
        assert current(storage).generation != old.generation
        assert generation_keys(current(storage)) <= storage.objects.keys()
        assert old.attributes.key in storage.objects
        assert all(part.object.key not in storage.objects for part in old.parts)
        assert f"generation_cleanup_failed key={old.attributes.key}" in caplog.text
        restored = await restore(yt=yt, storage=storage)
        assert (restored.completed, restored.failed) == (1, 0)

    with caplog.at_level(logging.WARNING, logger="ytclone.engine"):
        run(scenario)


def test_restore_of_deleted_generation_aborts_without_switching_to_new_parts():
    async def scenario():
        yt, storage, events = setup()
        await export(yt=yt, storage=storage)
        old = current(storage)
        download = storage.download
        next_part = asyncio.Event()
        release_download = asyncio.Event()

        async def paused_download(*, ref):
            if ref.key == old.parts[1].object.key:
                next_part.set()
                await release_download.wait()
            async for chunk in download(ref=ref):
                yield chunk

        storage.download = paused_download
        previous_destination = (make_node(path=DESTINATION, revision=99), [b"prior"])
        yt.restored[DESTINATION] = previous_destination
        restoring = asyncio.create_task(
            restore_node(
                yt=yt,
                storage=storage,
                prefix=manifest_key().rsplit("/", 1)[0],
                destination=DESTINATION,
            )
        )
        await next_part.wait()
        result = await export(
            yt=source(revision=2, size=8, events=events), storage=storage
        )
        assert (result.completed, result.failed) == (1, 0)
        replacement = current(storage)
        release_download.set()
        with pytest.raises(KeyError, match=old.generation):
            await restoring

        assert yt.aborted == [DESTINATION]
        assert yt.restored[DESTINATION] == previous_destination
        assert set(storage.downloaded) <= generation_keys(old)
        assert not set(storage.downloaded) & generation_keys(replacement)
        assert generation_keys(replacement) <= storage.objects.keys()

    run(scenario)


@pytest.mark.parametrize("operation", [export, restore], ids=["export", "restore"])
def test_legacy_archive_is_rejected_without_mutation(operation, caplog):
    async def scenario():
        yt, storage, events = setup()
        await export(yt=yt, storage=storage)
        manifest = current(storage)
        generation_segment = "/@generations/" + manifest.generation
        raw = json.loads(storage.objects[manifest_key()])
        raw["format"] = "yt-s3-v1"
        del raw["generation"]
        raw["attributes"]["key"] = raw["attributes"]["key"].replace(
            generation_segment, ""
        )
        for part in raw["parts"]:
            part["object"]["key"] = part["object"]["key"].replace(
                generation_segment, ""
            )
        storage.objects = {
            key.replace(generation_segment, ""): data
            for key, data in storage.objects.items()
        }
        storage.objects[manifest_key()] = json.dumps(raw).encode()
        before = dict(storage.objects)
        events.clear()

        result = await operation(yt=yt, storage=storage)

        assert (result.completed, result.skipped, result.failed) == (0, 0, 1)
        assert storage.objects == before
        assert not any(
            operation in ("put", "upload", "delete") for operation, _ in events
        )
        assert yt.restored == {}
        assert "Unsupported archive format 'yt-s3-v1'; expected yt-s3-v2" in caplog.text

    with caplog.at_level(logging.ERROR, logger="ytclone.engine"):
        run(scenario)
