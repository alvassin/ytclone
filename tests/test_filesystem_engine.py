"""Archive orchestration with real disk IO and transactional in-memory YT."""

import asyncio
from contextlib import asynccontextmanager

import pytest
from test_engine import MemoryYt, make_node, options

from ytclone.engine import export_tree, restore_tree
from ytclone.format import Manifest, node_prefix
from ytclone.local import LocalStorage, resolve_local_archive

SOURCE = "//home/fixtures"
DESTINATION = "//restored"


async def archive(*, path, export=True):
    root, prefix = await resolve_local_archive(path=path, export=export)
    return LocalStorage(root=root, chunk_size=3), prefix


async def export(*, yt, storage, source=SOURCE):
    return await export_tree(
        yt=yt,
        storage=storage,
        source=source,
        prefix="",
        cluster="local-test",
        options=options(concurrency=2, queue_size=2),
    )


async def restore(*, yt, path, destination=DESTINATION):
    storage, prefix = await archive(path=path, export=False)
    return await restore_tree(
        yt=yt,
        storage=storage,
        source_prefix=prefix,
        destination=destination,
        options=options(concurrency=2, queue_size=2),
    )


def node_directory(*, root, path=SOURCE):
    return root / node_prefix(prefix="", path=path)


def test_disk_round_trip_preserves_tree_metadata_and_skips_complete_nodes(tmp_path):
    async def scenario():
        nodes = [
            make_node(path=SOURCE, kind="map_node", size=0),
            make_node(path=SOURCE + "/table", kind="table", size=9),
            make_node(path=SOURCE + "/file", size=11),
            make_node(path=SOURCE + "/empty", kind="map_node", size=0),
            make_node(path=SOURCE + "/empty_table", kind="table", size=0),
            make_node(path=SOURCE + "/empty_file", size=0),
        ]
        yt = MemoryYt(nodes=nodes, events=[])
        root = tmp_path / "dump"
        storage, prefix = await archive(path=root)
        assert prefix == ""
        result = await export(yt=yt, storage=storage)
        assert (result.completed, result.skipped, result.failed) == (6, 0, 0)
        original_reads = list(yt.reads)
        result = await export(yt=yt, storage=storage)
        assert (result.completed, result.skipped, result.failed) == (0, 6, 0)
        assert yt.reads == original_reads

        result = await restore(yt=yt, path=node_directory(root=root))
        assert (result.completed, result.skipped, result.failed) == (6, 0, 0)
        assert set(yt.restored) == {
            DESTINATION + node.path.removeprefix(SOURCE) for node in nodes
        }
        for source in nodes:
            target = DESTINATION + source.path.removeprefix(SOURCE)
            restored, parts = yt.restored[target]
            assert restored.kind == source.kind
            assert restored.attributes == source.attributes
            assert restored.row_count == source.row_count
            assert restored.data_size == source.data_size
            size = source.row_count if source.kind == "table" else source.data_size
            assert b"".join(parts) == b"x" * size
        commits = [event for event in yt.events if event[0] == "restore_commit"]
        result = await restore(yt=yt, path=node_directory(root=root))
        assert (result.completed, result.skipped, result.failed) == (0, 6, 0)
        assert [event for event in yt.events if event[0] == "restore_commit"] == commits

    asyncio.run(scenario())


def test_changed_node_replaces_disk_parts_and_preserves_sibling(tmp_path):
    async def scenario():
        file_path = SOURCE + "/file"
        sibling_path = SOURCE + "/file-other"
        yt = MemoryYt(
            nodes=[
                make_node(path=SOURCE, kind="map_node", size=0),
                make_node(path=file_path, size=12),
                make_node(path=sibling_path, size=7),
            ],
            events=[],
        )
        root = tmp_path / "dump"
        storage, _ = await archive(path=root)
        assert (await export(yt=yt, storage=storage)).failed == 0
        assert (await restore(yt=yt, path=node_directory(root=root))).failed == 0
        manifest_path = node_directory(root=root, path=file_path) / "@manifest.json"
        old = Manifest.from_bytes(manifest_path.read_bytes())
        sibling = node_directory(root=root, path=sibling_path) / "@manifest.json"
        sibling_before = sibling.read_bytes()
        reads_before = list(yt.reads)

        yt.nodes[file_path] = make_node(path=file_path, size=2, revision=2)
        result = await export(yt=yt, storage=storage)
        assert (result.completed, result.skipped, result.failed) == (1, 2, 0)
        current = Manifest.from_bytes(manifest_path.read_bytes())
        assert len(current.parts) == 1
        assert current.fingerprint != old.fingerprint
        assert all(not (root / part.object.key).exists() for part in old.parts[1:])
        assert sibling.read_bytes() == sibling_before
        assert yt.reads[len(reads_before) :] == [(file_path, 0)]
        result = await restore(yt=yt, path=node_directory(root=root))
        assert (result.completed, result.skipped, result.failed) == (1, 2, 0)
        assert b"".join(yt.restored[DESTINATION + "/file"][1]) == b"xx"
        assert b"".join(yt.restored[DESTINATION + "/file-other"][1]) == b"x" * 7

    asyncio.run(scenario())


@pytest.mark.parametrize("failure", ["read", "snapshot_exit"])
def test_failed_disk_export_has_no_manifest_and_resumes(tmp_path, failure):
    async def scenario():
        yt = MemoryYt(nodes=[make_node(path=SOURCE, size=12)], events=[])
        root = tmp_path / "dump"
        storage, _ = await archive(path=root)
        if failure == "read":
            yt.read_failures[(SOURCE, 1)] = [OSError("source interrupted")]
        else:
            yt.fail_snapshot_exit.add(SOURCE)
        result = await export(yt=yt, storage=storage)
        assert (result.completed, result.failed) == (0, 1)
        assert not (node_directory(root=root) / "@manifest.json").exists()
        assert yt.active == 0

        result = await restore(yt=yt, path=node_directory(root=root))
        assert result.completed == 0 and result.failed >= 1
        assert DESTINATION not in yt.fingerprints
        yt.fail_snapshot_exit.clear()
        result = await export(yt=yt, storage=storage)
        assert (result.completed, result.failed) == (1, 0)
        result = await restore(yt=yt, path=node_directory(root=root))
        assert (result.completed, result.failed) == (1, 0)
        assert b"".join(yt.restored[DESTINATION][1]) == b"x" * 12

    asyncio.run(scenario())


def test_relocated_archive_restores_only_selected_subtree(tmp_path):
    async def scenario():
        selected = SOURCE + "/selected"
        yt = MemoryYt(
            nodes=[
                make_node(path=SOURCE, kind="map_node", size=0),
                make_node(path=selected, kind="map_node", size=0),
                make_node(path=selected + "/file", size=7),
                make_node(path=SOURCE + "/unselected", size=9),
            ],
            events=[],
        )
        old_root = tmp_path / "old dump"
        storage, _ = await archive(path=old_root)
        assert (await export(yt=yt, storage=storage)).failed == 0
        moved = tmp_path / "moved dump"
        old_root.rename(moved)
        path = node_directory(root=moved, path=selected)
        root, prefix = await resolve_local_archive(path=path, export=False)
        assert root == moved
        assert prefix == node_prefix(prefix="", path=selected)

        result = await restore(yt=yt, path=path)
        assert (result.completed, result.failed) == (2, 0)
        assert set(yt.restored) == {DESTINATION, DESTINATION + "/file"}
        assert b"".join(yt.restored[DESTINATION + "/file"][1]) == b"x" * 7
        result = await restore(
            yt=yt, path=path / "file", destination="//restored-single"
        )
        assert (result.completed, result.failed) == (1, 0)
        assert b"".join(yt.restored["//restored-single"][1]) == b"x" * 7

    asyncio.run(scenario())


def test_unordered_listing_restores_changed_leaf_once_with_stale_descendants(
    tmp_path, monkeypatch
):
    """A directory changed to a file can retain old children between its own keys."""

    async def scenario():
        path = SOURCE + "/item"
        yt = MemoryYt(
            nodes=[
                make_node(path=SOURCE, kind="map_node", size=0),
                make_node(path=path, kind="map_node", size=0),
                make_node(path=path + "/old", size=3),
            ],
            events=[],
        )
        root = tmp_path / "dump"
        storage, _ = await archive(path=root)
        assert (await export(yt=yt, storage=storage)).failed == 0
        yt.nodes[path] = make_node(path=path, size=7, revision=2)
        del yt.nodes[path + "/old"]
        assert (await export(yt=yt, storage=storage)).failed == 0

        item_prefix = node_prefix(prefix="", path=path)
        old_prefix = item_prefix + "/old/"
        original_listing = storage.list_keys
        listing_complete = asyncio.Event()

        async def interleaved_listing(*, prefix):
            keys = [key async for key in original_listing(prefix=prefix)]
            first = item_prefix + "/@attributes.yson"
            ordered = [key for key in keys if not key.startswith(item_prefix + "/")]
            ordered += [first]
            ordered += [key for key in keys if key.startswith(old_prefix)]
            ordered += [
                key
                for key in keys
                if key.startswith(item_prefix + "/")
                and not key.startswith(old_prefix)
                and key != first
            ]
            assert sorted(ordered) == sorted(keys)
            try:
                for key in ordered:
                    yield key
            finally:
                listing_complete.set()

        original_restore = yt.restore
        leaf_restores = []

        @asynccontextmanager
        async def held_restore(*, path, node, fingerprint):
            if path == DESTINATION + "/item":
                leaf_restores.append(path)
                assert len(leaf_restores) == 1, (
                    "Duplicate concurrent restore of one leaf"
                )
                await listing_complete.wait()
            async with original_restore(
                path=path, node=node, fingerprint=fingerprint
            ) as sink:
                yield sink

        monkeypatch.setattr(storage, "list_keys", interleaved_listing)
        monkeypatch.setattr(yt, "restore", held_restore)
        result = await restore_tree(
            yt=yt,
            storage=storage,
            source_prefix=node_prefix(prefix="", path=SOURCE),
            destination=DESTINATION,
            options=options(concurrency=2, queue_size=2),
        )
        assert (result.completed, result.skipped, result.failed) == (2, 0, 0)
        assert leaf_restores == [DESTINATION + "/item"]
        assert set(yt.restored) == {DESTINATION, DESTINATION + "/item"}
        assert b"".join(yt.restored[DESTINATION + "/item"][1]) == b"x" * 7

    asyncio.run(scenario())


@pytest.mark.parametrize("corrupt", ["data", "attributes"])
def test_corrupt_disk_object_preserves_existing_target_and_can_be_repaired(
    tmp_path, corrupt
):
    async def scenario():
        yt = MemoryYt(nodes=[make_node(path=SOURCE, size=12)], events=[])
        root = tmp_path / "dump"
        storage, _ = await archive(path=root)
        assert (await export(yt=yt, storage=storage)).failed == 0
        path = node_directory(root=root)
        assert (await restore(yt=yt, path=path)).failed == 0
        previous = yt.restored[DESTINATION]
        manifest = Manifest.from_bytes((path / "@manifest.json").read_bytes())
        ref = manifest.parts[1].object if corrupt == "data" else manifest.attributes
        object_path = root / ref.key
        original = object_path.read_bytes()
        object_path.write_bytes(bytes([original[0] ^ 255]) + original[1:])
        # A target already marked unchanged intentionally bypasses data reads.
        yt.fingerprints.clear()
        result = await restore(yt=yt, path=path)
        assert (result.completed, result.failed) == (0, 1)
        assert yt.restored[DESTINATION] == previous
        assert DESTINATION not in yt.fingerprints
        assert yt.aborted == ([DESTINATION] if corrupt == "data" else [])

        object_path.write_bytes(original)
        result = await restore(yt=yt, path=path)
        assert (result.completed, result.failed) == (1, 0)
        assert yt.restored[DESTINATION] == previous
        assert yt.fingerprints[DESTINATION] == manifest.fingerprint

    asyncio.run(scenario())
