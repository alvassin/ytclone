"""Local archive contracts against a real temporary filesystem, without services."""

import asyncio
import errno
import hashlib
import json
import os
import shutil
import threading
from dataclasses import replace
from pathlib import Path

import pytest

from ytclone import local
from ytclone.format import ObjectRef
from ytclone.local import (
    ARCHIVE_FORMAT,
    ARCHIVE_MARKER,
    TEMP_PREFIX,
    IntegrityError,
    LocalStorage,
    resolve_local_archive,
)
from ytclone.progress import Progress


def run(coroutine):
    return asyncio.run(coroutine)


async def chunks(*items):
    for item in items:
        yield item


async def storage_at(path, *, chunk_size=3, progress=None):
    root, prefix = await resolve_local_archive(path=path, export=True)
    assert prefix == ""
    return LocalStorage(root=root, chunk_size=chunk_size, progress=progress)


async def content(storage, ref):
    return b"".join([chunk async for chunk in storage.download(ref=ref)])


def temporary_objects(root):
    return list(root.rglob(TEMP_PREFIX + "*"))


def test_streamed_round_trip_empty_objects_and_progress(tmp_path):
    async def scenario():
        progress = Progress()
        storage = await storage_at(tmp_path / "archive", progress=progress)
        key = "home/games/a%20b/@data/part-000000.bin.zst"
        payload = b"\x00\xffabcdef"
        ref = await storage.upload(
            key=key, chunks=chunks(b"", payload[:1], payload[1:])
        )
        assert ref == ObjectRef(
            key=key, size=len(payload), sha256=hashlib.sha256(payload).hexdigest()
        )
        assert await content(storage, ref) == payload
        assert await storage.read_bytes(key=key) == payload
        assert await storage.exists(ref=ref)
        assert not await storage.exists(ref=replace(ref, size=99))
        assert progress.transferred_bytes == 2 * len(payload)
        assert progress.timings["local"].count > 0
        assert progress.timings["local_read"].count > 0
        empty = await storage.put_bytes(key="home/empty", data=b"")
        assert await content(storage, empty) == b""
        assert not temporary_objects(storage.root)

    run(scenario())


def test_missing_read_exists_delete_and_prefix_listing(tmp_path):
    async def scenario():
        storage = await storage_at(tmp_path / "archive")
        ref = ObjectRef(key="absent/part", size=0, sha256="0" * 64)
        assert await storage.read_bytes(key=ref.key) is None
        assert not await storage.exists(ref=ref)
        await storage.delete(key=ref.key)
        assert [key async for key in storage.list_keys(prefix="absent/")] == []
        expected = ["tree/a/@manifest.json", "tree/ab/@attributes.yson", "other/data"]
        for key in expected:
            await storage.put_bytes(key=key, data=b"data")
        (storage.root / "tree" / (TEMP_PREFIX + "stale")).write_bytes(b"partial")
        assert sorted([key async for key in storage.list_keys(prefix="")]) == sorted(
            expected
        )
        assert sorted(
            [key async for key in storage.list_keys(prefix="tree/a")]
        ) == sorted(expected[:2])
        assert [key async for key in storage.list_keys(prefix="tree/a/")] == [
            expected[0]
        ]
        await storage.delete(key=expected[0])
        await storage.delete(key=expected[0])
        assert await storage.read_bytes(key=expected[0]) is None

    run(scenario())


@pytest.mark.parametrize("change", ["same-size", "truncate", "extend", "missing"])
def test_download_checks_corruption(tmp_path, change):
    async def scenario():
        storage = await storage_at(tmp_path / "archive")
        ref = await storage.put_bytes(key="node/data", data=b"payload")
        path = storage.root / ref.key
        if change == "same-size":
            path.write_bytes(b"damaged")
        elif change == "truncate":
            path.write_bytes(b"pay")
        elif change == "extend":
            path.write_bytes(b"payload!")
        else:
            path.unlink()
        with pytest.raises(
            FileNotFoundError if change == "missing" else IntegrityError
        ):
            await content(storage, ref)

    run(scenario())


def test_file_changed_during_download_is_checked_at_eof(tmp_path):
    async def scenario():
        storage = await storage_at(tmp_path / "archive", chunk_size=2)
        ref = await storage.put_bytes(key="node/data", data=b"123456")
        stream = storage.download(ref=ref)
        assert await anext(stream) == b"12"
        (storage.root / ref.key).write_bytes(b"12")
        with pytest.raises(IntegrityError):
            await anext(stream)

    run(scenario())


def test_failed_input_preserves_previous_object_and_removes_temporary(tmp_path):
    async def scenario():
        storage = await storage_at(tmp_path / "archive")
        old = await storage.put_bytes(key="node/data", data=b"original")

        async def failing():
            yield b"replacement"
            assert await storage.read_bytes(key=old.key) == b"original"
            raise RuntimeError("source failed")

        with pytest.raises(RuntimeError, match="source failed"):
            await storage.upload(key=old.key, chunks=failing())
        assert await content(storage, old) == b"original"
        assert not temporary_objects(storage.root)

    run(scenario())


@pytest.mark.parametrize("stage", ["write", "sync"])
def test_full_disk_never_publishes_partial_object(tmp_path, monkeypatch, stage):
    async def scenario():
        storage = await storage_at(tmp_path / "archive")
        old = await storage.put_bytes(key="node/data", data=b"original")

        def fail(*args):
            raise OSError(errno.ENOSPC, "test disk is full")

        monkeypatch.setattr(local._Writer, stage, fail)
        with pytest.raises(OSError, match="full"):
            await storage.upload(key=old.key, chunks=chunks(b"replacement"))
        assert await content(storage, old) == b"original"
        assert not temporary_objects(storage.root)

    run(scenario())


def test_cancellation_waits_for_active_io_before_closing_and_cleanup(
    tmp_path, monkeypatch
):
    entered = threading.Event()
    release = threading.Event()
    closed = []
    original_write = local._Writer.write
    original_close = local._Writer.close

    def delayed_write(writer, data):
        entered.set()
        assert release.wait(timeout=5)
        # Closing/reusing the descriptor during cancellation would fail here.
        original_write(writer, data)

    def tracked_close(writer):
        closed.append(writer.fd)
        original_close(writer)

    async def scenario():
        storage = await storage_at(tmp_path / "archive")
        old = await storage.put_bytes(key="node/data", data=b"original")
        monkeypatch.setattr(local._Writer, "write", delayed_write)
        monkeypatch.setattr(local._Writer, "close", tracked_close)
        task = asyncio.create_task(
            storage.upload(key=old.key, chunks=chunks(b"replacement"))
        )
        try:
            assert await asyncio.to_thread(entered.wait, 5)
            task.cancel()
            await asyncio.sleep(0.02)
            task.cancel()  # Repeated cancellation must also wait for the writer.
            await asyncio.sleep(0.02)
            assert not task.done()
            assert closed == []
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert closed
        assert await content(storage, old) == b"original"
        assert not temporary_objects(storage.root)

    run(scenario())


def test_cancelled_input_removes_new_incomplete_object(tmp_path):
    async def scenario():
        storage = await storage_at(tmp_path / "archive")
        written = asyncio.Event()

        async def waiting():
            yield b"partial"
            written.set()
            await asyncio.Event().wait()

        task = asyncio.create_task(storage.upload(key="node/new", chunks=waiting()))
        await written.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert await storage.read_bytes(key="node/new") is None
        assert not temporary_objects(storage.root)

    run(scenario())


@pytest.mark.parametrize(
    "key",
    [
        "",
        "/absolute",
        "../escape",
        "a/../escape",
        "a//b",
        "a/./b",
        "a\\b",
        "a\x00b",
        ARCHIVE_MARKER,
        "a/" + ARCHIVE_MARKER,
        TEMP_PREFIX + "reserved",
    ],
)
def test_unsafe_keys_are_rejected_by_all_object_operations(tmp_path, key):
    async def scenario():
        storage = await storage_at(tmp_path / "archive")
        ref = ObjectRef(key=key, size=0, sha256="0" * 64)
        for operation in (
            storage.read_bytes(key=key),
            storage.put_bytes(key=key, data=b"unsafe"),
            storage.delete(key=key),
            storage.exists(ref=ref),
            content(storage, ref),
        ):
            with pytest.raises(ValueError, match="Unsafe|reserved"):
                await operation

    run(scenario())


@pytest.mark.parametrize("target_kind", ["file", "directory"])
def test_symlinks_cannot_escape_archive_for_any_operation(tmp_path, target_kind):
    async def scenario():
        storage = await storage_at(tmp_path / "archive")
        outside = tmp_path / "outside"
        if target_kind == "directory":
            outside.mkdir()
            (outside / "secret").write_bytes(b"untouched")
            (storage.root / "link").symlink_to(outside, target_is_directory=True)
            key = "link/secret"
        else:
            outside.write_bytes(b"untouched")
            (storage.root / "link").symlink_to(outside)
            key = "link"
        ref = ObjectRef(key=key, size=9, sha256="0" * 64)
        for operation in (
            storage.read_bytes(key=key),
            storage.put_bytes(key=key, data=b"overwrite"),
            storage.delete(key=key),
            storage.exists(ref=ref),
            content(storage, ref),
        ):
            with pytest.raises(ValueError, match="Symlinks"):
                await operation
        with pytest.raises(ValueError, match="Symlinks"):
            _ = [key async for key in storage.list_keys(prefix="")]
        assert (
            outside / "secret" if target_kind == "directory" else outside
        ).read_bytes() == b"untouched"

    run(scenario())


def test_hardlinked_files_and_special_files_are_rejected(tmp_path):
    async def scenario():
        storage = await storage_at(tmp_path / "archive")
        outside = tmp_path / "outside"
        outside.write_bytes(b"secret")
        os.link(outside, storage.root / "hardlink")
        with pytest.raises(ValueError, match="Hard-linked"):
            await storage.read_bytes(key="hardlink")
        os.mkfifo(storage.root / "pipe")
        with pytest.raises(ValueError, match="Special files"):
            await storage.read_bytes(key="pipe")

    run(scenario())


def test_case_alias_fails_on_insensitive_filesystems(tmp_path):
    async def scenario():
        storage = await storage_at(tmp_path / "archive")
        await storage.put_bytes(key="Foo/data", data=b"original")
        aliases = (storage.root / "foo").exists()
        if aliases:
            with pytest.raises(ValueError, match="filename collision"):
                await storage.put_bytes(key="foo/data", data=b"replacement")
            with pytest.raises(ValueError, match="filename collision"):
                await storage.read_bytes(key="foo/data")
        else:
            await storage.put_bytes(key="foo/data", data=b"replacement")
            assert await storage.read_bytes(key="foo/data") == b"replacement"
        assert await storage.read_bytes(key="Foo/data") == b"original"

    run(scenario())


def test_long_filename_fails_without_truncation_or_temp_leaks(tmp_path):
    async def scenario():
        storage = await storage_at(tmp_path / "archive")
        with pytest.raises(OSError) as error:
            await storage.put_bytes(key="node/" + "x" * 1024, data=b"payload")
        assert error.value.errno == errno.ENAMETOOLONG
        assert not temporary_objects(storage.root)
        assert [key async for key in storage.list_keys(prefix="")] == []

    run(scenario())


def test_archive_marker_and_subtree_survive_whole_archive_move(tmp_path):
    async def scenario():
        storage = await storage_at(tmp_path / "archive")
        ref = await storage.put_bytes(
            key="home/games/table/@attributes.yson", data=b"metadata"
        )
        assert json.loads((storage.root / ARCHIVE_MARKER).read_text()) == ARCHIVE_FORMAT
        assert str(tmp_path) not in (storage.root / ARCHIVE_MARKER).read_text()
        new_root = tmp_path / "moved"
        shutil.move(storage.root, new_root)
        root, prefix = await resolve_local_archive(
            path=new_root / "home/games/table", export=False
        )
        assert root == new_root.resolve()
        assert prefix == "home/games/table"
        assert await content(LocalStorage(root=root, chunk_size=2), ref) == b"metadata"
        assert await resolve_local_archive(path=new_root, export=True) == (root, "")

    run(scenario())


def test_restore_requires_marked_existing_directory(tmp_path):
    async def scenario():
        with pytest.raises(ValueError, match="No @archive"):
            await resolve_local_archive(path=tmp_path, export=False)
        storage = await storage_at(tmp_path / "archive")
        assert await resolve_local_archive(path=storage.root, export=False) == (
            storage.root,
            "",
        )
        with pytest.raises(FileNotFoundError):
            await resolve_local_archive(path=storage.root / "missing", export=False)
        await storage.put_bytes(key="object", data=b"payload")
        with pytest.raises(NotADirectoryError):
            await resolve_local_archive(path=storage.root / "object", export=False)

    run(scenario())


def test_initialization_rejects_unknown_nonempty_or_nested_directories(tmp_path):
    async def scenario():
        existing = tmp_path / "existing"
        existing.mkdir()
        (existing / "important").write_bytes(b"preserve")
        with pytest.raises(ValueError, match="nonempty"):
            await resolve_local_archive(path=existing, export=True)
        assert not (existing / ARCHIVE_MARKER).exists()
        assert (existing / "important").read_bytes() == b"preserve"
        storage = await storage_at(tmp_path / "archive")
        with pytest.raises(ValueError, match="archive root"):
            await resolve_local_archive(path=storage.root / "child", export=True)
        assert not (storage.root / "child").exists()
        child = storage.root / "nested"
        child.mkdir()
        (child / ARCHIVE_MARKER).write_text(json.dumps(ARCHIVE_FORMAT))
        with pytest.raises(ValueError, match="Nested"):
            await resolve_local_archive(path=child, export=False)
        with pytest.raises(ValueError, match="Nested"):
            _ = [key async for key in storage.list_keys(prefix="")]

    run(scenario())


@pytest.mark.parametrize(
    "marker",
    [b"garbage", b"[]", b'{"format":"ytclone-local","version":99}', b"x" * 5000],
)
def test_invalid_archive_markers_are_not_overwritten(tmp_path, marker):
    async def scenario():
        (tmp_path / ARCHIVE_MARKER).write_bytes(marker)
        with pytest.raises(ValueError, match="archive marker"):
            await resolve_local_archive(path=tmp_path, export=True)
        assert (tmp_path / ARCHIVE_MARKER).read_bytes() == marker

    run(scenario())


def test_restore_rejects_symlink_subtree_but_supports_system_tmp_alias(tmp_path):
    async def scenario():
        storage = await storage_at(tmp_path / "archive")
        (storage.root / "real").mkdir()
        (storage.root / "link").symlink_to(
            storage.root / "real", target_is_directory=True
        )
        with pytest.raises(ValueError, match="Symlink"):
            await resolve_local_archive(path=storage.root / "link", export=False)
        with pytest.raises(ValueError, match="Symlink"):
            await resolve_local_archive(
                path=storage.root / "link/missing", export=False
            )
        # macOS exposes /tmp as an alias of /private/tmp; user paths remain usable.
        if Path("/tmp").is_symlink() and storage.root.is_relative_to(Path("/private")):
            alias = Path("/") / storage.root.relative_to("/private")
            root, prefix = await resolve_local_archive(path=alias, export=False)
            assert (root, prefix) == (storage.root, "")

    run(scenario())


def test_marker_link_crash_state_is_recoverable(tmp_path):
    async def scenario():
        storage = await storage_at(tmp_path / "archive")
        # Model a kill after marker publication and before temporary cleanup.
        os.link(storage.root / ARCHIVE_MARKER, storage.root / (TEMP_PREFIX + "marker"))
        assert await resolve_local_archive(path=storage.root, export=True) == (
            storage.root,
            "",
        )
        assert await resolve_local_archive(path=storage.root, export=False) == (
            storage.root,
            "",
        )
        ref = await storage.put_bytes(key="home/data", data=b"payload")
        assert await content(storage, ref) == b"payload"
        assert [key async for key in storage.list_keys(prefix="")] == [ref.key]

    run(scenario())


def test_outer_directory_alias_is_resolved_before_creating_archive(tmp_path):
    async def scenario():
        physical = tmp_path / "physical"
        physical.mkdir()
        alias = tmp_path / "alias"
        alias.symlink_to(physical, target_is_directory=True)
        root, prefix = await resolve_local_archive(path=alias / "archive", export=True)
        assert (root, prefix) == (physical / "archive", "")
        assert await resolve_local_archive(path=alias / "archive", export=False) == (
            root,
            "",
        )

    run(scenario())


def test_case_collision_check_fallback_preserves_listing_offsets(tmp_path, monkeypatch):
    monkeypatch.delattr(local.fcntl, "F_GETPATH", raising=False)

    async def scenario():
        storage = await storage_at(tmp_path / "archive")
        expected = {f"node/@data/part-{i:06d}.bin.zst" for i in range(8)}
        for key in expected:
            await storage.put_bytes(key=key, data=b"payload")
        assert {key async for key in storage.list_keys(prefix="node/")} == expected
        await storage.put_bytes(key="Foo", data=b"original")
        if (storage.root / "foo").exists():
            with pytest.raises(ValueError, match="filename collision"):
                await storage.put_bytes(key="foo", data=b"replacement")
        assert await storage.read_bytes(key="Foo") == b"original"

    run(scenario())


def test_alias_cannot_hide_existing_archive_ancestor(tmp_path):
    async def scenario():
        storage = await storage_at(tmp_path / "archive")
        alias = tmp_path / "alias"
        alias.symlink_to(storage.root, target_is_directory=True)
        with pytest.raises(ValueError, match="archive root"):
            await resolve_local_archive(path=alias / "new", export=True)
        assert not (storage.root / "new").exists()

    run(scenario())


def test_metadata_bytes_are_excluded_from_streaming_progress(tmp_path):
    async def scenario():
        progress = Progress()
        storage = await storage_at(tmp_path / "archive", progress=progress)
        await storage.put_bytes(key="node/@manifest.json", data=b"metadata")
        assert progress.transferred_bytes == 0
        await storage.upload(key="node/@data/part", chunks=chunks(b"12345"))
        assert progress.transferred_bytes == 5

    run(scenario())


def test_concurrent_case_aliases_never_silently_overwrite(tmp_path):
    async def scenario():
        storage = await storage_at(tmp_path / "archive")
        # Probe this actual filesystem; Linux case-sensitive archives accept both.
        (storage.root / "Probe").write_bytes(b"")
        insensitive = (storage.root / "probe").exists()
        (storage.root / "Probe").unlink()
        arrived = 0
        ready = asyncio.Event()
        proceed = asyncio.Event()

        async def source(data):
            nonlocal arrived
            yield data
            arrived += 1
            if arrived == 2:
                ready.set()
            await proceed.wait()

        first = asyncio.create_task(storage.upload(key="Foo", chunks=source(b"first")))
        second = asyncio.create_task(
            storage.upload(key="foo", chunks=source(b"second"))
        )
        await ready.wait()
        proceed.set()
        results = await asyncio.gather(first, second, return_exceptions=True)
        if insensitive:
            errors = [item for item in results if isinstance(item, ValueError)]
            assert len(errors) == 1
            assert "filename collision" in str(errors[0])
            successes = [item for item in results if isinstance(item, ObjectRef)]
            assert len(successes) == 1
            expected = b"first" if successes[0].key == "Foo" else b"second"
            assert await content(storage, successes[0]) == expected
        else:
            assert all(isinstance(item, ObjectRef) for item in results)
            assert await storage.read_bytes(key="Foo") == b"first"
            assert await storage.read_bytes(key="foo") == b"second"
        assert not temporary_objects(storage.root)

    run(scenario())


@pytest.mark.parametrize("prefix", ["/", "/node/", "../", "node//", "node/../"])
def test_unsafe_listing_prefixes_are_rejected(tmp_path, prefix):
    async def scenario():
        storage = await storage_at(tmp_path / "archive")
        with pytest.raises(ValueError, match="Unsafe"):
            _ = [key async for key in storage.list_keys(prefix=prefix)]

    run(scenario())
