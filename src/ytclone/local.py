"""Portable local archives with atomic objects and confined, streaming file IO."""

import asyncio
import fcntl
import hashlib
import json
import logging
import os
import stat
from collections.abc import AsyncIterator, Iterator
from contextlib import nullcontext, suppress
from dataclasses import dataclass
from itertools import islice
from pathlib import Path
from uuid import uuid4

from .format import MIB, ObjectRef
from .interfaces import ObjectStorage
from .progress import Progress

log = logging.getLogger(__name__)
ARCHIVE_MARKER = "@archive.json"
ARCHIVE_FORMAT = {"format": "ytclone-local", "version": 1}
TEMP_PREFIX = ".@ytclone-tmp-"
DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW


class IntegrityError(ValueError):
    """An archive object differs from its completed manifest."""


async def _io(function, *args):
    """Finish in-flight filesystem work before cancellation closes its handles."""
    task = asyncio.create_task(asyncio.to_thread(function, *args))
    cancelled = False
    while True:
        try:
            result = await asyncio.shield(task)
            break
        except asyncio.CancelledError:
            cancelled = True
            if task.done():
                # Retrieve any worker exception before propagating cancellation.
                with suppress(BaseException):
                    task.result()
                raise
        except BaseException:
            if cancelled:
                raise asyncio.CancelledError() from None
            raise
    if cancelled:
        raise asyncio.CancelledError()
    return result


def _parts(key: str, *, prefix: bool = False) -> list[str]:
    if prefix:
        if not key:
            return []
        key = key.removesuffix("/")
    parts = key.split("/")
    if any(
        part in ("", ".", "..", ARCHIVE_MARKER)
        or "\\" in part
        or "\x00" in part
        or part.startswith(TEMP_PREFIX)
        for part in parts
    ):
        raise ValueError(f"Unsafe or reserved local archive key: {key!r}")
    return parts


def _exact_spelling(directory: int, name: str, info) -> bool:
    """Use canonical descriptor paths on macOS and cheap case probes on Linux."""
    if hasattr(fcntl, "F_GETPATH") and (
        stat.S_ISDIR(info.st_mode) or info.st_nlink == 1
    ):
        fd = os.open(
            name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory
        )
        try:
            raw = fcntl.fcntl(fd, fcntl.F_GETPATH, b"\0" * 1024)
            actual = os.fsdecode(raw.split(b"\0", 1)[0]).rsplit("/", 1)[-1]
            return actual == name
        finally:
            os.close(fd)
    if name.isascii():
        alternate = name.swapcase()
        if alternate == name:
            return True
        try:
            other = os.stat(alternate, dir_fd=directory, follow_symlinks=False)
        except FileNotFoundError:
            return True
        if (other.st_dev, other.st_ino) != (info.st_dev, info.st_ino):
            return True
    # Fallback for insensitive mounts and Unicode normalization. A fresh open is
    # essential: dup/scandir would share an active directory walk's seek offset.
    scan_fd = os.open(".", DIRECTORY_FLAGS, dir_fd=directory)
    try:
        with os.scandir(scan_fd) as entries:
            return any(entry.name == name for entry in entries)
    finally:
        os.close(scan_fd)


def _check_entry(directory: int, name: str, *, marker: bool = False):
    """Reject aliases on case-insensitive filesystems, symlinks and special files."""
    try:
        info = os.stat(name, dir_fd=directory, follow_symlinks=False)
    except FileNotFoundError:
        return None
    if stat.S_ISLNK(info.st_mode):
        raise ValueError(f"Symlinks are not allowed in local archives: {name!r}")
    if not (stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode)):
        raise ValueError(f"Special files are not allowed in local archives: {name!r}")
    if stat.S_ISREG(info.st_mode) and info.st_nlink != 1 and not marker:
        raise ValueError(
            f"Hard-linked files are not allowed in local archives: {name!r}"
        )
    if not _exact_spelling(directory, name, info):
        raise ValueError(f"Local filename collision: {name!r}")
    return info


def _read_marker(directory: int) -> bool:
    info = _check_entry(directory, ARCHIVE_MARKER, marker=True)
    if info is None:
        return False
    if not stat.S_ISREG(info.st_mode) or info.st_size > 4096:
        raise ValueError("Invalid local archive marker")
    fd = os.open(ARCHIVE_MARKER, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory)
    try:
        raw = os.read(fd, 4097)
        if len(raw) > 4096 or json.loads(raw) != ARCHIVE_FORMAT:
            raise ValueError("Unsupported local archive marker")
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("Invalid local archive marker") from error
    finally:
        os.close(fd)
    return True


def _open_root(root: Path) -> int:
    fd = os.open(root, DIRECTORY_FLAGS)
    try:
        if not _read_marker(fd):
            raise ValueError(f"Missing {ARCHIVE_MARKER} in local archive: {root}")
        return fd
    except BaseException:
        os.close(fd)
        raise


def _open_directory(parent: int, name: str, *, create: bool) -> int:
    info = _check_entry(parent, name)
    if info is None and create:
        try:
            os.mkdir(name, mode=0o700, dir_fd=parent)
            os.fsync(parent)
        except FileExistsError:
            pass
        _check_entry(parent, name)
    fd = os.open(name, DIRECTORY_FLAGS, dir_fd=parent)
    try:
        if _read_marker(fd):
            raise ValueError("Nested local archives are not supported")
        return fd
    except BaseException:
        os.close(fd)
        raise


def _parent(root: Path, parts: list[str], *, create: bool) -> int:
    fd = _open_root(root)
    try:
        for name in parts[:-1]:
            child = _open_directory(fd, name, create=create)
            os.close(fd)
            fd = child
        return fd
    except BaseException:
        os.close(fd)
        raise


def _write_all(fd: int, data) -> None:
    remaining = memoryview(data)
    while remaining:
        written = os.write(fd, remaining)
        if written <= 0:
            raise OSError("Local archive write made no progress")
        remaining = remaining[written:]


@dataclass
class _Writer:
    parent: int = -1
    fd: int = -1
    temporary: str = ""
    name: str = ""

    def open(self, root: Path, key: str) -> None:
        parts = _parts(key)
        self.parent = _parent(root, parts, create=True)
        self.name = parts[-1]
        info = _check_entry(self.parent, self.name)
        if info is not None and not stat.S_ISREG(info.st_mode):
            raise ValueError(f"Object destination is not a regular file: {key}")
        self.temporary = TEMP_PREFIX + uuid4().hex
        self.fd = os.open(
            self.temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
            dir_fd=self.parent,
        )

    def write(self, data) -> None:
        _write_all(self.fd, data)

    def sync(self) -> None:
        os.fsync(self.fd)

    def publish(self) -> None:
        # Serialize the name check and replacement across local workers, including
        # differently cased names that were both absent when streaming began.
        fcntl.flock(self.parent, fcntl.LOCK_EX)
        try:
            info = _check_entry(self.parent, self.name)
            if info is not None and not stat.S_ISREG(info.st_mode):
                raise ValueError(
                    f"Object destination is not a regular file: {self.name}"
                )
            os.replace(
                self.temporary,
                self.name,
                src_dir_fd=self.parent,
                dst_dir_fd=self.parent,
            )
            self.temporary = ""
            os.fsync(self.parent)
        finally:
            fcntl.flock(self.parent, fcntl.LOCK_UN)

    def close(self) -> None:
        try:
            if self.fd >= 0:
                os.close(self.fd)
                self.fd = -1
        finally:
            if self.parent >= 0:
                try:
                    if self.temporary:
                        with suppress(FileNotFoundError):
                            os.unlink(self.temporary, dir_fd=self.parent)
                finally:
                    os.close(self.parent)
                    self.parent = -1


@dataclass
class _Reader:
    fd: int = -1

    def open(self, root: Path, key: str) -> int:
        parts = _parts(key)
        parent = _parent(root, parts, create=False)
        try:
            info = _check_entry(parent, parts[-1])
            if info is not None and not stat.S_ISREG(info.st_mode):
                raise ValueError(f"Archive object is not a regular file: {key}")
            self.fd = os.open(
                parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent
            )
            info = os.fstat(self.fd)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise ValueError(
                    f"Archive object is not an independent regular file: {key}"
                )
            return info.st_size
        finally:
            os.close(parent)

    def read(self, size: int) -> bytes:
        return os.read(self.fd, size)

    def close(self) -> None:
        if self.fd >= 0:
            os.close(self.fd)
            self.fd = -1


def _marker_at(path: Path) -> bool:
    try:
        fd = os.open(path, DIRECTORY_FLAGS)
    except FileNotFoundError:
        return False
    except NotADirectoryError:
        return False
    try:
        return _read_marker(fd)
    finally:
        os.close(fd)


def _resolve(path: str | Path, export: bool) -> tuple[Path, str]:
    selected = Path(os.path.abspath(os.fspath(path)))
    if selected.is_symlink():
        raise ValueError(f"Symlink in local archive path: {selected}")
    roots = [
        candidate
        for candidate in (selected, *selected.parents)
        if _marker_at(candidate)
    ]
    if len(roots) > 1:
        raise ValueError("Nested local archives are not supported")
    if roots:
        logical_root = roots[0]
        root = logical_root.resolve(strict=True)
        if any(_marker_at(parent) for parent in root.parents):
            raise ValueError("Nested local archives are not supported")
        relative = selected.relative_to(logical_root)
        prefix = "" if relative == Path(".") else relative.as_posix()
        if export and prefix:
            raise ValueError(
                "Export destination must be the archive root, not a subtree"
            )
        if not export and prefix:
            parts = _parts(prefix)
            parent = _parent(root, parts + ["unused"], create=False)
            os.close(parent)
        return root, prefix
    if not export:
        raise ValueError(f"No {ARCHIVE_MARKER} found above local source: {selected}")

    # Resolve the caller's outer path (including standard /tmp and /var aliases).
    # Paths *inside* an archive are always opened component by component, no-follow.
    root = selected.parent.resolve(strict=False) / selected.name
    if any(_marker_at(parent) for parent in root.parents):
        raise ValueError("Export destination must be the archive root, not a subtree")
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    directory = os.open(root, DIRECTORY_FLAGS)
    temporary = ""
    try:
        if _read_marker(directory):
            return root, ""
        with os.scandir(directory) as entries:
            if next(entries, None) is not None:
                raise ValueError(
                    "Refusing to initialize a nonempty unrecognized archive directory"
                )
        temporary = TEMP_PREFIX + uuid4().hex
        fd = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
            dir_fd=directory,
        )
        try:
            _write_all(fd, (json.dumps(ARCHIVE_FORMAT, sort_keys=True) + "\n").encode())
            os.fsync(fd)
        finally:
            os.close(fd)
        # A link publishes without ever replacing an independently created marker.
        try:
            os.link(
                temporary, ARCHIVE_MARKER, src_dir_fd=directory, dst_dir_fd=directory
            )
        except FileExistsError:
            if not _read_marker(directory):
                raise ValueError("Archive initialization changed concurrently")
        os.unlink(temporary, dir_fd=directory)
        temporary = ""
        os.fsync(directory)
    finally:
        if temporary:
            with suppress(FileNotFoundError):
                os.unlink(temporary, dir_fd=directory)
        os.close(directory)
    return root, ""


async def resolve_local_archive(*, path: str | Path, export: bool) -> tuple[Path, str]:
    """Initialize an export root or find an existing root above a restore subtree."""
    return await _io(_resolve, path, export)


class LocalStorage(ObjectStorage):
    def __init__(
        self,
        *,
        root: str | Path,
        chunk_size: int = 32 * MIB,
        progress: Progress | None = None,
    ):
        if chunk_size <= 0:
            raise ValueError("Local chunk_size must be positive")
        self.root = Path(root)
        self.chunk_size = chunk_size
        self.progress = progress

    def _measure(self, operation: str = "local"):
        if self.progress is None:
            return nullcontext()
        return self.progress.measure(operation=operation)

    async def read_bytes(self, *, key: str) -> bytes | None:
        reader = _Reader()
        try:
            with self._measure():
                try:
                    await _io(reader.open, self.root, key)
                except FileNotFoundError:
                    return None
                data = bytearray()
                while chunk := await _io(reader.read, self.chunk_size):
                    data.extend(chunk)
                return bytes(data)
        finally:
            await _io(reader.close)

    async def put_bytes(self, *, key: str, data: bytes) -> ObjectRef:
        async def chunks():
            yield data

        return await self._upload(key=key, chunks=chunks(), record_transfer=False)

    async def upload(self, *, key: str, chunks: AsyncIterator[bytes]) -> ObjectRef:
        return await self._upload(key=key, chunks=chunks, record_transfer=True)

    async def _upload(
        self, *, key: str, chunks: AsyncIterator[bytes], record_transfer: bool
    ) -> ObjectRef:
        writer = _Writer()
        digest = hashlib.sha256()
        total = 0
        try:
            with self._measure():
                await _io(writer.open, self.root, key)
            async for chunk in chunks:
                view = memoryview(chunk)
                for offset in range(0, len(view), self.chunk_size):
                    block = view[offset : offset + self.chunk_size]
                    with self._measure():
                        await _io(writer.write, block)
                    digest.update(block)
                    total += len(block)
                    if self.progress is not None and record_transfer:
                        self.progress.transferred_bytes += len(block)
            with self._measure():
                await _io(writer.sync)
                await _io(writer.publish)
            return ObjectRef(key=key, size=total, sha256=digest.hexdigest())
        finally:
            await _io(writer.close)

    async def download(self, *, ref: ObjectRef) -> AsyncIterator[bytes]:
        reader = _Reader()
        digest = hashlib.sha256()
        total = 0
        try:
            with self._measure():
                size = await _io(reader.open, self.root, ref.key)
            if size != ref.size:
                raise IntegrityError(
                    f"Local object size differs from manifest: {ref.key}"
                )
            while True:
                with self._measure("local_read"):
                    chunk = await _io(reader.read, self.chunk_size)
                    if not chunk:
                        if total != ref.size or digest.hexdigest() != ref.sha256:
                            raise IntegrityError(
                                f"Local object checksum or size differs from manifest: {ref.key}"
                            )
                        return
                    total += len(chunk)
                    if total > ref.size:
                        raise IntegrityError(
                            f"Local object exceeds manifest size: {ref.key}"
                        )
                    digest.update(chunk)
                    if self.progress is not None:
                        self.progress.transferred_bytes += len(chunk)
                yield chunk
        finally:
            await _io(reader.close)

    def _delete(self, key: str) -> None:
        parts = _parts(key)
        try:
            parent = _parent(self.root, parts, create=False)
        except FileNotFoundError:
            return
        try:
            info = _check_entry(parent, parts[-1])
            if info is not None:
                if not stat.S_ISREG(info.st_mode):
                    raise ValueError(f"Archive object is not a regular file: {key}")
                with suppress(FileNotFoundError):
                    os.unlink(parts[-1], dir_fd=parent)
                os.fsync(parent)
        finally:
            os.close(parent)

    async def delete(self, *, key: str) -> None:
        with self._measure():
            await _io(self._delete, key)

    async def exists(self, *, ref: ObjectRef) -> bool:
        reader = _Reader()
        try:
            with self._measure():
                try:
                    return await _io(reader.open, self.root, ref.key) == ref.size
                except FileNotFoundError:
                    return False
        finally:
            await _io(reader.close)

    def _keys(self, prefix: str) -> Iterator[str]:
        # Narrow the walk to the prefix's parent, preserving string-prefix semantics.
        _parts(prefix, prefix=True)
        base, _, _ = prefix.rpartition("/")
        parts = _parts(base) if base else []
        try:
            directory = _parent(self.root, parts + ["unused"], create=False)
        except FileNotFoundError:
            return

        def walk(fd: int, path: str) -> Iterator[str]:
            with os.scandir(fd) as entries:
                for entry in entries:
                    if entry.name == ARCHIVE_MARKER or entry.name.startswith(
                        TEMP_PREFIX
                    ):
                        continue
                    key = f"{path}/{entry.name}" if path else entry.name
                    if not (key.startswith(prefix) or prefix.startswith(key + "/")):
                        continue
                    info = _check_entry(fd, entry.name)
                    if info is None:
                        continue  # A concurrent incomplete-object cleanup won.
                    if stat.S_ISDIR(info.st_mode):
                        child = _open_directory(fd, entry.name, create=False)
                        try:
                            yield from walk(child, key)
                        finally:
                            os.close(child)
                    elif key.startswith(prefix):
                        yield key

        try:
            yield from walk(directory, base)
        finally:
            os.close(directory)

    async def list_keys(self, *, prefix: str) -> AsyncIterator[str]:
        iterator = self._keys(prefix)
        try:
            while True:
                with self._measure():
                    batch = await _io(lambda: list(islice(iterator, 128)))
                if not batch:
                    return
                for key in batch:
                    yield key
        finally:
            await _io(iterator.close)
