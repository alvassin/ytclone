"""Parse archive endpoints and open the matching object-storage adapter."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from urllib.parse import unquote, urlsplit

import aiohttp
from aiohttp_s3_client import S3Client

from .format import parse_s3_url, path_components
from .interfaces import ObjectStorage
from .local import LocalStorage, resolve_local_archive
from .progress import Progress
from .s3 import S3Storage


@dataclass(frozen=True)
class StorageLocation:
    """A serializable archive location, resolved before handing it to workers."""

    kind: str
    bucket: str = ""
    root: str = ""
    prefix: str = ""


def parse_storage(address: str) -> StorageLocation:
    """Accept S3 URLs, local paths and absolute, hostless file URLs."""
    if not address or "\0" in address:
        raise ValueError("Archive path must not be empty or contain NUL")
    parsed = urlsplit(address)
    if parsed.scheme == "s3":
        bucket, prefix = parse_s3_url(address)
        return StorageLocation(kind="s3", bucket=bucket, prefix=prefix)
    if parsed.scheme == "file":
        if (
            not address.startswith("file:///")
            or parsed.netloc
            or "?" in address
            or "#" in address
        ):
            raise ValueError(
                "Expected file:///absolute/path without host, query or fragment"
            )
        address = unquote(parsed.path, errors="strict")
        if "\0" in address:
            raise ValueError("Archive path must not contain NUL")
    elif parsed.scheme:
        raise ValueError("Expected an S3 URL or a local archive path")
    return StorageLocation(kind="local", root=address)


def direction(*, source: str, destination: str) -> tuple[bool, str, StorageLocation]:
    """Require exactly one YT endpoint and determine the transfer direction."""
    export = source.startswith("//")
    if export == destination.startswith("//"):
        raise ValueError(
            "Specify one YT path and one S3 URL or local archive path; source comes first"
        )
    yt_path = source if export else destination
    path_components(yt_path)
    if any(c in yt_path for c in "\\[]{}<>&*@"):
        raise ValueError(
            "YT selectors, links and escaped/special node names are not supported"
        )
    return export, yt_path, parse_storage(destination if export else source)


async def resolve_storage(
    *, location: StorageLocation, export: bool
) -> StorageLocation:
    """Initialize a local archive or locate the root of a selected subtree."""
    if location.kind == "local":
        root, prefix = await resolve_local_archive(path=location.root, export=export)
        return replace(location, root=str(root), prefix=prefix)
    return location


@asynccontextmanager
async def open_storage(
    *,
    location: StorageLocation,
    endpoint: str,
    region: str,
    chunk_size: int,
    progress: Progress | None = None,
) -> AsyncIterator[ObjectStorage]:
    """Create network resources only for S3; local workers reuse the given root."""
    if location.kind == "local":
        yield LocalStorage(root=location.root, chunk_size=chunk_size, progress=progress)
        return
    if location.kind != "s3":
        raise ValueError(f"Unsupported archive storage: {location.kind}")
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=300)
    ) as session:
        client = S3Client(
            session=session,
            url=endpoint.rstrip("/") + "/" + location.bucket,
            region=region,
        )
        yield S3Storage(client=client, chunk_size=chunk_size, progress=progress)
