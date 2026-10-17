"""Structural adapter contracts; the engine depends only on these interfaces."""

from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from typing import Protocol

from .format import DiscoveredNode, Node, ObjectRef, Range, SourceEstimate


class RetryableReadError(Exception):
    """A temporary source failure; retry the whole part in the same snapshot."""


class RetryableDownloadError(Exception):
    """A temporary archive read failure; the consumer must roll back before replay."""


class RetryableRestoreError(Exception):
    """A temporary restore failure whose destination transaction was rolled back."""


class SourceSnapshot(Protocol):
    node: Node

    def read_part(
        self, *, part: Range, compression_level: int, chunk_size: int
    ) -> AsyncIterator[bytes]:
        """Open a fresh stream; table ranges may run concurrently in one snapshot.

        All readers must be closed before snapshot exit. Temporary source failures
        raise RetryableReadError so a range can restart in that same snapshot.
        """
        ...


class RestoreSink(Protocol):
    async def write_part(self, *, chunks: AsyncIterator[bytes]) -> None:
        """Consume and restore one compressed data part completely."""
        ...


class YtBackend(Protocol):
    async def inspect_node(self, *, path: str) -> DiscoveredNode:
        """Resolve one node safely and read its lightweight metadata."""
        ...

    async def list_children(self, *, node: DiscoveredNode) -> list[DiscoveredNode]:
        """List a discovered directory by ID without following links."""
        ...

    async def estimate_source(self, *, path: str) -> SourceEstimate:
        """Estimate source nodes and disk usage without scanning the subtree."""
        ...

    def walk(self, *, path: str) -> AsyncIterator[DiscoveredNode]:
        """Yield source identities and change hints without following links."""
        ...

    def snapshot(self, *, path: str) -> AbstractAsyncContextManager[SourceSnapshot]:
        """Pin a source node while its metadata and data are read."""
        ...

    async def ensure_directory(self, *, path: str) -> None:
        """Create missing directories while preserving existing attributes."""
        ...

    async def matches(self, *, path: str, fingerprint: str) -> bool:
        """Check that a restored node still matches the saved fingerprint."""
        ...

    def restore(
        self, *, path: str, node: Node, fingerprint: str
    ) -> AbstractAsyncContextManager[RestoreSink]:
        """Restore atomically; signal RetryableRestoreError only after safe rollback."""
        ...


class YtWorkerFactory(Protocol):
    def worker(self) -> AbstractAsyncContextManager[YtBackend]:
        """Keep one YT client and thread alive for a worker's queued tasks."""
        ...


class ObjectStorage(Protocol):
    async def read_bytes(self, *, key: str) -> bytes | None:
        """Read an object, returning None when it is missing."""
        ...

    async def put_bytes(self, *, key: str, data: bytes) -> ObjectRef:
        """Upload bytes and return their size and checksum."""
        ...

    async def upload(self, *, key: str, chunks: AsyncIterator[bytes]) -> ObjectRef:
        """Stream bytes into an object and return its integrity metadata."""
        ...

    def download(self, *, ref: ObjectRef) -> AsyncIterator[bytes]:
        """Verify the stream; temporary failures raise RetryableDownloadError."""
        ...

    async def delete(self, *, key: str) -> None:
        """Delete one object, tolerating an already missing key."""
        ...

    async def exists(self, *, ref: ObjectRef) -> bool:
        """Check that an object exists with the recorded size."""
        ...

    def list_keys(self, *, prefix: str) -> AsyncIterator[str]:
        """Yield every object key under the requested prefix."""
        ...
