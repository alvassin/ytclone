"""Streaming S3 storage; callers own the client and decide which keys to replace."""

import asyncio
import hashlib
import logging
import time
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, aclosing, nullcontext
from urllib.parse import unquote
from uuid import uuid4
from xml.etree import ElementTree as ET

import aiohttp
from aiohttp_s3_client import S3Client

from .format import MIB, ObjectRef
from .interfaces import ObjectStorage, RetryableDownloadError
from .progress import Progress
from .s3_retry import is_retryable_error, retry

log = logging.getLogger(__name__)
MAX_MULTIPART_PARTS = 10_000
EMPTY_HASH = hashlib.sha256(b"").hexdigest()
# Keep the object metadata key stable across the package rename.
UPLOAD_MARKER_HEADER = "x-amz-meta-yt-s3-upload"
TRANSIENT_MULTIPART_ERRORS = frozenset(
    {"InternalError", "SlowDown", "ServiceUnavailable", "RequestTimeout"}
)


class RetryableMultipartError(aiohttp.ClientError):
    """S3 reported a temporary completion failure inside a successful HTTP response."""


class IntegrityError(ValueError):
    """An object differs from its completed manifest."""


async def _multipart_chunks(
    *, chunks: AsyncIterator[bytes], size: int
) -> AsyncIterator[bytes]:
    """Buffer at most one part, independently of producer chunk boundaries."""
    pending = bytearray()
    async for chunk in chunks:
        view = memoryview(chunk)
        offset = 0
        while offset < len(view):
            stop = min(len(view), offset + size - len(pending))
            pending.extend(view[offset:stop])
            offset = stop
            if len(pending) == size:
                part = bytes(pending)
                pending.clear()
                yield part

    if pending:
        yield bytes(pending)


class S3Storage(ObjectStorage):
    def __init__(
        self,
        *,
        client: S3Client,
        chunk_size: int = 32 * MIB,
        progress: Progress | None = None,
    ):
        """Bind the client and validate the multipart buffer size."""
        if not 5 * MIB <= chunk_size <= 512 * MIB:
            raise ValueError("S3 chunk_size must be between 5 and 512 MiB")

        self.client = client
        self.chunk_size = chunk_size
        self.progress = progress

    def _measure(self, *, operation: str = "s3"):
        """Time one request attempt or streamed read when telemetry is enabled."""
        if self.progress is None:
            return nullcontext()
        return self.progress.measure(operation=operation)

    @retry
    async def read_bytes(self, *, key: str) -> bytes | None:
        """Read an object, returning None when it is missing."""
        with self._measure():
            async with self.client.get(object_name=key) as response:
                if response.status == 404:
                    return None
                response.raise_for_status()
                return await response.read()

    @retry
    async def _put(
        self,
        *,
        key: str,
        data: bytes,
        sha256: str,
        params: dict[str, str] | None = None,
    ) -> str | None:
        """Upload bytes with retries and return the response ETag."""
        with self._measure():
            async with self.client.put(
                object_name=key,
                data=data,
                content_sha256=sha256,
                params=params,
            ) as response:
                response.raise_for_status()
                await response.read()
                return response.headers.get("ETag")

    async def put_bytes(self, *, key: str, data: bytes) -> ObjectRef:
        """Upload bytes and record their size and checksum."""
        digest = hashlib.sha256(data).hexdigest()
        await self._put(key=key, data=data, sha256=digest)
        return ObjectRef(key=key, size=len(data), sha256=digest)

    async def upload(self, *, key: str, chunks: AsyncIterator[bytes]) -> ObjectRef:
        """Use one PUT below the buffer size, otherwise stream through multipart."""
        async with aclosing(
            _multipart_chunks(chunks=chunks, size=self.chunk_size)
        ) as parts:
            chunk = await anext(parts, b"")
            if len(chunk) < self.chunk_size:
                # A short normalized part is emitted only after successful EOF.
                started = time.monotonic()
                ref = await self.put_bytes(key=key, data=chunk)
                if self.progress is not None:
                    self.progress.transferred_bytes += ref.size
                log.info(
                    "object_uploaded key=%s method=put bytes=%d seconds=%.3f",
                    key,
                    ref.size,
                    time.monotonic() - started,
                )
                return ref

            upload = self._upload_multipart(key=key, chunk=chunk, parts=parts)
            # Let only the multipart loop retain the first payload as it advances.
            del chunk
            return await upload

    @retry
    async def _create_multipart(self, *, key: str, marker: str) -> str:
        """Create an upload, retaining a unique object marker across request retries."""
        with self._measure():
            async with self.client.post(
                object_name=key,
                params={"uploads": ""},
                data=b"",
                content_sha256=EMPTY_HASH,
                headers={UPLOAD_MARKER_HEADER: marker},
            ) as response:
                response.raise_for_status()
                upload_id = ET.fromstring(await response.read()).findtext(
                    ".//{*}UploadId"
                )
            if not upload_id:
                raise ValueError("S3 did not return a multipart UploadId")
            return upload_id

    async def _multipart_completed(self, *, key: str, marker: str, size: int) -> bool:
        """Recognize this upload after a lost completion response, never an older object."""
        with self._measure():
            async with self.client.head(object_name=key) as response:
                if response.status == 404:
                    return False
                response.raise_for_status()
                return (
                    response.headers.get(UPLOAD_MARKER_HEADER) == marker
                    and response.content_length == size
                )

    @retry
    async def _complete_multipart(
        self, *, key: str, upload_id: str, body: bytes, marker: str, size: int
    ) -> None:
        """Retry the same completion request and resolve an already committed upload."""
        with self._measure():
            async with self.client.post(
                object_name=key,
                params={"uploadId": upload_id},
                data=body,
                content_sha256=hashlib.sha256(body).hexdigest(),
                headers={"Content-Type": "application/xml"},
            ) as response:
                if response.status != 404:
                    response.raise_for_status()
                payload = await response.read()
                try:
                    result = ET.fromstring(payload)
                except ET.ParseError:
                    response.raise_for_status()
                    raise aiohttp.ClientPayloadError(
                        "Invalid S3 completion XML"
                    ) from None

                tag = result.tag.rsplit("}", 1)[-1]
                code = result.findtext("{*}Code") if tag == "Error" else None
                if code != "NoSuchUpload":
                    response.raise_for_status()
                    if tag == "CompleteMultipartUploadResult":
                        return
                    if code in TRANSIENT_MULTIPART_ERRORS:
                        raise RetryableMultipartError(
                            f"S3 multipart completion: {code}"
                        )
                    raise ValueError(
                        f"S3 multipart completion returned an error: {code}"
                    )

        if await self._multipart_completed(key=key, marker=marker, size=size):
            log.info("multipart_completion_recovered key=%s", key)
            return
        raise ValueError(
            "S3 multipart upload disappeared without a matching completed object"
        )

    async def _upload_multipart(
        self, *, key: str, chunk: bytes, parts: AsyncIterator[bytes]
    ) -> ObjectRef:
        """Upload the first buffered part and the remaining stream, aborting on failure."""
        marker = uuid4().hex
        upload_id = await self._create_multipart(key=key, marker=marker)

        completion = ET.Element("CompleteMultipartUpload")
        digest = hashlib.sha256()
        total = 0
        number = 0

        try:
            while chunk is not None:
                number += 1
                if number > MAX_MULTIPART_PARTS:
                    raise ValueError(
                        "S3 multipart part limit reached; use smaller objects"
                    )

                started = time.monotonic()
                etag = await self._put(
                    key=key,
                    data=chunk,
                    sha256=hashlib.sha256(chunk).hexdigest(),
                    params={"uploadId": upload_id, "partNumber": str(number)},
                )
                if self.progress is not None:
                    self.progress.transferred_bytes += len(chunk)
                if not etag:
                    raise ValueError("S3 did not return a multipart ETag")

                part = ET.SubElement(completion, "Part")
                ET.SubElement(part, "PartNumber").text = str(number)
                ET.SubElement(part, "ETag").text = etag
                digest.update(chunk)
                total += len(chunk)
                log.info(
                    "part_uploaded key=%s part=%d bytes=%d seconds=%.3f",
                    key,
                    number,
                    len(chunk),
                    time.monotonic() - started,
                )
                chunk = await anext(parts, None)

            body = ET.tostring(element=completion, encoding="utf-8")
            await self._complete_multipart(
                key=key, upload_id=upload_id, body=body, marker=marker, size=total
            )
        except BaseException:
            try:
                await asyncio.wait_for(
                    self._abort(key=key, upload_id=upload_id), timeout=30
                )
            except BaseException:
                log.exception(
                    "multipart_abort_failed key=%s upload_id=%s", key, upload_id
                )
            raise

        return ObjectRef(key=key, size=total, sha256=digest.hexdigest())

    @retry
    async def _abort(self, *, key: str, upload_id: str) -> None:
        """Discard an unfinished multipart upload."""
        with self._measure():
            async with self.client.delete(
                object_name=key, params={"uploadId": upload_id}
            ) as response:
                if response.status != 404:
                    response.raise_for_status()
                await response.read()

    async def download(self, *, ref: ObjectRef) -> AsyncIterator[bytes]:
        """Stream verified bytes; flag temporary failures after closing the response."""
        digest = hashlib.sha256()
        total = 0
        try:
            async with AsyncExitStack() as stack:
                with self._measure():
                    response = await stack.enter_async_context(
                        self.client.get(object_name=ref.key)
                    )
                    response.raise_for_status()
                    if (
                        response.content_length is not None
                        and response.content_length != ref.size
                    ):
                        raise IntegrityError(
                            f"S3 object size differs from manifest: {ref.key}"
                        )

                while True:
                    with self._measure(operation="s3_download"):
                        chunk = await response.content.read(self.chunk_size)
                        if not chunk:
                            if total != ref.size or digest.hexdigest() != ref.sha256:
                                raise IntegrityError(
                                    f"S3 object checksum or size differs from manifest: {ref.key}"
                                )
                            break

                        if self.progress is not None:
                            self.progress.transferred_bytes += len(chunk)
                        total += len(chunk)
                        if total > ref.size:
                            raise IntegrityError(
                                f"S3 object exceeds manifest size: {ref.key}"
                            )
                        digest.update(chunk)
                    yield chunk
        except (aiohttp.ClientError, asyncio.TimeoutError) as error:
            if not is_retryable_error(error):
                raise
            raise RetryableDownloadError(
                f"Temporary S3 download failure: {type(error).__name__}"
            ) from error

    @retry
    async def delete(self, *, key: str) -> None:
        """Delete one object, tolerating an already missing key."""
        with self._measure():
            async with self.client.delete(object_name=key) as response:
                if response.status != 404:
                    response.raise_for_status()
                await response.read()

    @retry
    async def exists(self, *, ref: ObjectRef) -> bool:
        """Check that an object exists with the recorded size."""
        with self._measure():
            async with self.client.head(object_name=ref.key) as response:
                if response.status == 404:
                    return False
                response.raise_for_status()
                return response.content_length == ref.size

    @retry
    async def _list_page(self, *, params: dict[str, str]) -> ET.Element:
        """Fetch and validate one ListObjectsV2 response."""
        with self._measure():
            async with self.client.get(object_name="", params=params) as response:
                response.raise_for_status()
                result = ET.fromstring(await response.read())
                if result.tag.rsplit("}", 1)[-1] != "ListBucketResult":
                    raise ValueError("S3 listing returned an invalid response")
                return result

    async def list_keys(self, *, prefix: str) -> AsyncIterator[str]:
        """Yield keys under a prefix across all listing pages."""
        params = {"list-type": "2", "prefix": prefix, "encoding-type": "url"}
        seen_tokens = set()
        while True:
            page = await self._list_page(params=params)
            encoded = page.findtext("{*}EncodingType") == "url"
            for entry in page.findall("{*}Contents"):
                key = entry.findtext("{*}Key")
                if key is None:
                    raise ValueError("S3 listing returned an object without a key")
                if encoded:
                    key = unquote(string=key, encoding="utf-8", errors="strict")
                if not key.startswith(prefix):
                    raise ValueError(
                        "S3 listing returned an object outside the requested prefix"
                    )
                yield key

            truncated = page.findtext("{*}IsTruncated")
            if truncated not in ("true", "false"):
                raise ValueError("S3 listing omitted IsTruncated")
            if truncated == "false":
                return
            token = page.findtext("{*}NextContinuationToken")
            if not token or token in seen_tokens:
                raise ValueError("S3 listing returned an invalid continuation token")
            seen_tokens.add(token)
            params["continuation-token"] = token
