"""S3 adapter contracts against an isolated HTTP server and the real S3 client."""

import asyncio
import hashlib
import time
from contextlib import asynccontextmanager, contextmanager
from urllib.parse import quote
from xml.etree import ElementTree as ET

import aiomisc
import pytest
from aiohttp import ClientError, ClientResponseError, ClientSession, web
from aiohttp_s3_client import S3Client

from ytclone import s3 as storage_module
from ytclone import s3_retry
from ytclone.format import MIB, ObjectRef
from ytclone.interfaces import PublicationConflict, StoredBytes
from ytclone.s3 import IntegrityError, S3Storage, _multipart_chunks


def run(coroutine):
    return aiomisc.run(coroutine, pool_size=2, log_level="error")


def reference(*, key="data", data=b"payload"):
    return ObjectRef(key=key, size=len(data), sha256=hashlib.sha256(data).hexdigest())


async def chunks(*items):
    for item in items:
        yield item


class Server:
    def __init__(self):
        self.objects = {}
        self.parts = {}
        self.requests = []
        self.pages = []
        self.list_queries = []
        self.put_failures = []
        self.put_requests = []
        self.put_conditions = []
        self.lost_put_responses = 0
        self.lost_put_after_headers = False
        self.competing_write = None
        self.complete_error = False
        self.abort_status = 204
        self.missing_etag = False
        self.aborted = False
        self.failure_status = None
        self.request_failures = []
        self.chunked_download = False
        self.part_uploaded = asyncio.Event()

    async def handle(self, request):
        key = request.match_info["key"]
        query = dict(request.query)
        self.requests.append((request.method, key, query))
        if self.request_failures:
            return web.Response(status=self.request_failures.pop(0))

        if self.failure_status:
            return web.Response(status=self.failure_status)

        if request.method == "GET" and query.get("list-type") == "2":
            self.list_queries.append(query)
            return web.Response(body=self.pages.pop(0))
        if request.method == "HEAD":
            if key not in self.objects:
                return web.Response(status=404)
            return web.Response(headers={"Content-Length": str(len(self.objects[key]))})
        if request.method == "GET":
            if key not in self.objects:
                return web.Response(status=404)
            if self.chunked_download:
                response = web.StreamResponse()
                await response.prepare(request)
                await response.write(self.objects[key])
                await response.write_eof()
                return response
            headers = (
                {}
                if self.missing_etag
                else {"ETag": '"' + hashlib.md5(self.objects[key]).hexdigest() + '"'}
            )
            return web.Response(body=self.objects[key], headers=headers)
        if request.method == "POST" and "uploads" in query:
            return web.Response(
                body=b'<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                b"<UploadId>test-upload</UploadId></InitiateMultipartUploadResult>"
            )
        if request.method == "PUT":
            data = await request.read()
            self.put_requests.append((key, query, data))
            self.put_conditions.append(
                (request.headers.get("If-Match"), request.headers.get("If-None-Match"))
            )
            assert (
                request.headers["x-amz-content-sha256"]
                == hashlib.sha256(data).hexdigest()
            )
            if self.put_failures:
                return web.Response(status=self.put_failures.pop(0))
            if request.headers.get("If-None-Match") == "*" and key in self.objects:
                return web.Response(status=412)
            expected = request.headers.get("If-Match")
            if expected is not None and (
                key not in self.objects
                or expected != '"' + hashlib.md5(self.objects[key]).hexdigest() + '"'
            ):
                return web.Response(status=412)
            if "partNumber" in query:
                self.parts[int(query["partNumber"])] = data
                self.part_uploaded.set()
            else:
                self.objects[key] = data
            if self.lost_put_responses:
                self.lost_put_responses -= 1
                if self.competing_write is not None:
                    self.objects[key] = self.competing_write
                if self.lost_put_after_headers:
                    response = web.StreamResponse(headers={"Content-Length": "1"})
                    await response.prepare(request)
                    request.transport.close()
                    return response
                request.transport.close()
                return web.Response()
            headers = (
                {}
                if self.missing_etag
                else {"ETag": '"' + hashlib.md5(data).hexdigest() + '"'}
            )
            return web.Response(headers=headers)
        if request.method == "POST" and "uploadId" in query:
            completion = ET.fromstring(await request.read())
            numbers = [int(part.findtext("PartNumber")) for part in completion]
            assert numbers == sorted(self.parts)
            assert all(len(self.parts[number]) >= 5 * MIB for number in numbers[:-1])
            if self.complete_error:
                return web.Response(body=b"<Error><Code>InvalidPart</Code></Error>")
            self.objects[key] = b"".join(self.parts[number] for number in numbers)
            return web.Response(body=b"<CompleteMultipartUploadResult/>")
        if request.method == "DELETE":
            if "uploadId" in query:
                self.aborted = True
                self.parts.clear()
                return web.Response(status=self.abort_status)
            if key not in self.objects:
                return web.Response(status=404)
            del self.objects[key]
            return web.Response(status=204)
        raise AssertionError((request.method, key, query))

    @asynccontextmanager
    async def storage(self, *, progress=None):
        """Serve isolated S3 requests with optional transfer telemetry."""
        app = web.Application(client_max_size=32 * MIB)
        app.router.add_route("*", "/bucket/{key:.*}", self.handle)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner=runner, host="127.0.0.1", port=0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        try:
            async with ClientSession() as session:
                client = S3Client(
                    session=session,
                    url=f"http://127.0.0.1:{port}/bucket",
                    access_key_id="offline-test",
                    secret_access_key="offline-test",
                    region="us-east-1",
                )
                yield S3Storage(client=client, chunk_size=5 * MIB, progress=progress)
        finally:
            await runner.cleanup()


def test_small_objects_and_encoded_keys_round_trip():
    server = Server()
    key = "folder/a %@?/текст"
    payload = b"\x00\xffpayload"

    async def scenario():
        async with server.storage() as storage:
            assert await storage.read_bytes(key=key) is None
            assert not await storage.exists(ref=reference(key=key, data=payload))
            ref = await storage.put_bytes(key=key, data=payload)
            assert ref == reference(key=key, data=payload)
            assert await storage.read_bytes(key=key) == payload
            assert await storage.exists(ref=ref)
            assert not await storage.exists(ref=reference(key=key, data=b"wrong size"))
            assert (
                b"".join([item async for item in storage.download(ref=ref)]) == payload
            )
            await storage.delete(key=key)
            await storage.delete(key=key)

    run(scenario())
    assert not server.objects


def test_versioned_read_and_conditional_create_update():
    """Read the exact token and preserve it when replacing a manifest."""
    server = Server()
    progress = Telemetry()
    key = "folder/текст/@manifest.json"
    first, second = b"first manifest", b"second manifest"

    async def scenario():
        async with server.storage(progress=progress) as storage:
            assert await storage.read_versioned(key=key) is None
            await storage.put_conditional(key=key, data=first, etag=None)
            stored = await storage.read_versioned(key=key)
            assert stored == StoredBytes(
                data=first, etag='"' + hashlib.md5(first).hexdigest() + '"'
            )
            await storage.put_conditional(key=key, data=second, etag=stored.etag)
            assert await storage.read_bytes(key=key) == second
            assert server.put_conditions == [(None, "*"), (stored.etag, None)]
            assert len(progress.events) == len(server.requests)
            assert progress.transferred_bytes == 0

    run(scenario())


def test_versioned_read_requires_etag_but_read_bytes_does_not():
    """A backend without a concurrency token must fail before publication."""
    server = Server()
    server.objects["manifest"] = b"current"
    server.missing_etag = True

    async def scenario():
        async with server.storage() as storage:
            with pytest.raises(ValueError, match="object ETag"):
                await storage.read_versioned(key="manifest")
            assert await storage.read_bytes(key="manifest") == b"current"

    run(scenario())
    assert [method for method, _, _ in server.requests] == ["GET", "GET"]


@pytest.mark.parametrize("etag", [None, '"outdated-etag"'])
def test_conditional_publication_never_overwrites_competitor(etag):
    """Both create and replacement conditions protect a competing manifest."""
    server = Server()
    server.objects["manifest"] = b"competitor"

    async def scenario():
        async with server.storage() as storage:
            with pytest.raises(PublicationConflict, match="manifest changed"):
                await storage.put_conditional(key="manifest", data=b"ours", etag=etag)

    run(scenario())
    assert server.objects["manifest"] == b"competitor"
    assert [method for method, _, _ in server.requests] == ["PUT", "GET"]
    assert server.put_conditions == [(etag, "*" if etag is None else None)]


@pytest.mark.parametrize("status", [409, 412])
@pytest.mark.parametrize("matching", [False, True])
def test_conditional_conflict_checks_exact_current_body(status, matching):
    """Only our complete manifest can resolve an ambiguous conflict as success."""
    server = Server()
    server.objects["manifest"] = b"ours" if matching else b"ours-plus-a-change"
    server.put_failures = [status]

    async def scenario():
        async with server.storage() as storage:
            if matching:
                await storage.put_conditional(key="manifest", data=b"ours", etag=None)
            else:
                with pytest.raises(PublicationConflict):
                    await storage.put_conditional(
                        key="manifest", data=b"ours", etag=None
                    )

    run(scenario())
    assert [method for method, _, _ in server.requests] == ["PUT", "GET"]


@pytest.mark.parametrize("replacement", [False, True])
@pytest.mark.parametrize("competitor", [False, True])
def test_lost_publication_response_retries_original_condition(
    replacement, competitor, monkeypatch
):
    """A committed PUT can be recognized without overwriting a later writer."""
    server = Server()
    if replacement:
        server.objects["manifest"] = b"previous"
    server.lost_put_responses = 1
    if competitor:
        server.competing_write = b"later manifest"
    monkeypatch.setattr(s3_retry, "retry_delay", lambda *, attempt: 0)
    etag = '"' + hashlib.md5(b"previous").hexdigest() + '"' if replacement else None

    async def scenario():
        async with server.storage() as storage:
            if competitor:
                with pytest.raises(PublicationConflict):
                    await storage.put_conditional(
                        key="manifest", data=b"ours", etag=etag
                    )
            else:
                await storage.put_conditional(key="manifest", data=b"ours", etag=etag)

    run(scenario())
    assert server.objects["manifest"] == (b"later manifest" if competitor else b"ours")
    assert [method for method, _, _ in server.requests] == ["PUT", "PUT", "GET"]
    assert server.put_conditions == [(etag, None if replacement else "*")] * 2


@pytest.mark.parametrize("competitor", [False, True])
def test_final_transport_failure_checks_publication_without_rewriting(
    competitor, monkeypatch
):
    """After exhausting retries, a GET can prove success or retain uncertainty."""
    server = Server()
    server.lost_put_responses = 1
    server.lost_put_after_headers = True
    if competitor:
        server.competing_write = b"later manifest"
    monkeypatch.setattr(
        s3_retry,
        "_policy",
        aiomisc.asyncbackoff(
            attempt_timeout=None,
            deadline=5,
            pause=0,
            max_tries=1,
            exceptions=(ClientError, asyncio.TimeoutError),
        ),
    )

    async def scenario():
        async with server.storage() as storage:
            if competitor:
                with pytest.raises(ClientError):
                    await storage.put_conditional(
                        key="manifest", data=b"ours", etag=None
                    )
            else:
                await storage.put_conditional(key="manifest", data=b"ours", etag=None)

    run(scenario())
    assert server.objects["manifest"] == (b"later manifest" if competitor else b"ours")
    assert [method for method, _, _ in server.requests] == ["PUT", "GET"]
    assert server.put_conditions == [(None, "*")]


def test_conditional_publication_retries_transient_errors_with_same_condition(
    monkeypatch,
):
    """Retries preserve both the intended bytes and the original opaque ETag."""
    server = Server()
    server.objects["manifest"] = b"previous"
    server.put_failures = [500, 503, 429, 408]
    monkeypatch.setattr(s3_retry, "retry_delay", lambda *, attempt: 0)
    etag = '"' + hashlib.md5(b"previous").hexdigest() + '"'

    async def scenario():
        async with server.storage() as storage:
            await storage.put_conditional(key="manifest", data=b"ours", etag=etag)

    run(scenario())
    assert server.objects["manifest"] == b"ours"
    assert server.put_conditions == [(etag, None)] * 5
    assert [body for _, _, body in server.put_requests] == [b"ours"] * 5


def test_conditional_publication_does_not_fall_back_when_unsupported():
    """A server rejecting conditional headers cannot receive an unconditional PUT."""
    server = Server()
    server.put_failures = [400]

    async def scenario():
        async with server.storage() as storage:
            with pytest.raises(ClientResponseError) as error:
                await storage.put_conditional(key="manifest", data=b"ours", etag=None)
            assert error.value.status == 400

    run(scenario())
    assert server.objects == {}
    assert server.put_conditions == [(None, "*")]
    assert [method for method, _, _ in server.requests] == ["PUT"]


@pytest.mark.parametrize(
    "method", ["read", "read-versioned", "exists", "delete", "download", "list"]
)
def test_access_errors_are_not_interpreted_as_missing(method):
    server = Server()
    server.failure_status = 403

    async def scenario():
        async with server.storage() as storage:
            with pytest.raises(ClientResponseError, match="403"):
                if method == "read":
                    await storage.read_bytes(key="data")
                elif method == "read-versioned":
                    await storage.read_versioned(key="data")
                elif method == "exists":
                    await storage.exists(ref=reference())
                elif method == "delete":
                    await storage.delete(key="data")
                elif method == "download":
                    _ = [item async for item in storage.download(ref=reference())]
                else:
                    _ = [key async for key in storage.list_keys(prefix="prefix/")]

    run(scenario())
    assert len(server.requests) == 1


def test_multipart_coalesces_short_chunks_and_splits_large_chunks():
    server = Server()
    pieces = [b"", b"a" * MIB, b"b" * (11 * MIB), b"c", b""]
    payload = b"".join(pieces)

    async def scenario():
        async with server.storage() as storage:
            ref = await storage.upload(key="data", chunks=chunks(*pieces))
            assert ref == reference(data=payload)

    run(scenario())
    assert server.objects["data"] == payload
    assert list(map(len, server.parts.values())) == [5 * MIB, 5 * MIB, 2 * MIB + 1]
    assert not server.aborted


@pytest.mark.parametrize("size_offset", [-1, 0, 1])
def test_stream_size_selects_put_or_multipart(size_offset):
    """Choose by total bytes, independently of empty or fragmented input chunks."""
    server = Server()
    payload = b"a" * (5 * MIB + size_offset)
    pieces = [b"", payload[:13], b"", payload[13:MIB], payload[MIB:], b""]

    async def scenario():
        """Upload a fragmented stream and verify its complete checksum and size."""
        async with server.storage() as storage:
            assert await storage.upload(
                key="data", chunks=chunks(*pieces)
            ) == reference(data=payload)

    run(scenario())

    assert server.objects["data"] == payload
    assert not server.aborted
    if size_offset < 0:
        assert server.requests == [("PUT", "data", {})]
        assert not server.parts
    else:
        assert server.requests[0] == ("POST", "data", {"uploads": ""})
        assert server.requests[-1] == ("POST", "data", {"uploadId": "test-upload"})
        assert [method for method, _, _ in server.requests[1:-1]] == ["PUT"] * (
            1 if size_offset == 0 else 2
        )


def test_short_stream_error_does_not_publish_or_count_data():
    """Do not mistake short chunks for EOF before a source failure arrives."""
    server = Server()
    server.objects["data"] = b"previous object"
    progress = Telemetry()

    async def failing_source():
        """Produce a short buffer, then fail instead of ending cleanly."""
        yield b"partial data"
        yield b""
        raise OSError("source broke")

    async def scenario():
        """Preserve the destination when no complete payload was read."""
        async with server.storage(progress=progress) as storage:
            with pytest.raises(OSError, match="source broke"):
                await storage.upload(key="data", chunks=failing_source())

    run(scenario())

    assert server.requests == []
    assert server.objects["data"] == b"previous object"
    assert progress.transferred_bytes == 0
    assert progress.events == []


def test_cancellation_before_short_stream_eof_does_not_start_upload():
    """Cancellation during initial buffering must leave S3 untouched."""
    server = Server()
    progress = Telemetry()

    async def scenario():
        """Cancel only after the short payload has reached the buffer."""
        waiting_for_eof = asyncio.Event()

        async def blocked_source():
            """Hold the source open after one short chunk."""
            yield b"partial data"
            waiting_for_eof.set()
            await asyncio.Event().wait()

        async with server.storage(progress=progress) as storage:
            task = asyncio.create_task(
                storage.upload(key="data", chunks=blocked_source())
            )
            await asyncio.wait_for(waiting_for_eof.wait(), timeout=5)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

    run(scenario())

    assert server.requests == []
    assert server.objects == {}
    assert progress.transferred_bytes == 0
    assert progress.events == []


def test_failed_small_put_preserves_existing_object_and_byte_counter():
    """Rejected single PUTs neither replace existing bytes nor count as uploaded."""
    server = Server()
    server.objects["data"] = b"previous object"
    server.put_failures = [403]
    progress = Telemetry()

    async def scenario():
        """Return the S3 failure and record its sole HTTP attempt."""
        async with server.storage(progress=progress) as storage:
            with pytest.raises(ClientResponseError) as error:
                await storage.upload(key="data", chunks=chunks(b"new data"))
            assert error.value.status == 403

    run(scenario())

    assert server.requests == [("PUT", "data", {})]
    assert server.objects["data"] == b"previous object"
    assert progress.transferred_bytes == 0
    assert len(progress.events) == 1
    operation, _, error = progress.events[0]
    assert operation == "s3"
    assert error.status == 403


def test_empty_stream_uses_put_without_opening_multipart():
    server = Server()

    async def scenario():
        async with server.storage() as storage:
            assert await storage.upload(key="empty", chunks=chunks(b"")) == reference(
                key="empty", data=b""
            )

    run(scenario())
    assert server.requests == [("PUT", "empty", {})]


def test_failed_source_aborts_multipart_and_preserves_source_error():
    server = Server()
    server.abort_status = 403

    async def failing_source():
        yield b"a" * (5 * MIB)
        raise OSError("source broke")

    async def scenario():
        async with server.storage() as storage:
            with pytest.raises(OSError, match="source broke"):
                await storage.upload(key="data", chunks=failing_source())

    run(scenario())
    assert server.aborted
    assert "data" not in server.objects


@pytest.mark.parametrize("failure", ["etag", "completion", "part-limit"])
def test_invalid_multipart_response_or_limit_aborts(failure, monkeypatch):
    server = Server()
    server.missing_etag = failure == "etag"
    server.complete_error = failure == "completion"
    if failure == "part-limit":
        monkeypatch.setattr(storage_module, "MAX_MULTIPART_PARTS", 1)

    async def scenario():
        async with server.storage() as storage:
            with pytest.raises(ValueError):
                await storage.upload(key="data", chunks=chunks(b"a" * (5 * MIB), b"b"))

    run(scenario())
    assert server.aborted
    assert "data" not in server.objects


def test_cancellation_aborts_active_multipart():
    server = Server()

    async def blocked_source():
        yield b"a" * (5 * MIB)
        await asyncio.Event().wait()

    async def scenario():
        async with server.storage() as storage:
            task = asyncio.create_task(
                storage.upload(key="data", chunks=blocked_source())
            )
            await asyncio.wait_for(server.part_uploaded.wait(), timeout=5)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

    run(scenario())
    assert server.aborted
    assert "data" not in server.objects


def test_upload_part_retries_with_identical_bytes_and_number():
    server = Server()
    server.put_failures = [503]
    payload = b"a" * (5 * MIB)

    async def scenario():
        async with server.storage() as storage:
            assert await storage.upload(
                key="data", chunks=chunks(payload)
            ) == reference(data=payload)

    run(scenario())
    puts = [query for method, _, query in server.requests if method == "PUT"]
    assert puts == [{"uploadId": "test-upload", "partNumber": "1"}] * 2
    assert [body for _, _, body in server.put_requests] == [payload, payload]
    assert server.objects["data"] == payload


@pytest.mark.parametrize("stored", [b"payloae", b"short", b"payload-extra"])
@pytest.mark.parametrize("chunked", [False, True])
def test_corrupt_or_truncated_download_never_reaches_clean_eof(stored, chunked):
    server = Server()
    server.objects["data"] = stored
    server.chunked_download = chunked

    async def scenario():
        async with server.storage() as storage:
            with pytest.raises(IntegrityError):
                _ = [item async for item in storage.download(ref=reference())]

    run(scenario())


def test_missing_download_is_an_error():
    server = Server()

    async def scenario():
        async with server.storage() as storage:
            with pytest.raises(ClientResponseError, match="404"):
                _ = [item async for item in storage.download(ref=reference())]

    run(scenario())


def test_early_download_close_releases_connection():
    server = Server()
    server.objects["data"] = b"x" * (8 * MIB)

    async def scenario():
        async with server.storage() as storage:
            iterator = storage.download(ref=reference(data=server.objects["data"]))
            assert await anext(iterator)
            await iterator.aclose()
            assert not storage.client._session.connector._acquired

    run(scenario())


def page(*, keys=(), truncated="false", token=None):
    root = ET.Element(
        "ListBucketResult", xmlns="http://s3.amazonaws.com/doc/2006-03-01/"
    )
    ET.SubElement(root, "EncodingType").text = "url"
    ET.SubElement(root, "IsTruncated").text = truncated
    if token is not None:
        ET.SubElement(root, "NextContinuationToken").text = token
    for key in keys:
        ET.SubElement(ET.SubElement(root, "Contents"), "Key").text = quote(key)
    return ET.tostring(root)


def test_list_objects_v2_paginates_and_decodes_keys():
    server = Server()
    server.pages = [
        page(keys=["root/a%@?"], truncated="true", token="opaque+/="),
        page(keys=["root/текст"]),
    ]

    async def scenario():
        async with server.storage() as storage:
            assert [key async for key in storage.list_keys(prefix="root/")] == [
                "root/a%@?",
                "root/текст",
            ]

    run(scenario())
    assert server.list_queries == [
        {"list-type": "2", "prefix": "root/", "encoding-type": "url"},
        {
            "list-type": "2",
            "prefix": "root/",
            "encoding-type": "url",
            "continuation-token": "opaque+/=",
        },
    ]


@pytest.mark.parametrize(
    "pages",
    [
        [page(truncated="true")],
        [page(truncated="true", token="same"), page(truncated="true", token="same")],
        [page(truncated="unknown")],
        [page(keys=["other/key"])],
        [b"<Error><Code>InternalError</Code></Error>"],
    ],
    ids=[
        "missing-token",
        "repeated-token",
        "unknown-completeness",
        "wrong-prefix",
        "error-200",
    ],
)
def test_incomplete_or_invalid_listing_is_an_error(pages):
    server = Server()
    server.pages = pages

    async def scenario():
        async with server.storage() as storage:
            with pytest.raises(ValueError):
                _ = [key async for key in storage.list_keys(prefix="root/")]

    run(scenario())


def test_normalization_does_not_prefetch_the_next_producer_chunk():
    consumed = []

    async def source():
        for value in (b"abcdef", b"gh"):
            consumed.append(value)
            yield value

    async def scenario():
        iterator = _multipart_chunks(chunks=source(), size=2)
        assert await anext(iterator) == b"ab"
        assert consumed == [b"abcdef"]
        assert await anext(iterator) == b"cd"
        assert consumed == [b"abcdef"]
        assert [item async for item in iterator] == [b"ef", b"gh"]

    run(scenario())


class Telemetry:
    """Capture the adapter's public progress contract independently."""

    def __init__(self):
        """Initialize counters and request measurement records."""
        self.transferred_bytes = 0
        self.completed_bytes = 0
        self.events = []
        self.active = []

    @contextmanager
    def measure(self, *, operation):
        """Record measured boundaries, elapsed time, and raised errors."""
        started = time.monotonic()
        error = None
        self.active.append(operation)
        try:
            yield
        except BaseException as exc:
            error = exc
            raise
        finally:
            assert self.active.pop() == operation
            self.events.append((operation, time.monotonic() - started, error))


def test_progress_counts_data_once_despite_retries_and_excludes_metadata():
    """Accepted payload bytes count once while every HTTP attempt is measured."""
    server = Server()
    progress = Telemetry()
    payload = b"payload"

    async def scenario():
        """Mix metadata requests, a retried upload, and a complete empty object."""
        async with server.storage(progress=progress) as storage:
            await storage.put_bytes(key="@attributes.yson", data=b"metadata")
            assert await storage.read_bytes(key="@attributes.yson") == b"metadata"
            assert await storage.read_bytes(key="missing") is None
            assert not await storage.exists(ref=reference(key="missing"))
            await storage.delete(key="missing")
            server.pages = [page(keys=["@attributes.yson"])]
            assert [key async for key in storage.list_keys(prefix="")] == [
                "@attributes.yson"
            ]
            assert progress.transferred_bytes == 0

            server.put_failures = [503]
            assert await storage.upload(
                key="data", chunks=chunks(payload)
            ) == reference(data=payload)
            await storage.upload(key="empty", chunks=chunks())
            assert [request for request in server.requests if request[1] == "data"] == [
                ("PUT", "data", {}),
                ("PUT", "data", {}),
            ]
            assert [body for key, _, body in server.put_requests if key == "data"] == [
                payload,
                payload,
            ]
            assert server.objects["data"] == payload
            assert progress.transferred_bytes == len(payload)
            assert progress.completed_bytes == 0
            assert len(progress.events) == len(server.requests)
            assert all(operation == "s3" for operation, _, _ in progress.events)
            errors = [error for _, _, error in progress.events if error is not None]
            assert len(errors) == 1 and errors[0].status == 503
            assert all(seconds > 0 for _, seconds, _ in progress.events)

    run(scenario())


@pytest.mark.parametrize("failure", ["source", "completion"])
def test_progress_keeps_accepted_bytes_when_multipart_aborts(failure):
    """Aborting an object does not erase data that S3 already accepted."""
    server = Server()
    server.complete_error = failure == "completion"
    progress = Telemetry()
    payload = b"a" * (5 * MIB)

    async def source():
        """Fail only after one complete multipart payload was accepted."""
        yield payload
        if failure == "source":
            raise OSError("source failed")

    async def scenario():
        """Verify accepted transfer bytes survive either failure boundary."""
        async with server.storage(progress=progress) as storage:
            with pytest.raises((OSError, ValueError)):
                await storage.upload(key="data", chunks=source())
            assert progress.transferred_bytes == len(payload)
            assert progress.completed_bytes == 0
            assert len(progress.events) == len(server.requests)
            errors = [error for _, _, error in progress.events if error is not None]
            assert len(errors) == (1 if failure == "completion" else 0)

    run(scenario())
    assert server.aborted


def test_progress_counts_corrupt_download_bytes_and_excludes_consumer_waits():
    """Downloaded bytes remain counted when EOF checksum verification fails."""
    server = Server()
    server.objects["data"] = b"payloae"
    progress = Telemetry()

    async def scenario():
        """Pause the consumer outside all S3 timing scopes, then reach corrupt EOF."""
        async with server.storage(progress=progress) as storage:
            stream = storage.download(ref=reference())
            received = bytearray()
            with pytest.raises(IntegrityError):
                while True:
                    chunk = await anext(stream)
                    received.extend(chunk)
                    assert progress.active == []
                    assert progress.transferred_bytes == len(received)
                    await asyncio.sleep(0)
            await stream.aclose()
            assert bytes(received) == server.objects["data"]
            assert progress.completed_bytes == 0
            headers = [event for event in progress.events if event[0] == "s3"]
            reads = [event for event in progress.events if event[0] == "s3_download"]
            assert len(headers) == 1 and headers[0][2] is None
            assert len(reads) >= 2
            assert isinstance(reads[-1][2], IntegrityError)
            assert all(error is None for _, _, error in reads[:-1])

    run(scenario())


def test_progress_records_missing_download_as_failed_request():
    """A failed GET is timed without counting any transferred data."""
    server = Server()
    progress = Telemetry()

    async def scenario():
        """Read a missing object through the streaming data interface."""
        async with server.storage(progress=progress) as storage:
            with pytest.raises(ClientResponseError, match="404"):
                _ = [chunk async for chunk in storage.download(ref=reference())]
            assert progress.transferred_bytes == 0
            assert len(progress.events) == 1
            operation, _, error = progress.events[0]
            assert operation == "s3" and error.status == 404

    run(scenario())


def test_progress_counts_all_data_parts_once_in_each_direction():
    """Multipart completion and metadata do not duplicate payload accounting."""
    server = Server()
    progress = Telemetry()
    payload = b"a" * (5 * MIB) + b"tail"

    async def scenario():
        """Transfer a two-part object in both directions with shared telemetry."""
        async with server.storage(progress=progress) as storage:
            ref = await storage.upload(key="data", chunks=chunks(payload))
            assert progress.transferred_bytes == len(payload)
            assert len(server.parts) == 2

            downloaded = b"".join([chunk async for chunk in storage.download(ref=ref)])
            assert downloaded == payload
            assert progress.transferred_bytes == 2 * len(payload)
            assert progress.completed_bytes == 0
            assert all(error is None for _, _, error in progress.events)

    run(scenario())


@pytest.mark.parametrize(
    "method", ["read", "read-versioned", "exists", "delete", "list", "put", "abort"]
)
def test_s3_operations_recover_after_multiple_transient_errors(method, monkeypatch):
    """Retry actual S3 requests beyond the old limit without counting delays as I/O."""
    server = Server()
    server.objects["data"] = b"payload"
    server.request_failures = [500, 503, 429, 408]
    server.pages = [
        b"<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>"
    ]
    telemetry = Telemetry()
    delays = []

    def immediate_delay(*, attempt):
        """Record each retry without slowing the HTTP contract test."""
        delays.append(attempt)
        return 0

    monkeypatch.setattr(s3_retry, "retry_delay", immediate_delay)

    async def scenario():
        """Exercise the real client and verify the final successful operation."""
        async with server.storage(progress=telemetry) as storage:
            if method == "read":
                assert await storage.read_bytes(key="data") == b"payload"
            elif method == "read-versioned":
                stored = await storage.read_versioned(key="data")
                assert stored.data == b"payload"
                assert stored.etag == '"' + hashlib.md5(b"payload").hexdigest() + '"'
            elif method == "exists":
                assert await storage.exists(ref=reference())
            elif method == "delete":
                await storage.delete(key="data")
                assert "data" not in server.objects
            elif method == "list":
                assert [key async for key in storage.list_keys(prefix="prefix/")] == []
            elif method == "put":
                assert await storage.put_bytes(key="data", data=b"new") == reference(
                    key="data", data=b"new"
                )
                assert server.objects["data"] == b"new"
            else:
                await storage._abort(key="data", upload_id="test-upload")
                assert server.aborted

    run(scenario())

    assert len(server.requests) == 5
    assert delays == [1, 2, 3, 4]
    assert [event[2] is not None for event in telemetry.events] == [True] * 4 + [False]
