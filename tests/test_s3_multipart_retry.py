"""Multipart request retries against an isolated, fault-injecting S3 server."""

import asyncio
from uuid import UUID

import aiomisc
import pytest
from aiohttp import ClientError, ClientResponseError, web
from test_s3 import Server, chunks, reference, run

from ytclone import s3_retry
from ytclone.format import MIB

UPLOAD_MARKER_HEADER = "x-amz-meta-yt-s3-upload"
FIRST_PART = b"a" * (5 * MIB)
LAST_PART = b"tail"
PAYLOAD = FIRST_PART + LAST_PART


class MultipartRetryServer(Server):
    """Inject faults at create and complete without replacing the real client."""

    def __init__(self):
        """Track request identity and object metadata independently of payloads."""
        super().__init__()
        self.create_actions = []
        self.complete_actions = []
        self.head_actions = []
        self.create_attempts = []
        self.complete_attempts = []
        self.object_metadata = {}
        self.upload_marker = None
        self.completed = False
        self.head_attempts = 0
        self.head_size = None
        self.head_uses_current_marker = False
        self.completion_started = asyncio.Event()
        self.release_completion = asyncio.Event()

    def _disconnect(self, *, request):
        """Drop the real TCP connection before any response headers arrive."""
        assert request.transport is not None
        request.transport.abort()
        return web.Response()

    def _error(self, *, status=200, code="InternalError"):
        """Build the S3 error envelope, including errors carried by HTTP 200."""
        return web.Response(
            status=status,
            body=f"<Error><Code>{code}</Code><Message>injected</Message></Error>".encode(),
        )

    async def handle(self, request):
        """Apply a single scripted fault, then serve ordinary S3 behavior."""
        key = request.match_info["key"]
        query = dict(request.query)

        if request.method == "POST" and "uploads" in query:
            self.upload_marker = request.headers.get(UPLOAD_MARKER_HEADER)
            self.create_attempts.append((key, self.upload_marker, await request.read()))
            action = self.create_actions.pop(0) if self.create_actions else None
            if action is not None:
                self.requests.append((request.method, key, query))
                if action == "disconnect":
                    return self._disconnect(request=request)
                return self._error(status=action)
            return await super().handle(request=request)

        if request.method == "POST" and "uploadId" in query:
            self.complete_attempts.append((query["uploadId"], await request.read()))
            self.completion_started.set()
            action = self.complete_actions.pop(0) if self.complete_actions else None
            if self.completed and action is None:
                action = "NoSuchUpload"
            if action is not None and action != "commit-disconnect":
                self.requests.append((request.method, key, query))
                if action == "disconnect":
                    return self._disconnect(request=request)
                if action == "hold":
                    await self.release_completion.wait()
                    return self._error(status=503)
                if isinstance(action, int):
                    return self._error(status=action)
                if action == "NoSuchUpload":
                    return self._error(status=404, code=action)
                return self._error(code=action)

            response = await super().handle(request=request)
            self.completed = True
            self.object_metadata[key] = self.upload_marker
            if action == "commit-disconnect":
                return self._disconnect(request=request)
            return response

        if request.method == "HEAD":
            self.requests.append((request.method, key, query))
            self.head_attempts += 1
            if self.head_actions:
                return self._error(status=self.head_actions.pop(0))
            if key not in self.objects:
                return web.Response(status=404)
            size = (
                self.head_size if self.head_size is not None else len(self.objects[key])
            )
            headers = {"Content-Length": str(size)}
            marker = (
                self.upload_marker
                if self.head_uses_current_marker
                else self.object_metadata.get(key)
            )
            if marker is not None:
                headers[UPLOAD_MARKER_HEADER] = marker
            return web.Response(headers=headers)

        return await super().handle(request=request)


@pytest.fixture(autouse=True)
def immediate_retries(monkeypatch):
    """Exercise retry attempts without waiting through production backoff."""

    def no_delay(*, attempt):
        """Keep the production retry loop while eliminating its pause."""
        return 0

    monkeypatch.setattr(s3_retry, "retry_delay", no_delay)


def limit_attempts(*, monkeypatch, count):
    """Bound exhaustion tests through the existing configurable retry policy."""
    policy = aiomisc.asyncbackoff(
        attempt_timeout=None,
        deadline=5,
        pause=0,
        max_tries=count,
        exceptions=(ClientError, asyncio.TimeoutError),
        giveup=s3_retry._give_up,
    )
    monkeypatch.setattr(s3_retry, "_policy", policy)


def assert_parts_uploaded_once(*, server):
    """Reject retries that restart the source or resend accepted payload parts."""
    assert [entry[1]["partNumber"] for entry in server.put_requests] == ["1", "2"]
    assert [entry[1]["uploadId"] for entry in server.put_requests] == [
        "test-upload"
    ] * 2
    assert [entry[2] for entry in server.put_requests] == [FIRST_PART, LAST_PART]


@pytest.mark.parametrize("failure", ["disconnect", 503, 429, 408])
def test_create_retries_preserve_upload_marker_and_buffered_source(failure):
    """Retry initiation without losing or rereading the first buffered part."""
    server = MultipartRetryServer()
    server.create_actions = [failure]
    reads = []

    async def source():
        """Record every source chunk consumed across request retries."""
        for part in (FIRST_PART, LAST_PART):
            reads.append(len(part))
            yield part

    async def scenario():
        """Complete an upload after one failed initiation request."""
        async with server.storage() as storage:
            assert await storage.upload(key="data", chunks=source()) == reference(
                data=PAYLOAD
            )

    run(scenario())
    assert len(server.create_attempts) == 2
    assert server.create_attempts[0] == server.create_attempts[1]
    marker = server.create_attempts[0][1]
    assert marker and UUID(marker).version == 4
    assert reads == [len(FIRST_PART), len(LAST_PART)]
    assert_parts_uploaded_once(server=server)
    assert server.objects["data"] == PAYLOAD
    assert not server.aborted


def test_upload_marker_is_unique_for_each_logical_upload():
    """Never identify a previous upload as the current completed object."""
    server = MultipartRetryServer()

    async def scenario():
        """Run two independent uploads through the same storage instance."""
        async with server.storage() as storage:
            for key in ("first", "second"):
                server.completed = False
                await storage.upload(key=key, chunks=chunks(FIRST_PART, LAST_PART))

    run(scenario())
    markers = [attempt[1] for attempt in server.create_attempts]
    assert len(markers) == 2
    assert all(marker and UUID(marker).version == 4 for marker in markers)
    assert markers[0] != markers[1]


def test_create_forbidden_is_not_retried_or_followed_by_parts():
    """Fail initiation immediately when credentials or permissions are rejected."""
    server = MultipartRetryServer()
    server.create_actions = [403]

    async def scenario():
        """Propagate the real HTTP status from the failed creation request."""
        async with server.storage() as storage:
            with pytest.raises(ClientResponseError) as caught:
                await storage.upload(key="data", chunks=chunks(FIRST_PART, LAST_PART))
            assert caught.value.status == 403

    run(scenario())
    assert len(server.create_attempts) == 1
    assert not server.put_requests
    assert not server.complete_attempts
    assert not server.aborted


@pytest.mark.parametrize(
    "failure",
    [
        "disconnect",
        503,
        429,
        408,
        "InternalError",
        "SlowDown",
        "ServiceUnavailable",
        "RequestTimeout",
    ],
)
def test_complete_retries_identical_request_without_reuploading_parts(failure):
    """Retry only completion for transport, status, and embedded transient errors."""
    server = MultipartRetryServer()
    server.complete_actions = [failure]
    reads = []

    async def source():
        """Count chunks to detect an unintended restart of the input stream."""
        for part in (FIRST_PART, LAST_PART):
            reads.append(len(part))
            yield part

    async def scenario():
        """Complete the same multipart upload after its first request fails."""
        async with server.storage() as storage:
            assert await storage.upload(key="data", chunks=source()) == reference(
                data=PAYLOAD
            )

    run(scenario())
    assert len(server.create_attempts) == 1
    assert len(server.complete_attempts) == 2
    assert server.complete_attempts[0] == server.complete_attempts[1]
    assert reads == [len(FIRST_PART), len(LAST_PART)]
    assert_parts_uploaded_once(server=server)
    assert server.objects["data"] == PAYLOAD
    assert not server.aborted


@pytest.mark.parametrize("head_failures", [[], [503]])
def test_completion_committed_before_disconnect_is_verified_by_head(head_failures):
    """Recover an ambiguous completion only from this upload's marker and size."""
    server = MultipartRetryServer()
    server.complete_actions = ["commit-disconnect"]
    server.head_actions = head_failures.copy()

    async def scenario():
        """Treat NoSuchUpload as success only after verifying the committed object."""
        async with server.storage() as storage:
            assert await storage.upload(
                key="data", chunks=chunks(FIRST_PART, LAST_PART)
            ) == reference(data=PAYLOAD)

    run(scenario())
    assert len(server.complete_attempts) == 2 + len(head_failures)
    assert len(set(server.complete_attempts)) == 1
    assert server.head_attempts == 1 + len(head_failures)
    assert_parts_uploaded_once(server=server)
    assert server.object_metadata["data"] == server.create_attempts[0][1]
    assert server.objects["data"] == PAYLOAD
    assert not server.aborted


@pytest.mark.parametrize(
    "mismatch", ["missing-marker", "wrong-marker", "wrong-size", "missing-object"]
)
def test_no_such_upload_does_not_accept_an_unrelated_destination(mismatch):
    """Reject a missing upload when HEAD cannot prove this exact completed object."""
    server = MultipartRetryServer()
    server.complete_actions = ["disconnect", "NoSuchUpload"]
    old_payload = b"z" * len(PAYLOAD)
    if mismatch != "missing-object":
        server.objects["data"] = old_payload
    if mismatch == "wrong-marker":
        server.object_metadata["data"] = "old-upload"
    if mismatch == "wrong-size":
        server.head_uses_current_marker = True
        server.head_size = len(PAYLOAD) + 1

    async def scenario():
        """Fail and abort while preserving any pre-existing destination content."""
        async with server.storage() as storage:
            with pytest.raises((ClientResponseError, ValueError)):
                await storage.upload(key="data", chunks=chunks(FIRST_PART, LAST_PART))

    run(scenario())
    assert len(server.complete_attempts) == 2
    assert server.head_attempts == 1
    assert_parts_uploaded_once(server=server)
    assert server.aborted
    assert server.objects.get("data") == (
        None if mismatch == "missing-object" else old_payload
    )


@pytest.mark.parametrize(
    "failure",
    [403, "InvalidPart", "InvalidPartOrder", "EntityTooSmall", "AccessDenied"],
)
def test_permanent_completion_failure_aborts_without_retry(failure):
    """Never retry permanent completion errors, including HTTP 200 error XML."""
    server = MultipartRetryServer()
    server.complete_actions = [failure]

    async def scenario():
        """Preserve the failure after aborting the known active upload."""
        async with server.storage() as storage:
            expected = ClientResponseError if isinstance(failure, int) else ValueError
            with pytest.raises(expected):
                await storage.upload(key="data", chunks=chunks(FIRST_PART, LAST_PART))

    run(scenario())
    assert len(server.complete_attempts) == 1
    assert server.aborted
    assert "data" not in server.objects
    assert_parts_uploaded_once(server=server)


@pytest.mark.parametrize("failure", [503, "InternalError"])
def test_exhausted_completion_retries_abort_known_upload(failure, monkeypatch):
    """Bound repeated transient completion failures and clean up their parts."""
    limit_attempts(monkeypatch=monkeypatch, count=3)
    server = MultipartRetryServer()
    server.complete_actions = [failure] * 3

    async def scenario():
        """Propagate the exhausted request error after the third attempt."""
        async with server.storage() as storage:
            with pytest.raises(ClientError):
                await storage.upload(key="data", chunks=chunks(FIRST_PART, LAST_PART))

    run(scenario())
    assert len(server.complete_attempts) == 3
    assert len(set(server.complete_attempts)) == 1
    assert_parts_uploaded_once(server=server)
    assert server.aborted
    assert "data" not in server.objects


def test_cancellation_during_completion_aborts_without_retry():
    """Cancellation must stop completion attempts and abort the known upload."""
    server = MultipartRetryServer()
    server.complete_actions = ["hold"]

    async def scenario():
        """Cancel an in-flight HTTP request, then release the server handler."""
        async with server.storage() as storage:
            task = asyncio.create_task(
                storage.upload(key="data", chunks=chunks(FIRST_PART, LAST_PART))
            )
            try:
                await asyncio.wait_for(fut=server.completion_started.wait(), timeout=5)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await asyncio.wait_for(fut=task, timeout=5)
            finally:
                server.release_completion.set()

    run(scenario())
    assert len(server.complete_attempts) == 1
    assert_parts_uploaded_once(server=server)
    assert server.aborted
    assert "data" not in server.objects


def test_exhausted_create_retries_never_upload_parts(monkeypatch):
    """Stop failed creation at the attempt limit without consuming the next part."""
    limit_attempts(monkeypatch=monkeypatch, count=3)
    server = MultipartRetryServer()
    server.create_actions = [503] * 3
    reads = []

    async def source():
        """Detect source consumption beyond the retained initial buffer."""
        for part in (FIRST_PART, LAST_PART):
            reads.append(len(part))
            yield part

    async def scenario():
        """Keep initiation failure distinct from an upload with a known ID."""
        async with server.storage() as storage:
            with pytest.raises(ClientResponseError) as caught:
                await storage.upload(key="data", chunks=source())
            assert caught.value.status == 503

    run(scenario())
    assert len(server.create_attempts) == 3
    assert len(set(server.create_attempts)) == 1
    assert reads == [len(FIRST_PART)]
    assert not server.put_requests
    assert not server.complete_attempts
    assert not server.aborted


def test_cancellation_during_completion_backoff_aborts_without_retry(monkeypatch):
    """Cancellation while waiting for a retry must not issue another completion."""
    server = MultipartRetryServer()
    server.complete_actions = [503]
    backoff_started = asyncio.Event()

    def held_delay(*, attempt):
        """Signal the actual retry backoff without replacing its cancellation path."""
        backoff_started.set()
        return 60

    monkeypatch.setattr(s3_retry, "retry_delay", held_delay)

    async def scenario():
        """Cancel after the first response failed and before its next HTTP attempt."""
        async with server.storage() as storage:
            task = asyncio.create_task(
                storage.upload(key="data", chunks=chunks(FIRST_PART, LAST_PART))
            )
            try:
                await asyncio.wait_for(fut=backoff_started.wait(), timeout=5)
            finally:
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await asyncio.wait_for(fut=task, timeout=5)

    run(scenario())
    assert len(server.complete_attempts) == 1
    assert_parts_uploaded_once(server=server)
    assert server.aborted
    assert "data" not in server.objects
