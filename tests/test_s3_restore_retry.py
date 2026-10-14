"""Classify failed S3 streams without replaying bytes inside the generator."""

import asyncio
import hashlib
from contextlib import asynccontextmanager

import aiomisc
import pytest
from aiohttp import ClientError, ClientResponseError, ClientSession, ClientTimeout, web
from aiohttp_s3_client import S3Client

from ytclone.format import MIB, ObjectRef
from ytclone.interfaces import RetryableDownloadError
from ytclone.s3 import IntegrityError, S3Storage

PAYLOAD = b"complete archive part"


def run(coroutine):
    """Run one isolated HTTP scenario with the application's async runtime."""
    return aiomisc.run(coroutine, pool_size=2, log_level="error")


def reference(*, data=PAYLOAD):
    """Describe the expected object independently from the server response."""
    return ObjectRef(
        key="data", size=len(data), sha256=hashlib.sha256(data).hexdigest()
    )


class DownloadServer:
    """Serve success, HTTP failure, or a controllably interrupted response."""

    def __init__(self, *, status=200, failure=None, payload=PAYLOAD):
        """Configure one response and retain synchronization events for the test."""
        self.status = status
        self.failure = failure
        self.payload = payload
        self.requests = 0
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def handle(self, request):
        """Hold headers or body until the test releases the request."""
        self.requests += 1
        self.entered.set()
        if self.failure == "headers":
            await self.release.wait()

        if self.failure not in ("disconnect", "body"):
            return web.Response(status=self.status, body=self.payload)

        response = web.StreamResponse(
            headers={"Content-Length": str(len(self.payload))}
        )
        await response.prepare(request)
        await response.write(self.payload[:3])
        await self.release.wait()
        if request.transport is not None:
            request.transport.close()
        return response

    @asynccontextmanager
    async def storage(self, *, timeout=None):
        """Provide the real S3 client against a localhost-only HTTP server."""
        app = web.Application()
        app.router.add_get("/bucket/data", self.handle)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner=runner, host="127.0.0.1", port=0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]

        try:
            async with ClientSession(
                timeout=ClientTimeout(total=None, sock_read=timeout)
            ) as session:
                client = S3Client(
                    session=session,
                    url=f"http://127.0.0.1:{port}/bucket",
                    access_key_id="offline-test",
                    secret_access_key="offline-test",
                    region="us-east-1",
                )
                yield S3Storage(client=client, chunk_size=5 * MIB), session
        finally:
            self.release.set()
            await runner.cleanup()


@pytest.mark.parametrize("status", [408, 429, 500, 503])
def test_transient_http_error_signals_retry_without_another_get(status):
    """Let the engine decide how to restart after a temporary HTTP failure."""
    server = DownloadServer(status=status)

    async def scenario():
        """Observe the preserved HTTP cause and the released connection."""
        async with server.storage() as (storage, session):
            with pytest.raises(RetryableDownloadError) as failure:
                _ = [chunk async for chunk in storage.download(ref=reference())]
            assert isinstance(failure.value.__cause__, ClientResponseError)
            assert failure.value.__cause__.status == status
            assert not session.connector._acquired

    run(scenario())
    assert server.requests == 1


@pytest.mark.parametrize("status", [400, 401, 403, 404])
def test_permanent_http_error_is_not_marked_retryable(status):
    """Keep access, missing object and bad request errors immediately fatal."""
    server = DownloadServer(status=status)

    async def scenario():
        """Read a permanent failure through the real HTTP client."""
        async with server.storage() as (storage, session):
            with pytest.raises(ClientResponseError) as failure:
                _ = [chunk async for chunk in storage.download(ref=reference())]
            assert failure.value.status == status
            assert not session.connector._acquired

    run(scenario())
    assert server.requests == 1


def test_mid_body_disconnect_does_not_replay_the_delivered_prefix():
    """Signal an interrupted body after closing it, without sending duplicate bytes."""
    server = DownloadServer(failure="disconnect")

    async def scenario():
        """Receive a prefix, then force the server to truncate the connection."""
        async with server.storage() as (storage, session):
            stream = storage.download(ref=reference())
            assert await anext(stream) == PAYLOAD[:3]
            server.release.set()

            with pytest.raises(RetryableDownloadError) as failure:
                await anext(stream)
            assert isinstance(failure.value.__cause__, ClientError)
            assert not session.connector._acquired

    run(scenario())
    assert server.requests == 1


@pytest.mark.parametrize("phase", ["headers", "body"])
def test_timeout_is_marked_retryable_after_closing_response(phase):
    """Treat both opening and streaming timeouts as interrupted downloads."""
    server = DownloadServer(failure=phase)

    async def scenario():
        """Wait for a bounded localhost read timeout without retrying in place."""
        async with server.storage(timeout=0.1) as (storage, session):
            stream = storage.download(ref=reference())
            if phase == "body":
                assert await anext(stream) == PAYLOAD[:3]

            with pytest.raises(RetryableDownloadError) as failure:
                await anext(stream)
            assert isinstance(failure.value.__cause__, asyncio.TimeoutError)
            assert not session.connector._acquired

    run(scenario())
    assert server.requests == 1


@pytest.mark.parametrize("payload", [b"x" * len(PAYLOAD), PAYLOAD[:-1]])
def test_integrity_failure_is_not_marked_retryable(payload):
    """Keep checksum and declared length mismatches fatal."""
    server = DownloadServer(payload=payload)

    async def scenario():
        """Consume an object inconsistent with the expected manifest."""
        async with server.storage() as (storage, session):
            with pytest.raises(IntegrityError):
                _ = [chunk async for chunk in storage.download(ref=reference())]
            assert not session.connector._acquired

    run(scenario())
    assert server.requests == 1


@pytest.mark.parametrize("phase", ["headers", "body"])
def test_cancellation_does_not_become_retryable(phase):
    """Preserve cancellation during both the initial GET and a body read."""
    server = DownloadServer(failure=phase)

    async def scenario():
        """Cancel a pending iterator step and verify connection cleanup."""
        async with server.storage() as (storage, session):
            stream = storage.download(ref=reference())
            if phase == "body":
                assert await anext(stream) == PAYLOAD[:3]

            pending = asyncio.create_task(anext(stream))
            await server.entered.wait()
            # Let the newly scheduled step reach its suspended HTTP read.
            await asyncio.sleep(delay=0)
            pending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await pending
            await stream.aclose()
            assert not session.connector._acquired

    run(scenario())
    assert server.requests == 1
