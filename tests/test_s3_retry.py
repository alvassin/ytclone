"""S3 retry policy without external services or real backoff waits."""

import asyncio
import logging
from types import SimpleNamespace

import aiohttp
import aiomisc
import pytest

from ytclone import s3_retry


def http_error(*, status=503, message="temporary failure"):
    """Build an HTTP error without contacting S3."""
    return aiohttp.ClientResponseError(
        request_info=SimpleNamespace(real_url="https://s3.example.test/object"),
        history=(),
        status=status,
        message=message,
    )


@pytest.fixture
def delays(monkeypatch):
    """Record requested retry numbers while avoiding wall-clock backoff."""
    attempts = []

    def zero_delay(*, attempt):
        """Record one retry and let the event loop yield immediately."""
        attempts.append(attempt)
        return 0

    monkeypatch.setattr(s3_retry, "retry_delay", zero_delay)
    return attempts


@pytest.mark.parametrize("status", [408, 429, 500, 503])
def test_transient_http_errors_recover_on_thirtieth_attempt(status, delays):
    """A temporary S3 outage can recover on the last allowed attempt."""
    attempts = 0

    @s3_retry.retry
    async def read_manifest(*, key):
        """Return the manifest once twenty-nine simulated errors have passed."""
        nonlocal attempts
        attempts += 1
        if attempts <= 29:
            raise http_error(status=status)
        return b"manifest"

    assert asyncio.run(read_manifest(key="folder/@manifest.json")) == b"manifest"
    assert attempts == 30
    assert delays == list(range(1, 30))


def test_exhaustion_makes_thirty_attempts_without_a_final_sleep(delays):
    """Exhaustion preserves the last HTTP error and stops before another pause."""
    errors = []

    @s3_retry.retry
    async def unavailable(*, key):
        """Fail each attempt with a separately identifiable HTTP error."""
        error = http_error()
        errors.append(error)
        raise error

    with pytest.raises(aiohttp.ClientResponseError) as captured:
        asyncio.run(unavailable(key="folder/@manifest.json"))

    assert len(errors) == 30
    assert captured.value is errors[-1]
    assert delays == list(range(1, 30))


@pytest.mark.parametrize("status", [400, 401, 403, 404, 409])
def test_permanent_http_error_is_not_retried(status, delays):
    """Permanent response errors fail immediately without backoff."""
    attempts = 0
    error = http_error(status=status)

    @s3_retry.retry
    async def denied(*, key):
        """Return the same permanent error on any unexpected repetition."""
        nonlocal attempts
        attempts += 1
        raise error

    with pytest.raises(aiohttp.ClientResponseError) as captured:
        asyncio.run(denied(key="folder/@manifest.json"))

    assert captured.value is error
    assert attempts == 1
    assert delays == []


def test_non_transport_error_is_not_retried(delays):
    """Invalid local data does not become an S3 retry."""
    attempts = 0
    error = ValueError("invalid manifest")

    @s3_retry.retry
    async def invalid(*, key):
        """Reject the invalid input before another request is attempted."""
        nonlocal attempts
        attempts += 1
        raise error

    with pytest.raises(ValueError) as captured:
        asyncio.run(invalid(key="folder/@manifest.json"))

    assert captured.value is error
    assert attempts == 1
    assert delays == []


def test_network_failure_can_recover(delays):
    """Connection failures use the same bounded retry policy."""
    attempts = 0

    @s3_retry.retry
    async def reconnect(*, key):
        """Fail the first connection and complete the second."""
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise aiohttp.ClientConnectionError("connection lost")
        return b"payload"

    assert asyncio.run(reconnect(key="data")) == b"payload"
    assert attempts == 2
    assert delays == [1]


def test_each_http_attempt_has_its_own_timeout(monkeypatch, delays):
    """Hung requests time out and retry up to the configured attempt limit."""
    monkeypatch.setattr(s3_retry, "ATTEMPT_TIMEOUT", 0.005)
    attempts = 0
    cancelled = 0

    @s3_retry.retry
    async def blocked(*, key):
        """Wait until the request timeout cancels this attempt."""
        nonlocal attempts, cancelled
        attempts += 1
        try:
            await asyncio.Event().wait()
        finally:
            cancelled += 1

    with pytest.raises(asyncio.TimeoutError):
        asyncio.run(blocked(key="data"))

    assert attempts == cancelled == 30
    assert delays == list(range(1, 30))


def test_cancellation_during_http_does_not_retry(delays):
    """Caller cancellation promptly closes the active request."""
    attempts = 0

    async def scenario():
        """Cancel after entering the first request."""
        started = asyncio.Event()
        closed = asyncio.Event()

        @s3_retry.retry
        async def blocked(*, key):
            """Signal request entry and cleanup around an indefinite wait."""
            nonlocal attempts
            attempts += 1
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                closed.set()

        task = asyncio.create_task(blocked(key="data"))
        await asyncio.wait_for(started.wait(), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=1)
        assert closed.is_set()

    asyncio.run(scenario())
    assert attempts == 1
    assert delays == []


def test_cancellation_during_backoff_does_not_start_another_request(monkeypatch):
    """Long retry pauses remain interruptible."""
    attempts = 0

    async def scenario():
        """Cancel once the first retry pause has been selected."""
        waiting = asyncio.Event()

        def long_delay(*, attempt):
            """Signal that the retry has reached its pause."""
            waiting.set()
            return 3600

        monkeypatch.setattr(s3_retry, "retry_delay", long_delay)

        @s3_retry.retry
        async def unavailable(*, key):
            """Reject the request before the long retry wait."""
            nonlocal attempts
            attempts += 1
            raise http_error()

        task = asyncio.create_task(unavailable(key="data"))
        await asyncio.wait_for(waiting.wait(), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=1)

    asyncio.run(scenario())
    assert attempts == 1


def test_concurrent_calls_have_independent_retry_sequences(delays):
    """One worker's failures do not advance another worker's backoff."""
    attempts = {"a": 0, "b": 0}

    @s3_retry.retry
    async def request(*, key):
        """Complete each independent request on its fourth attempt."""
        attempts[key] += 1
        if attempts[key] <= 3:
            raise http_error()
        return key

    async def scenario():
        """Run both retry loops on the same event loop."""
        return await asyncio.gather(request(key="a"), request(key="b"))

    assert asyncio.run(scenario()) == ["a", "b"]
    assert attempts == {"a": 4, "b": 4}
    assert sorted(delays) == [1, 1, 2, 2, 3, 3]


@pytest.mark.parametrize(
    ("attempt", "maximum"),
    [(1, 1), (2, 2), (3, 4), (4, 8), (5, 16), (6, 30), (7, 30), (20, 30)],
)
def test_retry_jitter_stays_inside_exponential_bounds(attempt, maximum):
    """Equal jitter preserves exponential growth and the maximum pause."""
    for _ in range(20):
        delay = s3_retry.retry_delay(attempt=attempt)
        assert maximum / 2 <= delay <= maximum


def test_retry_log_omits_exception_text(caplog, delays):
    """Retry diagnostics identify the failure without exposing response contents."""
    attempts = 0
    secret = "Authorization=private-test-credential"

    @s3_retry.retry
    async def read_manifest(*, key):
        """Fail once with sensitive exception text, then return the manifest."""
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise http_error(message=secret)
        return b"manifest"

    with caplog.at_level(level=logging.WARNING, logger="ytclone.s3_retry"):
        assert asyncio.run(read_manifest(key="folder/@manifest.json")) == b"manifest"

    messages = [record.getMessage() for record in caplog.records]
    retry_messages = [message for message in messages if "s3_retry " in message]
    assert len(retry_messages) == 1
    message = retry_messages[0]
    assert "operation=read_manifest" in message
    assert "key=folder/@manifest.json" in message
    assert "attempt=2/30" in message
    assert "delay=" in message
    assert "error=ClientResponseError" in message
    assert "status=503" in message
    assert secret not in caplog.text


def test_backoff_pause_does_not_consume_http_attempt_timeout(monkeypatch):
    """A pause longer than the HTTP budget does not time out a healthy retry."""
    monkeypatch.setattr(s3_retry, "ATTEMPT_TIMEOUT", 0.01)
    monkeypatch.setattr(s3_retry, "retry_delay", lambda *, attempt: 0.04)
    attempts = 0

    @s3_retry.retry
    async def request(*, key):
        """Fail immediately once, then finish immediately after the pause."""
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise http_error()
        return b"payload"

    assert asyncio.run(request(key="data")) == b"payload"
    assert attempts == 2


def test_deadline_includes_backoff_pause(monkeypatch):
    """The overall deadline cancels backoff and preserves the triggering error."""
    monkeypatch.setattr(
        s3_retry,
        "_policy",
        aiomisc.asyncbackoff(
            attempt_timeout=None,
            deadline=0.01,
            pause=0,
            max_tries=s3_retry.MAX_ATTEMPTS,
            exceptions=(aiohttp.ClientError, asyncio.TimeoutError),
            giveup=s3_retry._give_up,
        ),
    )
    monkeypatch.setattr(s3_retry, "retry_delay", lambda *, attempt: 3600)
    error = http_error()
    attempts = 0

    @s3_retry.retry
    async def request(*, key):
        """Fail before waiting longer than the overall retry deadline."""
        nonlocal attempts
        attempts += 1
        raise error

    async def scenario():
        """Bound the test even if the overall deadline regresses."""
        with pytest.raises(aiohttp.ClientResponseError) as captured:
            await asyncio.wait_for(request(key="data"), timeout=1)
        assert captured.value is error

    asyncio.run(scenario())
    assert attempts == 1
