"""Bounded S3 retries with independent exponential backoff for each request."""

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import ParamSpec, TypeVar

import aiohttp
import aiomisc

log = logging.getLogger(__name__)
MAX_ATTEMPTS = 30
ATTEMPT_TIMEOUT = 60
RETRY_DEADLINE = 45 * 60
MAX_DELAY = 30
P = ParamSpec("P")
T = TypeVar("T")


def is_retryable_error(error: Exception) -> bool:
    """Recognize temporary HTTP and transport failures, excluding local errors."""
    if isinstance(error, aiohttp.ClientResponseError):
        return error.status >= 500 or error.status in (408, 429)
    return isinstance(error, (aiohttp.ClientError, asyncio.TimeoutError))


def _give_up(error: Exception) -> bool:
    """Stop retrying nontransient HTTP errors."""
    return not is_retryable_error(error)


def retry_delay(*, attempt: int) -> float:
    """Return a jittered exponential pause after the failed attempt."""
    ceiling = min(MAX_DELAY, 2 ** (attempt - 1))
    return random.uniform(a=ceiling / 2, b=ceiling)


_policy = aiomisc.asyncbackoff(
    attempt_timeout=None,
    deadline=RETRY_DEADLINE,
    pause=0,
    max_tries=MAX_ATTEMPTS,
    exceptions=(aiohttp.ClientError, asyncio.TimeoutError),
    giveup=_give_up,
)


def retry(function: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:
    """Retry transient S3 failures without sharing delay state between calls."""

    @wraps(function)
    async def wrapped(*args: P.args, **kwargs: P.kwargs) -> T:
        """Keep attempt state within this request's total time budget."""
        attempt = 0
        last_error = None

        @_policy
        async def request() -> T:
            """Wait before retries, then give the HTTP operation its own timeout."""
            nonlocal attempt, last_error

            if attempt:
                delay = retry_delay(attempt=attempt)
                key = (
                    kwargs.get("key")
                    or getattr(kwargs.get("ref"), "key", None)
                    or (kwargs.get("params") or {}).get("prefix", "")
                )
                log.warning(
                    "s3_retry operation=%s key=%s attempt=%d/%d "
                    "delay=%.3fs error=%s status=%s",
                    function.__name__,
                    key,
                    attempt + 1,
                    MAX_ATTEMPTS,
                    delay,
                    type(last_error).__name__,
                    getattr(last_error, "status", "-"),
                )
                await asyncio.sleep(delay=delay)

            attempt += 1
            try:
                return await asyncio.wait_for(
                    fut=function(*args, **kwargs), timeout=ATTEMPT_TIMEOUT
                )
            except (aiohttp.ClientError, asyncio.TimeoutError) as error:
                last_error = error
                raise

        return await request()

    return wrapped
