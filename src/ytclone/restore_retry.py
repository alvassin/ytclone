"""Bounded node retries after confirmed rollback; never replay uncertain writes."""

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import ParamSpec, TypeVar

import aiomisc

from .interfaces import RetryableDownloadError, RetryableRestoreError

log = logging.getLogger(__name__)
MAX_ATTEMPTS = 30
MAX_DELAY = 30
P = ParamSpec("P")
T = TypeVar("T")


def retry_delay(*, attempt: int) -> float:
    """Return exponential backoff with jitter, capped at thirty seconds."""
    ceiling = min(MAX_DELAY, 2 ** (attempt - 1))
    return random.uniform(a=ceiling / 2, b=ceiling)


def retry(function: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:
    """Retry only the adapter's explicit rollback confirmation."""

    @wraps(function)
    async def wrapped(*args: P.args, **kwargs: P.kwargs) -> T:
        """Keep independent attempt counts without timing out a large restore."""
        attempt = 0
        last_error = None

        @aiomisc.asyncbackoff(
            attempt_timeout=None,
            deadline=None,
            pause=0,
            max_tries=MAX_ATTEMPTS,
            exceptions=(RetryableRestoreError,),
            # aiomisc also catches TimeoutError; require confirmed rollback.
            giveup=lambda error: not isinstance(error, RetryableRestoreError),
        )
        async def execute() -> T:
            """Delay between attempts, then restore from the first archive part."""
            nonlocal attempt, last_error

            if attempt:
                delay = retry_delay(attempt=attempt)
                cause = last_error.__cause__ or last_error
                if isinstance(cause, RetryableDownloadError) and cause.__cause__:
                    cause = cause.__cause__
                log.warning(
                    "restore_retry path=%s attempt=%d/%d delay=%.3fs "
                    "error=%s status=%s",
                    kwargs.get("destination", ""),
                    attempt + 1,
                    MAX_ATTEMPTS,
                    delay,
                    type(cause).__name__,
                    getattr(cause, "status", "-"),
                )
                await asyncio.sleep(delay=delay)

            attempt += 1
            try:
                return await function(*args, **kwargs)
            except RetryableRestoreError as error:
                last_error = error
                raise

        return await execute()

    return wrapped
