"""Runtime settings for retrying table ranges without changing archive identity."""

import math
import random

DEFAULT_TIMEOUT = 180.0
DEFAULT_ATTEMPTS = 10
MAX_DELAY = 30


def validate_timeout(timeout: float) -> None:
    """Reject disabled or unbounded socket waits before opening a client."""
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("YT read timeout must be positive and finite")


def retry_delay(*, attempt: int) -> float:
    """Return an independent exponential pause with jitter, capped at 30 seconds."""
    ceiling = min(MAX_DELAY, 2 ** (attempt - 1))
    return random.uniform(a=ceiling / 2, b=ceiling)
