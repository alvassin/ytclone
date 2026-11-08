"""Validate public retry limits and bounded exponential jitter."""

import pytest

from ytclone import read_retry
from ytclone.engine import CopyOptions
from ytclone.yt import YtAdapter


@pytest.mark.parametrize("value", [0, -1, float("nan"), float("inf"), -float("inf")])
def test_invalid_timeout_is_rejected_before_resources(value):
    with pytest.raises(ValueError, match="timeout"):
        CopyOptions(yt_read_timeout=value)
    with pytest.raises(ValueError, match="timeout"):
        YtAdapter(proxy="test", read_timeout=value)


@pytest.mark.parametrize("value", [0, -1, True, 1.5])
def test_attempts_require_a_positive_integer(value):
    with pytest.raises(ValueError, match="attempts"):
        CopyOptions(yt_read_attempts=value)


@pytest.mark.parametrize(
    "attempt,ceiling", [(1, 1), (2, 2), (3, 4), (5, 16), (6, 30), (20, 30)]
)
def test_exponential_backoff_has_jitter_and_a_cap(monkeypatch, attempt, ceiling):
    def uniform(*, a, b):
        assert (a, b) == (ceiling / 2, ceiling)
        return (a + b) / 2

    monkeypatch.setattr(read_retry.random, "uniform", uniform)
    assert read_retry.retry_delay(attempt=attempt) == ceiling * 0.75
