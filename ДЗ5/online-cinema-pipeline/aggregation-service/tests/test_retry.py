import random

import pytest

from app.retry import with_retry


class Flaky:
    def __init__(self, fails, exc=ConnectionError):
        self.fails, self.exc, self.calls = fails, exc, 0

    def __call__(self):
        self.calls += 1
        if self.calls <= self.fails:
            raise self.exc("boom")
        return "ok"


def test_retries_then_succeeds_with_exponential_backoff(monkeypatch):
    monkeypatch.setattr(random, "uniform", lambda a, b: b)  # без jitter
    sleeps, fn = [], Flaky(3)
    assert with_retry(fn, what="t", retriable=(ConnectionError,), attempts=5,
                      base=0.5, cap=8, sleep=sleeps.append) == "ok"
    assert fn.calls == 4 and sleeps == [0.5, 1.0, 2.0]


def test_gives_up():
    fn = Flaky(10)
    with pytest.raises(ConnectionError):
        with_retry(fn, what="t", retriable=(ConnectionError,), attempts=3, base=0, cap=0, sleep=lambda s: None)
    assert fn.calls == 3


def test_non_retriable_not_retried():
    fn = Flaky(1, exc=ValueError)
    with pytest.raises(ValueError):
        with_retry(fn, what="t", retriable=(ConnectionError,), attempts=5, base=0, cap=0, sleep=lambda s: None)
    assert fn.calls == 1
