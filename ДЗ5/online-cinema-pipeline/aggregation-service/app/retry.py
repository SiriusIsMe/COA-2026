"""Повтор операции с exponential backoff и jitter."""
from __future__ import annotations

import logging
import random
import time
from typing import Callable, TypeVar

T = TypeVar("T")
log = logging.getLogger("retry")


def backoff_delay(attempt: int, base: float, cap: float) -> float:
    """full jitter: случайно в [0, min(cap, base * 2^(attempt-1))]"""
    return random.uniform(0, min(cap, base * 2 ** (attempt - 1)))


def with_retry(fn: Callable[[], T], *, what: str, retriable: tuple[type[BaseException], ...],
               attempts: int, base: float, cap: float,
               sleep: Callable[[float], None] = time.sleep) -> T:
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except retriable as exc:
            if attempt == attempts:
                log.error("%s: попытка %s/%s не удалась, сдаёмся: %r", what, attempt, attempts, exc)
                raise
            delay = backoff_delay(attempt, base, cap)
            log.warning("%s: попытка %s/%s не удалась: %r — повтор через %.2f c",
                        what, attempt, attempts, exc, delay)
            sleep(delay)
    raise AssertionError("unreachable")
