"""Один цикл вычислений: ClickHouse (SELECT/INSERT агрегатов) -> PostgreSQL (UPSERT) + логи."""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone

from app.clickhouse import ClickHouse
from app.metrics import DayResult, compute_day
from app.storage import PostgresStore

log = logging.getLogger("pipeline")


@dataclass
class CycleResult:
    metric_date: date
    status: str
    events_processed: int
    rows_written: int
    duration_seconds: float
    error: str | None = None
    day: DayResult | None = None


def run_cycle(ch: ClickHouse, store: PostgresStore, d: date, trigger: str,
              today: date | None = None) -> CycleResult:
    started_at = datetime.now(timezone.utc)
    t0 = time.monotonic()
    log.info("Цикл начат: дата=%s, запуск=%s", d, trigger)
    day, rows_written = None, 0
    try:
        day = compute_day(ch, d, today)
        rows_written = store.save_day(day)
        res = CycleResult(d, "success", day.events_processed, rows_written, time.monotonic() - t0, day=day)
        log.info("Цикл завершён: дата=%s, обработано %s событий, записано %s метрик в PostgreSQL за %.2f сек",
                 d, res.events_processed, rows_written, res.duration_seconds)
    except Exception as exc:  # noqa: BLE001
        res = CycleResult(d, "failed", day.events_processed if day else 0, rows_written,
                          time.monotonic() - t0, error=repr(exc), day=day)
        log.error("Цикл завершён с ошибкой: дата=%s за %.2f сек: %r", d, res.duration_seconds, exc)
    store.record_run(d, trigger, res.status, res.events_processed, res.rows_written,
                     int(res.duration_seconds * 1000), res.error, started_at)
    return res
