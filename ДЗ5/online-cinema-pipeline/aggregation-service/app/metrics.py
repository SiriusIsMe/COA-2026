"""Вычисление дневных бизнес-метрик в ClickHouse.

Для даты D:
  1. SQL из sql/*.sql выполняются как INSERT ... SELECT: ClickHouse сам считает агрегаты
     по raw-событиям (cinema.movie_events) и материализует их в cinema.daily_metrics
     с меткой computed_at этого цикла;
  2. результаты цикла читаются обратно по точному computed_at — их потом выгружаем в PostgreSQL.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone

from app import config
from app.clickhouse import ClickHouse

log = logging.getLogger("metrics")

READ_SQL = """
SELECT metric_date, metric_name, dimension, value
FROM cinema.daily_metrics FINAL
WHERE metric_date = {d:Date}
  AND computed_at = parseDateTime64BestEffort({ts:String}, 3, 'UTC')
ORDER BY metric_name, value DESC, dimension
"""


@dataclass(frozen=True)
class MetricRow:
    metric_date: date
    metric_name: str
    dimension: str
    value: float


@dataclass
class DayResult:
    metric_date: date
    computed_at: datetime
    rows: list[MetricRow] = field(default_factory=list)
    events_processed: int = 0
    duration_seconds: float = 0.0

    def value(self, name: str, dimension: str = "") -> float | None:
        return next((r.value for r in self.rows if r.metric_name == name and r.dimension == dimension), None)


def load_sql() -> list[tuple[str, str]]:
    files = sorted(config.SQL_DIR.glob("*.sql"))
    if not files:
        raise RuntimeError(f"нет SQL-файлов в {config.SQL_DIR}")
    return [(f.name, f.read_text(encoding="utf-8")) for f in files]


def compute_day(ch: ClickHouse, d: date, today: date | None = None) -> DayResult:
    today = today or datetime.now(timezone.utc).date()
    started = time.monotonic()
    now = datetime.now(timezone.utc)
    computed_at = now.replace(microsecond=now.microsecond // 1000 * 1000)  # точность DateTime64(3)
    ts = computed_at.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    params = {
        "d": d.isoformat(),
        "ts": ts,
        "top_n": config.TOP_MOVIES_LIMIT,
        # retention_dN считаем, только когда день d+N полностью завершён (d+N < today),
        # иначе доля вернувшихся занижена
        "with_d1": int((today - d).days > 1),
        "with_d7": int((today - d).days > 7),
        # последний завершённый день жизни когорты (0..7); day_0 есть всегда
        "max_day": max(0, min(7, (today - d).days - 1)),
    }
    for name, sql in load_sql():
        ch.execute(sql, **params)
        log.debug("executed %s for %s", name, d)

    rows = [
        MetricRow(date.fromisoformat(r["metric_date"]), r["metric_name"], r["dimension"], float(r["value"]))
        for r in ch.rows(READ_SQL, d=d.isoformat(), ts=ts)
    ]
    result = DayResult(d, computed_at, rows)
    result.events_processed = int(result.value("events_total") or 0)
    result.duration_seconds = time.monotonic() - started
    return result
