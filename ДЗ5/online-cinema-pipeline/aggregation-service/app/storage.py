"""Выгрузка метрик в PostgreSQL: миграции, идемпотентный UPSERT, журнал циклов, retry."""
from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

import psycopg

from app import config
from app.metrics import DayResult
from app.retry import with_retry

log = logging.getLogger("storage")

# Временные ошибки: нет соединения, разрыв, сериализация, дедлок, перегрузка — их ретраим.
RETRIABLE = (
    psycopg.OperationalError,
    psycopg.InterfaceError,
    psycopg.errors.SerializationFailure,
    psycopg.errors.DeadlockDetected,
)

UPSERT_SQL = """
INSERT INTO daily_metrics (metric_date, metric_name, dimension, value, computed_at, updated_at)
VALUES (%s, %s, %s, %s, %s, now())
ON CONFLICT (metric_date, metric_name, dimension) DO UPDATE
SET value       = EXCLUDED.value,
    computed_at = EXCLUDED.computed_at,
    updated_at  = now()
WHERE daily_metrics.computed_at <= EXCLUDED.computed_at   -- старый результат не затирает новый
"""

# Строки этой даты от прошлых циклов, которых нет в новом (например, фильм выпал из топа)
DELETE_STALE_SQL = "DELETE FROM daily_metrics WHERE metric_date = %s AND computed_at < %s"

RUN_SQL = """
INSERT INTO aggregation_runs
    (metric_date, trigger, status, events_processed, rows_written, duration_ms, error, started_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""


class PostgresStore:
    def __init__(self, dsn: str = config.POSTGRES_DSN, attempts: int = config.PG_RETRY_ATTEMPTS,
                 base: float = config.PG_RETRY_BASE, cap: float = config.PG_RETRY_MAX, sleep=None):
        self.dsn = dsn
        self._retry = dict(retriable=RETRIABLE, attempts=attempts, base=base, cap=cap)
        if sleep:
            self._retry["sleep"] = sleep

    def _connect(self) -> psycopg.Connection:
        return psycopg.connect(self.dsn, connect_timeout=5)

    # ---------------- миграции ----------------
    def migrate(self, directory: Path = config.PG_MIGRATIONS_DIR) -> list[str]:
        def run() -> list[str]:
            applied = []
            with self._connect() as conn:
                # параллельные старты не мешают друг другу; блокировка снимется при закрытии соединения
                conn.execute("SELECT pg_advisory_lock(424242)")
                conn.execute("CREATE TABLE IF NOT EXISTS schema_migrations "
                             "(name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())")
                conn.commit()
                done = {r[0] for r in conn.execute("SELECT name FROM schema_migrations")}
                conn.commit()
                for f in sorted(directory.glob("*.sql")):
                    if f.name in done:
                        continue
                    with conn.transaction():  # миграция и отметка о ней — атомарно
                        conn.execute(f.read_text(encoding="utf-8"))
                        conn.execute("INSERT INTO schema_migrations (name) VALUES (%s)", (f.name,))
                    applied.append(f.name)
                    log.info("PostgreSQL: применена миграция %s", f.name)
            return applied

        return with_retry(run, what="PostgreSQL migrate", **self._retry)

    # ---------------- запись метрик ----------------
    def save_day(self, result: DayResult) -> int:
        """Идемпотентно записывает метрики дня одной транзакцией. Возвращает число строк."""
        rows = [(r.metric_date, r.metric_name, r.dimension, r.value, result.computed_at) for r in result.rows]

        def run() -> int:
            with self._connect() as conn, conn.transaction():
                with conn.cursor() as cur:
                    cur.executemany(UPSERT_SQL, rows)
                    cur.execute(DELETE_STALE_SQL, (result.metric_date, result.computed_at))
                    removed = cur.rowcount
            if removed:
                log.info("PostgreSQL: %s: удалено %s устаревших строк", result.metric_date, removed)
            return len(rows)

        return with_retry(run, what=f"PostgreSQL upsert {result.metric_date}", **self._retry)

    def record_run(self, metric_date, trigger: str, status: str, events: int, rows: int,
                   duration_ms: int, error: str | None, started_at: datetime) -> None:
        """Журнал цикла — вспомогательная запись: ошибку логируем, но цикл из-за неё не валим."""
        def run() -> None:
            with self._connect() as conn:
                conn.execute(RUN_SQL, (metric_date, trigger, status, events, rows, duration_ms,
                                       error, started_at))
        try:
            with_retry(run, what="PostgreSQL aggregation_runs", **self._retry)
        except Exception as exc:  # noqa: BLE001
            log.error("не удалось записать журнал цикла: %r", exc)

    # ---------------- чтение (для HTTP API) ----------------
    def get_metrics(self, metric_date) -> list[dict]:
        def run():
            with self._connect() as conn:
                cur = conn.execute(
                    "SELECT metric_name, dimension, value, computed_at, updated_at FROM daily_metrics "
                    "WHERE metric_date = %s ORDER BY metric_name, value DESC, dimension", (metric_date,))
                cols = [c.name for c in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]
        return with_retry(run, what="PostgreSQL read metrics", **self._retry)

    def recent_runs(self, limit: int = 20) -> list[dict]:
        def run():
            with self._connect() as conn:
                cur = conn.execute(
                    "SELECT id, metric_date, trigger, status, events_processed, rows_written, duration_ms, "
                    "error, started_at, finished_at FROM aggregation_runs ORDER BY id DESC LIMIT %s", (limit,))
                cols = [c.name for c in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]
        return with_retry(run, what="PostgreSQL read runs", **self._retry)

    # ---------------- журнал экспорта в S3 ----------------
    def get_export(self, metric_date) -> dict | None:
        def run():
            with self._connect() as conn:
                cur = conn.execute("SELECT * FROM s3_exports WHERE metric_date = %s", (metric_date,))
                row = cur.fetchone()
                return dict(zip([c.name for c in cur.description], row)) if row else None
        return with_retry(run, what="PostgreSQL read s3_exports", **self._retry)

    def record_export(self, metric_date, object_key: str, status: str, rows: int = 0, size: int = 0,
                      content_hash: str | None = None, error: str | None = None) -> None:
        sql = """
        INSERT INTO s3_exports AS e (metric_date, object_key, status, rows_exported, size_bytes,
                                     content_hash, attempts, error, exported_at, updated_at)
        VALUES (%(d)s, %(key)s, %(status)s, %(rows)s, %(size)s, %(hash)s, 1, %(error)s,
                CASE WHEN %(status)s = 'success' THEN now() END, now())
        ON CONFLICT (metric_date) DO UPDATE SET
            object_key    = EXCLUDED.object_key,
            status        = EXCLUDED.status,
            rows_exported = CASE WHEN EXCLUDED.status = 'success' THEN EXCLUDED.rows_exported ELSE e.rows_exported END,
            size_bytes    = CASE WHEN EXCLUDED.status = 'success' THEN EXCLUDED.size_bytes ELSE e.size_bytes END,
            content_hash  = CASE WHEN EXCLUDED.status = 'success' THEN EXCLUDED.content_hash ELSE e.content_hash END,
            attempts      = CASE WHEN EXCLUDED.status = 'success' THEN 1 ELSE e.attempts + 1 END,
            error         = EXCLUDED.error,
            exported_at   = COALESCE(EXCLUDED.exported_at, e.exported_at),
            updated_at    = now()
        """
        params = {"d": metric_date, "key": object_key, "status": status, "rows": rows, "size": size,
                  "hash": content_hash, "error": error}

        def run():
            with self._connect() as conn:
                conn.execute(sql, params)
        with_retry(run, what="PostgreSQL write s3_exports", **self._retry)

    def recent_exports(self, limit: int = 20) -> list[dict]:
        def run():
            with self._connect() as conn:
                cur = conn.execute("SELECT * FROM s3_exports ORDER BY metric_date DESC LIMIT %s", (limit,))
                cols = [c.name for c in cur.description]
                return [dict(zip(cols, r)) for r in cur.fetchall()]
        return with_retry(run, what="PostgreSQL read s3_exports", **self._retry)
