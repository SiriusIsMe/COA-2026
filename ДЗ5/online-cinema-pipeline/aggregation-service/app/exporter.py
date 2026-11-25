"""Экспорт дневных агрегатов из PostgreSQL в S3-совместимое хранилище (SeaweedFS) — «холодное» хранилище.

Объект: s3://<bucket>/daily/YYYY-MM-DD/aggregates.json — один файл на дату. Повторная выгрузка
пишет по тому же ключу, то есть перезаписывает файл, а не создаёт дубликат.

Плановый запуск проходит по завершённым дням [сегодня - EXPORT_LOOKBACK_DAYS, вчера] и выгружает дату,
если она ещё не выгружена успешно или её метрики изменились с прошлой выгрузки (по content_hash).
Если PostgreSQL или S3 недоступны, ошибка логируется, в s3_exports пишется status=failed (если PG доступен),
и дата будет выгружена при следующем запуске. Ручной запуск (HTTP/CLI) выгружает дату всегда.
"""
from __future__ import annotations

import hashlib
import io
import json
import logging
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

from app import config
from app.retry import with_retry

log = logging.getLogger("exporter")

FORMAT_VERSION = 1


def object_key(d: date) -> str:
    return f"{config.S3_PREFIX}/{d.isoformat()}/aggregates.json"


class S3Storage:
    def __init__(self):
        import urllib3
        from minio import Minio

        self.bucket = config.S3_BUCKET
        # короткие таймауты и без внутренних ретраев: ретраим сами (with_retry) и при следующем запуске
        http = urllib3.PoolManager(timeout=urllib3.Timeout(connect=3, read=15),
                                   retries=urllib3.Retry(total=0))
        self.client = Minio(config.S3_ENDPOINT, access_key=config.S3_ACCESS_KEY,
                            secret_key=config.S3_SECRET_KEY, secure=config.S3_SECURE,
                            region=config.S3_REGION, http_client=http)  # minio — обычный S3-клиент
        self.retriable = (urllib3.exceptions.HTTPError, OSError)
        self._bucket_ready = False

    def _ensure_bucket(self) -> None:
        if not self._bucket_ready:
            if not self.client.bucket_exists(self.bucket):
                self.client.make_bucket(self.bucket)
                log.info("S3: создан бакет %s", self.bucket)
            self._bucket_ready = True

    def put(self, key: str, data: bytes, content_type: str = "application/json") -> None:
        def run():
            self._ensure_bucket()
            self.client.put_object(self.bucket, key, io.BytesIO(data), length=len(data),
                                   content_type=content_type)
        with_retry(run, what=f"S3 put {key}", retriable=self.retriable, attempts=config.S3_RETRY_ATTEMPTS,
                   base=config.PG_RETRY_BASE, cap=config.PG_RETRY_MAX)

    def get(self, key: str) -> bytes:
        resp = self.client.get_object(self.bucket, key)
        try:
            return resp.read()
        finally:
            resp.close()
            resp.release_conn()

    def list_keys(self, prefix: str) -> list[str]:
        return [o.object_name for o in self.client.list_objects(self.bucket, prefix=prefix, recursive=True)]

    def delete(self, key: str) -> None:
        self.client.remove_object(self.bucket, key)


@dataclass
class ExportResult:
    metric_date: date
    status: str              # exported | skipped | no_data | failed
    object_key: str
    rows: int = 0
    size_bytes: int = 0
    duration_seconds: float = 0.0
    error: str | None = None


def _iso(v) -> str | None:
    return v.isoformat() if hasattr(v, "isoformat") else v


def build_document(d: date, rows: list[dict]) -> tuple[bytes, str]:
    """JSON-документ дня и хеш значений (без времени вычисления: оно меняется при каждом пересчёте)."""
    norm = sorted(
        ({"metric_name": r["metric_name"], "dimension": r["dimension"], "value": float(r["value"]),
          "computed_at": _iso(r["computed_at"])} for r in rows),
        key=lambda r: (r["metric_name"], r["dimension"]),
    )
    content_hash = hashlib.sha256(json.dumps(
        [(r["metric_name"], r["dimension"], round(r["value"], 9)) for r in norm]).encode()).hexdigest()

    summary = {r["metric_name"]: r["value"] for r in norm if r["dimension"] == ""}
    breakdowns: dict[str, dict[str, float]] = {}
    for r in norm:
        if r["dimension"] and r["metric_name"] != "top_movie_views":
            breakdowns.setdefault(r["metric_name"], {})[r["dimension"]] = r["value"]
    top = sorted((r for r in norm if r["metric_name"] == "top_movie_views"),
                 key=lambda r: (-r["value"], r["dimension"]))

    doc = {
        "format_version": FORMAT_VERSION,
        "date": d.isoformat(),
        "computed_at": max((r["computed_at"] for r in norm), default=None),
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "source": "postgresql:daily_metrics",
        "summary": summary,                     # dau, view_conversion, retention_d1, ...
        "top_movies": [{"rank": i, "movie_id": r["dimension"], "views": int(r["value"])}
                       for i, r in enumerate(top, 1)],
        "breakdowns": breakdowns,               # cohort_retention {day_0..}, device_users {TV..}
        "metrics": norm,                        # все строки: метрика, разрез, значение, computed_at
    }
    return json.dumps(doc, ensure_ascii=False, indent=2).encode("utf-8"), content_hash


class Exporter:
    def __init__(self, store, s3):
        self.store = store
        self.s3 = s3

    def export_date(self, d: date, trigger: str, force: bool = False) -> ExportResult:
        key = object_key(d)
        t0 = time.monotonic()
        log.info("Экспорт начат: дата=%s, запуск=%s, s3://%s/%s", d, trigger, config.S3_BUCKET, key)
        try:
            rows = self.store.get_metrics(d)
            if not rows:
                log.info("Экспорт пропущен: дата=%s — в PostgreSQL нет метрик", d)
                return ExportResult(d, "no_data", key, duration_seconds=time.monotonic() - t0)
            data, content_hash = build_document(d, rows)
            if not force:
                prev = self.store.get_export(d)
                if prev and prev["status"] == "success" and prev["content_hash"] == content_hash:
                    return ExportResult(d, "skipped", key, len(rows), duration_seconds=time.monotonic() - t0)
            self.s3.put(key, data)
            self.store.record_export(d, key, "success", rows=len(rows), size=len(data),
                                     content_hash=content_hash)
            res = ExportResult(d, "exported", key, len(rows), len(data), time.monotonic() - t0)
            log.info("Экспорт завершён: дата=%s, %s метрик, %s байт за %.2f сек", d, res.rows,
                     res.size_bytes, res.duration_seconds)
            return res
        except Exception as exc:  # noqa: BLE001 — PG или S3 недоступны: логируем, повторим в следующий запуск
            err = repr(exc)
            log.error("Экспорт не удался: дата=%s: %s — повтор при следующем запуске", d, err)
            try:
                self.store.record_export(d, key, "failed", error=err[:2000])
            except Exception as exc2:  # noqa: BLE001 — PostgreSQL тоже может быть недоступен
                log.error("не удалось записать ошибку экспорта в s3_exports: %r", exc2)
            return ExportResult(d, "failed", key, duration_seconds=time.monotonic() - t0, error=err)

    def scheduled_dates(self, today: date | None = None) -> list[date]:
        today = today or datetime.now(timezone.utc).date()
        return [today - timedelta(days=i) for i in range(config.EXPORT_LOOKBACK_DAYS, 0, -1)]

    def run_scheduled(self) -> list[ExportResult]:
        t0 = time.monotonic()
        results = [self.export_date(d, "schedule") for d in self.scheduled_dates()]
        by_status: dict[str, int] = {}
        for r in results:
            by_status[r.status] = by_status.get(r.status, 0) + 1
        log.info("Плановый экспорт завершён: %s за %.2f сек", by_status, time.monotonic() - t0)
        return results
