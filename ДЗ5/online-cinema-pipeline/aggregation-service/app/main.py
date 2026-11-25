"""Aggregation Service: расписание + HTTP API для ручного пересчёта."""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import date

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from app import config
from app.cli import setup_logging
from app.clickhouse import ClickHouse
from app.exporter import Exporter, S3Storage
from app.scheduler import Aggregator, start_scheduler
from app.storage import PostgresStore

setup_logging()
log = logging.getLogger("aggregation-service")


@asynccontextmanager
async def lifespan(app: FastAPI):
    store = PostgresStore()
    store.migrate()                       # миграции PostgreSQL при старте (с retry)
    ch = ClickHouse()
    ch.ping()
    aggregator = Aggregator(ch, store)
    exporter = Exporter(store, S3Storage())   # S3 может быть недоступен на старте — это не ошибка запуска
    app.state.aggregator = aggregator
    app.state.exporter = exporter
    app.state.scheduler = start_scheduler(aggregator, exporter)
    yield
    app.state.scheduler.shutdown(wait=False)
    ch.close()


app = FastAPI(title="Aggregation Service", version="1.0.0", lifespan=lifespan)


def aggregator(request: Request) -> Aggregator:
    return request.app.state.aggregator


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/recompute")
def recompute(request: Request, date_: date = Query(..., alias="date", description="YYYY-MM-DD")):
    """Ручной пересчёт метрик за дату — не дожидаясь расписания. Ответ после записи в PostgreSQL."""
    res = aggregator(request).recompute(date_, "manual")
    rows = res.day.rows if res.day else []
    breakdowns: dict[str, dict[str, float]] = {}
    for r in rows:
        if r.dimension and r.metric_name != "top_movie_views":
            breakdowns.setdefault(r.metric_name, {})[r.dimension] = r.value
    body = {
        "date": res.metric_date.isoformat(),
        "status": res.status,
        "events_processed": res.events_processed,
        "rows_written": res.rows_written,
        "duration_seconds": round(res.duration_seconds, 3),
        "error": res.error,
        # метрики без разреза
        "metrics": {r.metric_name: r.value for r in rows if r.dimension == ""},
        "top_movies": [{"movie_id": r.dimension, "views": int(r.value)}
                       for r in rows if r.metric_name == "top_movie_views"],
        # прочие метрики с разрезом: cohort_retention {day_0: …}, device_users {TV: …}
        "breakdowns": breakdowns,
    }
    return JSONResponse(body, status_code=200 if res.status == "success" else 500)


@app.get("/metrics/{metric_date}")
def get_metrics(request: Request, metric_date: date):
    """Готовые метрики за дату из PostgreSQL."""
    rows = aggregator(request).store.get_metrics(metric_date)
    if not rows:
        raise HTTPException(404, f"нет метрик за {metric_date}")
    return rows


@app.get("/runs")
def runs(request: Request, limit: int = Query(20, ge=1, le=500)):
    """Журнал последних циклов вычисления."""
    return aggregator(request).store.recent_runs(limit)


@app.get("/schedule")
def schedule(request: Request):
    job = request.app.state.scheduler.get_job("aggregate")
    agg = aggregator(request)
    return {
        "interval_seconds": None if config.AGG_CRON else config.AGG_INTERVAL_SECONDS,
        "cron": config.AGG_CRON or None,
        "lookback_days": config.AGG_LOOKBACK_DAYS,
        "next_run_at": job.next_run_time.isoformat() if job and job.next_run_time else None,
        "last_scheduled_at": agg.last_scheduled_at.isoformat() if agg.last_scheduled_at else None,
    }


@app.post("/export")
def export(request: Request, date_: date = Query(..., alias="date", description="YYYY-MM-DD")):
    """Ручная выгрузка метрик за дату из PostgreSQL в S3 (перезаписывает файл)."""
    res = request.app.state.exporter.export_date(date_, "manual", force=True)
    body = {"date": res.metric_date.isoformat(), "status": res.status,
            "object": f"s3://{config.S3_BUCKET}/{res.object_key}", "rows": res.rows,
            "size_bytes": res.size_bytes, "duration_seconds": round(res.duration_seconds, 3),
            "error": res.error}
    code = {"exported": 200, "no_data": 404, "failed": 503}.get(res.status, 200)
    return JSONResponse(body, status_code=code)


@app.get("/exports")
def exports(request: Request, limit: int = Query(20, ge=1, le=500)):
    """Журнал выгрузок в S3 (последняя попытка по каждой дате)."""
    return aggregator(request).store.recent_exports(limit)
