"""Плановый пересчёт: каждые AGG_INTERVAL_SECONDS (или по AGG_CRON) пересчитывает
сегодня и AGG_LOOKBACK_DAYS прошедших дней."""
from __future__ import annotations

import logging
import threading
import time
from datetime import date, datetime, timedelta, timezone

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from app import config
from app.pipeline import CycleResult, run_cycle

log = logging.getLogger("scheduler")


class Aggregator:
    """Общая точка входа для расписания и HTTP: циклы не идут параллельно (lock)."""

    def __init__(self, ch, store):
        self.ch = ch
        self.store = store
        self._lock = threading.Lock()
        self.last_scheduled_at: datetime | None = None

    def recompute(self, d: date, trigger: str) -> CycleResult:
        with self._lock:
            return run_cycle(self.ch, self.store, d, trigger)

    def scheduled_dates(self, today: date | None = None) -> list[date]:
        today = today or datetime.now(timezone.utc).date()
        return [today - timedelta(days=i) for i in range(config.AGG_LOOKBACK_DAYS, -1, -1)]

    def run_scheduled(self) -> None:
        dates = self.scheduled_dates()
        t0 = time.monotonic()
        log.info("Плановый пересчёт начат: %s дат (%s … %s)", len(dates), dates[0], dates[-1])
        results = [self.recompute(d, "schedule") for d in dates]
        failed = [r.metric_date.isoformat() for r in results if r.status != "success"]
        log.info("Плановый пересчёт завершён: обработано %s событий, записано %s метрик, "
                 "ошибок %s%s, за %.2f сек",
                 sum(r.events_processed for r in results), sum(r.rows_written for r in results),
                 len(failed), f" ({', '.join(failed)})" if failed else "", time.monotonic() - t0)
        self.last_scheduled_at = datetime.now(timezone.utc)


def make_trigger(cron: str | None = None, interval: int | None = None):
    cron = config.AGG_CRON if cron is None else cron
    interval = config.AGG_INTERVAL_SECONDS if interval is None else interval
    if cron:
        return CronTrigger.from_crontab(cron, timezone="UTC"), f"cron '{cron}'"
    return IntervalTrigger(seconds=interval), f"каждые {interval} c"


def start_scheduler(aggregator: Aggregator, exporter=None) -> BackgroundScheduler:
    sched = BackgroundScheduler(timezone="UTC")
    trigger, desc = make_trigger()
    extra = {}
    if config.AGG_RUN_ON_START:  # первый цикл сразу после старта, дальше — по расписанию
        extra["next_run_time"] = datetime.now(timezone.utc)
    sched.add_job(aggregator.run_scheduled, trigger, id="aggregate",
                  max_instances=1, coalesce=True, **extra)
    log.info("Планировщик: пересчёт %s, сегодня + %s прошедших дней", desc, config.AGG_LOOKBACK_DAYS)
    if exporter is not None and config.EXPORT_ENABLED:
        etrigger, edesc = make_trigger(config.EXPORT_CRON, config.EXPORT_INTERVAL_SECONDS)
        sched.add_job(exporter.run_scheduled, etrigger, id="export", max_instances=1, coalesce=True,
                      next_run_time=datetime.now(timezone.utc) + timedelta(seconds=30))
        log.info("Планировщик: экспорт в S3 %s, завершённые дни за последние %s", edesc,
                 config.EXPORT_LOOKBACK_DAYS)
    sched.start()
    log.info("Планировщик запущен")
    return sched
