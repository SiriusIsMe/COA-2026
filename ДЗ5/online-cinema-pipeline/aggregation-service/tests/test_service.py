"""Расписание и HTTP API без ClickHouse/PostgreSQL (подмены)."""
from datetime import date, datetime, timezone

import pytest
from fastapi.testclient import TestClient

from app import config, scheduler
from app.main import app
from app.metrics import DayResult, MetricRow
from app.pipeline import CycleResult
from app.scheduler import Aggregator


def test_scheduled_dates_cover_today_and_lookback(monkeypatch):
    monkeypatch.setattr(config, "AGG_LOOKBACK_DAYS", 8)
    dates = Aggregator(None, None).scheduled_dates(today=date(2026, 11, 25))
    assert dates[0] == date(2026, 11, 17) and dates[-1] == date(2026, 11, 25) and len(dates) == 9


def test_trigger_from_env(monkeypatch):
    monkeypatch.setattr(config, "AGG_CRON", "")
    monkeypatch.setattr(config, "AGG_INTERVAL_SECONDS", 42)
    trig, desc = scheduler.make_trigger()
    assert trig.interval.total_seconds() == 42
    monkeypatch.setattr(config, "AGG_CRON", "*/5 * * * *")
    trig, desc = scheduler.make_trigger()
    assert "cron" in desc


def test_run_scheduled_recomputes_all_dates(monkeypatch):
    monkeypatch.setattr(config, "AGG_LOOKBACK_DAYS", 2)
    calls = []
    agg = Aggregator(None, None)
    monkeypatch.setattr(agg, "recompute", lambda d, trigger: calls.append((d, trigger)) or
                        CycleResult(d, "success", 10, 5, 0.1))
    agg.run_scheduled()
    assert len(calls) == 3 and all(t == "schedule" for _, t in calls)


class FakeAggregator:
    def __init__(self):
        self.calls = []

    def recompute(self, d, trigger):
        self.calls.append((d, trigger))
        day = DayResult(d, datetime.now(timezone.utc), [
            MetricRow(d, "dau", "", 170.0), MetricRow(d, "top_movie_views", "movie_0001", 9.0),
            MetricRow(d, "cohort_retention", "day_0", 1.0), MetricRow(d, "device_users", "TV", 50.0)])
        return CycleResult(d, "success", 1234, 2, 0.05, day=day)


@pytest.fixture
def client():
    fake = FakeAggregator()
    app.state.aggregator = fake
    yield TestClient(app), fake  # без `with` — lifespan (подключения к БД, планировщик) не стартует


def test_manual_recompute_endpoint(client):
    c, fake = client
    r = c.post("/recompute", params={"date": "2026-11-20"})
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "success" and body["events_processed"] == 1234
    assert body["metrics"]["dau"] == 170.0
    assert body["top_movies"] == [{"movie_id": "movie_0001", "views": 9}]
    assert body["breakdowns"] == {"cohort_retention": {"day_0": 1.0}, "device_users": {"TV": 50.0}}
    assert "cohort_retention" not in body["metrics"]
    assert fake.calls == [(date(2026, 11, 20), "manual")]


def test_recompute_requires_valid_date(client):
    c, _ = client
    assert c.post("/recompute").status_code == 422
    assert c.post("/recompute", params={"date": "20-11-2026"}).status_code == 422
