"""Сквозной тест экспорта: ClickHouse -> /recompute -> PostgreSQL -> /export -> S3.

Запуск: docker compose run --rm aggregation-integration-tests
"""
import json
import uuid

import pytest

from app.exporter import S3Storage, object_key
from tests.integration.test_aggregation import build_events, insert_raw

pytestmark = pytest.mark.integration


def test_export_writes_partitioned_file_and_overwrites(api, ch, isolated):
    d, p = isolated
    events = build_events(d, p)
    insert_raw(ch, events)
    api.post("/recompute", params={"date": d.isoformat()}).raise_for_status()

    r = api.post("/export", params={"date": d.isoformat()})
    assert r.status_code == 200, r.text
    assert r.json()["object"] == f"s3://movie-analytics/daily/{d.isoformat()}/aggregates.json"

    s3 = S3Storage()
    doc = json.loads(s3.get(object_key(d)))
    assert doc["date"] == d.isoformat() and doc["computed_at"]
    assert doc["summary"]["dau"] == 3
    assert doc["summary"]["view_conversion"] == pytest.approx(2 / 3)
    assert doc["summary"]["retention_d7"] == pytest.approx(2 / 3)
    assert doc["breakdowns"]["cohort_retention"]["day_0"] == 1
    assert doc["top_movies"][0] == {"rank": 1, "movie_id": f"{p}_m1", "views": 2}
    assert all(m["computed_at"] for m in doc["metrics"])

    # данные изменились -> пересчёт -> повторный экспорт перезаписывает тот же объект
    insert_raw(ch, [dict(events[5], event_id=str(uuid.uuid4()), user_id=f"{p}_u4", session_id=f"{p}_s7")])
    api.post("/recompute", params={"date": d.isoformat()}).raise_for_status()
    api.post("/export", params={"date": d.isoformat()}).raise_for_status()

    assert s3.list_keys(f"daily/{d.isoformat()}/") == [object_key(d)]   # один файл, без дубликатов
    assert json.loads(s3.get(object_key(d)))["summary"]["dau"] == 4

    journal = [e for e in api.get("/exports", params={"limit": 500}).json() if e["metric_date"] == d.isoformat()]
    assert journal and journal[0]["status"] == "success"


def test_export_without_metrics_is_404(api, isolated):
    d, _ = isolated
    assert api.post("/export", params={"date": d.isoformat()}).status_code == 404
