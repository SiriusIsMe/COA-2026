"""Экспорт в S3 без PostgreSQL/MinIO: хранилища подменены."""
import json
import logging
from datetime import date, datetime, timezone

from app import config
from app.exporter import Exporter, build_document, object_key

D = date(2026, 11, 24)
TS = datetime(2026, 11, 25, 10, 0, tzinfo=timezone.utc)


def rows(dau=234.0):
    return [
        {"metric_name": "dau", "dimension": "", "value": dau, "computed_at": TS, "updated_at": TS},
        {"metric_name": "view_conversion", "dimension": "", "value": 0.7, "computed_at": TS, "updated_at": TS},
        {"metric_name": "retention_d1", "dimension": "", "value": 0.4, "computed_at": TS, "updated_at": TS},
        {"metric_name": "top_movie_views", "dimension": "movie_0002", "value": 5.0, "computed_at": TS,
         "updated_at": TS},
        {"metric_name": "top_movie_views", "dimension": "movie_0001", "value": 6.0, "computed_at": TS,
         "updated_at": TS},
        {"metric_name": "cohort_retention", "dimension": "day_0", "value": 1.0, "computed_at": TS,
         "updated_at": TS},
    ]


class FakeStore:
    def __init__(self, data=None, fail_read=False):
        self.data = data if data is not None else {D: rows()}
        self.fail_read = fail_read
        self.exports = {}

    def get_metrics(self, d):
        if self.fail_read:
            raise ConnectionError("postgres down")
        return self.data.get(d, [])

    def get_export(self, d):
        return self.exports.get(d)

    def record_export(self, d, key, status, rows=0, size=0, content_hash=None, error=None):
        prev = self.exports.get(d, {})
        self.exports[d] = {"status": status, "object_key": key,
                           "content_hash": content_hash if status == "success" else prev.get("content_hash"),
                           "error": error}


class FakeS3:
    def __init__(self, fail=False):
        self.objects, self.fail, self.puts = {}, fail, 0

    def put(self, key, data):
        if self.fail:
            raise ConnectionError("s3 down")
        self.puts += 1
        self.objects[key] = data  # тот же ключ -> перезапись


def test_object_key_partitioned_by_date():
    assert object_key(D) == f"{config.S3_PREFIX}/2026-11-24/aggregates.json"


def test_document_contains_all_metrics_with_computed_at():
    data, _ = build_document(D, rows())
    doc = json.loads(data)
    assert doc["date"] == "2026-11-24" and doc["computed_at"] == TS.isoformat()
    assert doc["summary"] == {"dau": 234.0, "view_conversion": 0.7, "retention_d1": 0.4}
    assert doc["top_movies"][0] == {"rank": 1, "movie_id": "movie_0001", "views": 6}
    assert doc["breakdowns"] == {"cohort_retention": {"day_0": 1.0}}
    assert len(doc["metrics"]) == 6 and all(m["computed_at"] for m in doc["metrics"])


def test_hash_ignores_computation_time_but_tracks_values():
    later = [dict(r, computed_at=datetime(2026, 11, 26, tzinfo=timezone.utc)) for r in rows()]
    assert build_document(D, rows())[1] == build_document(D, later)[1]
    assert build_document(D, rows())[1] != build_document(D, rows(dau=235.0))[1]


def test_reexport_overwrites_same_key_and_skips_unchanged():
    store, s3 = FakeStore(), FakeS3()
    ex = Exporter(store, s3)
    assert ex.export_date(D, "schedule").status == "exported"
    assert ex.export_date(D, "schedule").status == "skipped"      # не изменилось — не трогаем
    store.data[D] = rows(dau=240.0)
    assert ex.export_date(D, "schedule").status == "exported"     # изменилось — перезапись
    assert list(s3.objects) == [object_key(D)] and s3.puts == 2   # один объект, без дубликатов
    assert json.loads(s3.objects[object_key(D)])["summary"]["dau"] == 240.0
    assert ex.export_date(D, "manual", force=True).status == "exported"  # ручной — всегда


def test_s3_down_logged_recorded_and_retried_next_run(caplog):
    store, s3 = FakeStore(), FakeS3(fail=True)
    ex = Exporter(store, s3)
    with caplog.at_level(logging.ERROR, logger="exporter"):
        res = ex.export_date(D, "schedule")
    assert res.status == "failed" and "s3 down" in res.error
    assert store.exports[D]["status"] == "failed"
    assert any("Экспорт не удался" in r.getMessage() for r in caplog.records)
    s3.fail = False                                                # S3 поднялся
    assert ex.export_date(D, "schedule").status == "exported"      # следующий запуск выгружает
    assert store.exports[D]["status"] == "success"


def test_postgres_down_does_not_crash():
    ex = Exporter(FakeStore(fail_read=True), FakeS3())
    res = ex.export_date(D, "schedule")
    assert res.status == "failed" and "postgres down" in res.error


def test_scheduled_exports_only_finished_days(monkeypatch):
    monkeypatch.setattr(config, "EXPORT_LOOKBACK_DAYS", 3)
    dates = Exporter(None, None).scheduled_dates(today=date(2026, 11, 25))
    assert dates == [date(2026, 11, 22), date(2026, 11, 23), date(2026, 11, 24)]   # без сегодня


def test_no_data_is_not_uploaded():
    s3 = FakeS3()
    assert Exporter(FakeStore(data={}), s3).export_date(D, "schedule").status == "no_data"
    assert s3.objects == {}
