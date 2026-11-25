"""Сквозной тест сервиса агрегации: raw-события в ClickHouse -> POST /recompute ->
метрики в cinema.daily_metrics и в PostgreSQL. Ожидаемые значения посчитаны вручную.

Запуск: docker compose run --rm aggregation-integration-tests

Контрольный набор (день D, если не указано иное):
  u1: s1  m1  STARTED(0) PAUSED(600) RESUMED(600) FINISHED(5400) LIKED
  u2: s2  m1  STARTED(0) FINISHED(3600)
  u3: s3  m2  SEARCHED STARTED(0) PAUSED(100)                     — бросил
  D+1: u1 s4 m3 STARTED(0)
  D+7: u1 s5 m2 STARTED(0);  u2 s6 m2 STARTED(0)
Ожидаем за D:
  DAU = 3; среднее время просмотра = (5400 + 3600) / 2 = 4500;
  конверсия = 2 завершённые сессии / 3 начатые = 0.6667;
  топ: m1 = 2 просмотра, m2 = 1;
  когорта = 3, retention D1 = 1/3 (u1), D7 = 2/3 (u1, u2);
  когортная строка: day_0 = 100%, day_1 = 33%, day_2..6 = 0, day_7 = 67%; устройства: TV = 3.
"""
import json
import uuid
from datetime import datetime, time, timedelta, timezone

import pytest

pytestmark = pytest.mark.integration


def build_events(d, p):
    def ev(user, session, movie, etype, day_offset, minute, progress=None):
        day = d + timedelta(days=day_offset)
        ts = datetime.combine(day, time(10, 0), tzinfo=timezone.utc) + timedelta(minutes=minute)
        return {
            "event_id": str(uuid.uuid4()), "user_id": f"{p}_{user}", "movie_id": f"{p}_{movie}",
            "event_type": etype, "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S.000"),
            "device_type": "TV", "session_id": f"{p}_{session}", "progress_seconds": progress,
            "event_date": day.isoformat(), "kafka_partition": 0, "kafka_offset": 0,
        }

    return [
        ev("u1", "s1", "m1", "VIEW_STARTED", 0, 0, 0),
        ev("u1", "s1", "m1", "VIEW_PAUSED", 0, 10, 600),
        ev("u1", "s1", "m1", "VIEW_RESUMED", 0, 15, 600),
        ev("u1", "s1", "m1", "VIEW_FINISHED", 0, 95, 5400),
        ev("u1", "s1", "m1", "LIKED", 0, 96),
        ev("u2", "s2", "m1", "VIEW_STARTED", 0, 30, 0),
        ev("u2", "s2", "m1", "VIEW_FINISHED", 0, 90, 3600),
        ev("u3", "s3", "m2", "SEARCHED", 0, 40),
        ev("u3", "s3", "m2", "VIEW_STARTED", 0, 41, 0),
        ev("u3", "s3", "m2", "VIEW_PAUSED", 0, 43, 100),
        ev("u1", "s4", "m3", "VIEW_STARTED", 1, 0, 0),
        ev("u1", "s5", "m2", "VIEW_STARTED", 7, 0, 0),
        ev("u2", "s6", "m2", "VIEW_STARTED", 7, 5, 0),
    ]


def insert_raw(ch, events):
    body = "\n".join(json.dumps(e) for e in events).encode()
    ch.execute("INSERT INTO cinema.movie_events FORMAT JSONEachRow", content=body)


def pg_metrics(pg, d):
    rows = pg.execute("SELECT metric_name, dimension, value FROM daily_metrics WHERE metric_date = %s",
                      (d,)).fetchall()
    return {(name, dim): value for name, dim, value in rows}


def test_recompute_writes_expected_metrics(api, ch, pg, isolated):
    d, p = isolated
    insert_raw(ch, build_events(d, p))

    r = api.post("/recompute", params={"date": d.isoformat()})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "success"
    assert body["events_processed"] == 10                       # событий за день D

    m = body["metrics"]
    assert m["dau"] == 3
    assert m["avg_watch_time_seconds"] == pytest.approx(4500)
    assert m["views_started"] == 3 and m["views_finished"] == 2
    assert m["view_conversion"] == pytest.approx(2 / 3)
    assert m["retention_cohort_size"] == 3
    assert m["retention_d1"] == pytest.approx(1 / 3)
    assert m["retention_d7"] == pytest.approx(2 / 3)
    assert body["top_movies"] == [{"movie_id": f"{p}_m1", "views": 2}, {"movie_id": f"{p}_m2", "views": 1}]

    # когортный retention по дням жизни: day_0 = 100%, day_1 = u1, day_2..6 никого, day_7 = u1 + u2
    cohort = body["breakdowns"]["cohort_retention"]
    assert set(cohort) == {f"day_{n}" for n in range(8)}
    assert cohort["day_0"] == 1 and cohort["day_1"] == pytest.approx(1 / 3)
    assert all(cohort[f"day_{n}"] == 0 for n in range(2, 7))
    assert cohort["day_7"] == pytest.approx(2 / 3)
    assert body["breakdowns"]["device_users"] == {"TV": 3}

    # Материализовано в ClickHouse
    ch_rows = ch.rows("SELECT metric_name, value FROM cinema.daily_metrics FINAL "
                      "WHERE metric_date = {d:Date} AND metric_name = 'dau'", d=d.isoformat())
    assert [row["value"] for row in ch_rows] == [3]

    # Выгружено в PostgreSQL
    stored = pg_metrics(pg, d)
    assert stored[("dau", "")] == 3
    assert stored[("view_conversion", "")] == pytest.approx(2 / 3)
    assert stored[("retention_d7", "")] == pytest.approx(2 / 3)
    assert stored[("top_movie_views", f"{p}_m1")] == 2
    assert stored[("cohort_retention", "day_7")] == pytest.approx(2 / 3)
    assert len(stored) == (len(body["metrics"]) + len(body["top_movies"])
                           + sum(len(v) for v in body["breakdowns"].values()))


def test_recompute_is_idempotent_and_updates(api, ch, pg, isolated):
    d, p = isolated
    events = build_events(d, p)
    insert_raw(ch, events)

    api.post("/recompute", params={"date": d.isoformat()}).raise_for_status()
    first = pg.execute("SELECT count(*), max(updated_at) FROM daily_metrics WHERE metric_date = %s",
                       (d,)).fetchone()

    api.post("/recompute", params={"date": d.isoformat()}).raise_for_status()
    second = pg.execute("SELECT count(*), max(updated_at) FROM daily_metrics WHERE metric_date = %s",
                        (d,)).fetchone()
    assert second[0] == first[0]            # строки не задублировались
    assert second[1] > first[1]             # значения перезаписаны новым пересчётом

    # новые данные за день -> пересчёт обновляет значение, а не добавляет строку
    extra = dict(events[5], event_id=str(uuid.uuid4()), user_id=f"{p}_u4", session_id=f"{p}_s7")
    insert_raw(ch, [extra])
    api.post("/recompute", params={"date": d.isoformat()}).raise_for_status()
    stored = pg_metrics(pg, d)
    assert stored[("dau", "")] == 4
    assert pg.execute("SELECT count(*) FROM daily_metrics WHERE metric_date = %s AND metric_name = 'dau'",
                      (d,)).fetchone()[0] == 1

    runs = pg.execute("SELECT trigger, status FROM aggregation_runs WHERE metric_date = %s", (d,)).fetchall()
    assert runs == [("manual", "success")] * 3


def test_empty_day_gives_zeros(api, isolated):
    d, _ = isolated
    r = api.post("/recompute", params={"date": d.isoformat()})
    assert r.status_code == 200
    m = r.json()["metrics"]
    assert m["dau"] == 0 and m["view_conversion"] == 0 and m["avg_watch_time_seconds"] == 0
