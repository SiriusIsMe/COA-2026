"""Сквозной тест: продюсер -> Kafka (Avro + Schema Registry) -> ClickHouse Kafka Engine -> MV -> MergeTree.

Запуск одной командой (стек поднимается сам):
    docker compose run --rm integration-tests
"""
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from confluent_kafka import Producer

import config
from models import DeviceType, EventType, MovieEvent

pytestmark = pytest.mark.integration

ROW_SQL = """
SELECT
    event_id, user_id, movie_id, event_type, timestamp, device_type,
    session_id, progress_seconds, event_date, kafka_partition, kafka_offset
FROM cinema.movie_events FINAL
WHERE event_id = {eid:UUID}
"""
# В JSONEachRow ClickHouse сам отдаёт UUID, Enum, DateTime64 и Date строками —
# преобразования не нужны (а псевдонимы с именами колонок ломают WHERE).


def ms_now(offset_minutes: int = 1) -> datetime:
    """Текущее время UTC, обрезанное до миллисекунд (точность timestamp-millis)."""
    t = datetime.now(timezone.utc) - timedelta(minutes=offset_minutes)
    return t.replace(microsecond=t.microsecond // 1000 * 1000)


def ch_ts(t: datetime) -> str:
    return t.strftime("%Y-%m-%d %H:%M:%S.") + f"{t.microsecond // 1000:03d}"


def make_event(run_id: str, **over) -> MovieEvent:
    data = dict(
        user_id=f"{run_id}_user", movie_id=f"{run_id}_movie", event_type=EventType.VIEW_PAUSED,
        timestamp=ms_now(), device_type=DeviceType.TABLET, session_id=f"{run_id}_session",
        progress_seconds=4321,
    )
    data.update(over)
    return MovieEvent(**data)


def expected_row(e: MovieEvent) -> dict:
    return {
        "event_id": str(e.event_id),
        "user_id": e.user_id,
        "movie_id": e.movie_id,
        "event_type": e.event_type.value,
        "timestamp": ch_ts(e.timestamp),
        "device_type": e.device_type.value,
        "session_id": e.session_id,
        "progress_seconds": e.progress_seconds,
        "event_date": e.timestamp.date().isoformat(),
    }


def test_event_from_producer_lands_in_clickhouse(publisher, clickhouse, run_id):
    event = make_event(run_id)
    result = publisher.publish(event)

    [row] = clickhouse.wait_rows(ROW_SQL, eid=str(event.event_id))

    assert {k: row[k] for k in expected_row(event)} == expected_row(event)
    # служебные поля совпадают с тем, что подтвердил брокер
    assert row["kafka_partition"] == result.partition
    assert row["kafka_offset"] == result.offset


def test_nullable_progress_for_liked(publisher, clickhouse, run_id):
    event = make_event(run_id, event_type=EventType.LIKED, progress_seconds=None,
                       device_type=DeviceType.MOBILE)
    publisher.publish(event)

    [row] = clickhouse.wait_rows(ROW_SQL, eid=str(event.event_id))

    assert row["progress_seconds"] is None
    assert row["event_type"] == "LIKED" and row["device_type"] == "MOBILE"


def test_event_via_http_api(api, clickhouse, run_id):
    payload = {
        "user_id": f"{run_id}_api_user", "movie_id": "movie_0001", "event_type": "SEARCHED",
        "device_type": "DESKTOP", "session_id": f"{run_id}_api_session",
        "timestamp": ms_now().isoformat(),
    }
    r = api.post("/events", json=payload)
    assert r.status_code == 201, r.text
    event_id = r.json()["event_id"]

    [row] = clickhouse.wait_rows(ROW_SQL, eid=event_id)

    assert row["user_id"] == payload["user_id"]
    assert row["event_type"] == "SEARCHED" and row["progress_seconds"] is None


def test_user_events_keep_order_in_one_partition(publisher, clickhouse, run_id):
    user, session, t0 = f"{run_id}_order_user", f"{run_id}_order_session", ms_now(10)
    steps = [(EventType.VIEW_STARTED, 0), (EventType.VIEW_PAUSED, 600),
             (EventType.VIEW_RESUMED, 600), (EventType.VIEW_FINISHED, 5400)]
    events = [make_event(run_id, user_id=user, session_id=session, event_type=t,
                         progress_seconds=p, timestamp=t0 + timedelta(seconds=i * 30))
              for i, (t, p) in enumerate(steps)]
    for e in events:
        publisher.publish(e)

    rows = clickhouse.wait_rows(
        "SELECT event_type, progress_seconds, kafka_partition, kafka_offset "
        "FROM cinema.movie_events FINAL WHERE session_id = {s:String} ORDER BY kafka_offset",
        expected=len(events), s=session)

    assert len({r["kafka_partition"] for r in rows}) == 1        # ключ user_id -> одна партиция
    assert [r["event_type"] for r in rows] == [t.value for t, _ in steps]  # порядок сохранён
    assert [r["progress_seconds"] for r in rows] == [p for _, p in steps]


def test_broken_message_goes_to_errors_table(publisher, clickhouse, run_id):
    marker = f"{run_id}_broken"
    producer = Producer({"bootstrap.servers": config.BOOTSTRAP_SERVERS, "acks": "all"})
    producer.produce(config.TOPIC, key=marker.encode(), value=f"not-avro {marker}".encode())
    assert producer.flush(10) == 0

    [row] = clickhouse.wait_rows(
        "SELECT raw_message, error FROM cinema.movie_events_errors "
        "WHERE position(raw_message, {m:String}) > 0", m=marker)

    assert row["error"]                                           # причина ошибки записана
    # поток не остановился: следующее корректное событие тоже доходит
    event = make_event(run_id, user_id=f"{run_id}_after_broken")
    publisher.publish(event)
    clickhouse.wait_rows(ROW_SQL, eid=str(event.event_id))

