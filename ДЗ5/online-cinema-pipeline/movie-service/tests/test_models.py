from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from models import EventType, MovieEvent


def payload(**over):
    base = {
        "user_id": "user_00001",
        "movie_id": "movie_0001",
        "event_type": "VIEW_STARTED",
        "timestamp": "2026-11-25T10:00:00Z",
        "device_type": "TV",
        "session_id": "s-1",
        "progress_seconds": 0,
    }
    base.update(over)
    return {k: v for k, v in base.items() if v is not ...}


def test_valid_event_gets_id_and_utc():
    e = MovieEvent(**payload(timestamp="2026-11-25T14:00:00+04:00"))
    assert e.event_id is not None
    assert e.timestamp == datetime(2026, 11, 25, 10, tzinfo=timezone.utc)
    avro = e.to_avro()
    assert avro["event_type"] == "VIEW_STARTED" and isinstance(avro["event_id"], str)


def test_timestamp_defaults_to_now():
    e = MovieEvent(**payload(timestamp=...))
    assert abs(e.timestamp - datetime.now(timezone.utc)) < timedelta(seconds=5)


@pytest.mark.parametrize("over", [
    {"event_type": "WATCHED"},                         # неизвестный enum
    {"device_type": "FRIDGE"},
    {"user_id": ""},                                   # пустой id
    {"progress_seconds": -1},                          # отрицательный прогресс
    {"progress_seconds": ...},                         # нет прогресса у VIEW_*
    {"event_type": "LIKED"},                           # прогресс у LIKED
    {"timestamp": "2026-11-25T10:00:00"},              # без часового пояса
    {"timestamp": (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()},  # из будущего
    {"event_id": "not-a-uuid"},
    {"extra_field": 1},                                # лишнее поле
])
def test_invalid_events_rejected(over):
    with pytest.raises(ValidationError):
        MovieEvent(**payload(**over))


def test_liked_without_progress_ok():
    e = MovieEvent(**payload(event_type="LIKED", progress_seconds=None))
    assert e.event_type is EventType.LIKED and e.progress_seconds is None
