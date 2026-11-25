"""HTTP API без Kafka: publisher подменяется через dependency_overrides, lifespan не запускается."""
import uuid

import pytest
from fastapi.testclient import TestClient

from main import app, get_publisher
from publisher import DeliveryResult, PublishError, SchemaValidationError


class StubPublisher:
    def __init__(self, error=None):
        self.error = error
        self.events = []

    def publish(self, event):
        if self.error:
            raise self.error
        self.events.append(event)
        return DeliveryResult(str(event.event_id), partition=2, offset=7, attempts=1)


@pytest.fixture
def client_with():
    def make(publisher):
        app.dependency_overrides[get_publisher] = lambda: publisher
        return TestClient(app)  # без `with` — lifespan (подключение к Kafka) не стартует
    yield make
    app.dependency_overrides.clear()


VALID = {
    "user_id": "user_00001", "movie_id": "movie_0001", "event_type": "VIEW_STARTED",
    "timestamp": "2026-11-25T10:00:00Z", "device_type": "TV", "session_id": "s-1",
    "progress_seconds": 0,
}


def test_publish_returns_event_id(client_with):
    pub = StubPublisher()
    r = client_with(pub).post("/events", json=VALID)
    assert r.status_code == 201
    body = r.json()
    uuid.UUID(body["event_id"])
    assert body["partition"] == 2 and body["offset"] == 7
    assert str(pub.events[0].event_id) == body["event_id"]


def test_client_event_id_is_kept(client_with):
    eid = str(uuid.uuid4())
    r = client_with(StubPublisher()).post("/events", json={**VALID, "event_id": eid})
    assert r.status_code == 201 and r.json()["event_id"] == eid


def test_validation_error(client_with):
    pub = StubPublisher()
    r = client_with(pub).post("/events", json={**VALID, "event_type": "WATCHED"})
    assert r.status_code == 422
    assert pub.events == []


def test_missing_progress_for_view_event(client_with):
    body = {k: v for k, v in VALID.items() if k != "progress_seconds"}
    assert client_with(StubPublisher()).post("/events", json=body).status_code == 422


def test_schema_error_is_400(client_with):
    r = client_with(StubPublisher(SchemaValidationError("x"))).post("/events", json=VALID)
    assert r.status_code == 400


def test_kafka_down_is_503(client_with):
    r = client_with(StubPublisher(PublishError("down"))).post("/events", json=VALID)
    assert r.status_code == 503
