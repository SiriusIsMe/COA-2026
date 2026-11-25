"""Проверка retry/backoff и логирования без настоящей Kafka: producer подменён фейком."""
import logging
import random

import pytest
from confluent_kafka import KafkaError

from models import MovieEvent
from publisher import (
    EventPublisher, PublishError, SchemaValidationError, backoff_delay, is_retriable,
)


class FakeMsg:
    def __init__(self, partition, offset):
        self._p, self._o = partition, offset

    def partition(self):
        return self._p

    def offset(self):
        return self._o


class FakeProducer:
    """Первые `fail_times` отправок завершаются ошибкой `error`, дальше — успех."""

    def __init__(self, fail_times=0, error=None, raise_buffer=False):
        self.fail_times = fail_times
        self.error = error
        self.raise_buffer = raise_buffer
        self.calls = []

    def produce(self, topic, key, value, on_delivery):
        self.calls.append((topic, key, value))
        if len(self.calls) <= self.fail_times:
            if self.raise_buffer:
                raise BufferError("queue full")
            on_delivery(self.error, None)
        else:
            on_delivery(None, FakeMsg(1, 100 + len(self.calls)))

    def poll(self, timeout=0):
        return 0

    def flush(self, timeout=0):
        return 0


def make_publisher(producer, validate=lambda r: None, max_attempts=5):
    sleeps = []
    pub = EventPublisher(
        producer, serialize=lambda rec: b"avro", validate=validate, topic="movie-events",
        max_attempts=max_attempts, backoff_base=0.5, backoff_max=8,
        sleep=sleeps.append, start_poll_thread=False,
    )
    return pub, sleeps


def event(**over):
    data = dict(user_id="user_00042", movie_id="movie_0001", event_type="VIEW_PAUSED",
                device_type="MOBILE", session_id="s", progress_seconds=120)
    data.update(over)
    return MovieEvent(**data)


RETRIABLE = KafkaError(KafkaError._MSG_TIMED_OUT, "timed out", retriable=True)
FATAL = KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED, "denied", retriable=False)


def test_success_first_try_uses_user_id_key(caplog):
    producer = FakeProducer()
    pub, sleeps = make_publisher(producer)
    e = event()
    with caplog.at_level(logging.INFO, logger="publisher"):
        res = pub.publish(e)
    assert res.attempts == 1 and res.partition == 1 and res.event_id == str(e.event_id)
    assert producer.calls[0][1] == b"user_00042"          # ключ = user_id
    assert sleeps == []
    line = caplog.records[-1].getMessage()                # логируем факт публикации
    assert f"event_id={e.event_id}" in line and "event_type=VIEW_PAUSED" in line and "timestamp=" in line


def test_retries_with_exponential_backoff(monkeypatch):
    monkeypatch.setattr(random, "uniform", lambda a, b: b)  # без jitter — видна экспонента
    producer = FakeProducer(fail_times=3, error=RETRIABLE)
    pub, sleeps = make_publisher(producer)
    res = pub.publish(event())
    assert res.attempts == 4
    assert sleeps == [0.5, 1.0, 2.0]


def test_buffer_error_is_retried():
    pub, sleeps = make_publisher(FakeProducer(fail_times=2, raise_buffer=True))
    assert pub.publish(event()).attempts == 3 and len(sleeps) == 2


def test_gives_up_after_max_attempts():
    producer = FakeProducer(fail_times=100, error=RETRIABLE)
    pub, sleeps = make_publisher(producer, max_attempts=4)
    with pytest.raises(PublishError):
        pub.publish(event())
    assert len(producer.calls) == 4 and len(sleeps) == 3


def test_non_retriable_error_fails_fast():
    producer = FakeProducer(fail_times=1, error=FATAL)
    pub, sleeps = make_publisher(producer)
    with pytest.raises(PublishError):
        pub.publish(event())
    assert len(producer.calls) == 1 and sleeps == []


def test_schema_validation_error_not_sent():
    def bad(record):
        raise SchemaValidationError("bad")
    producer = FakeProducer()
    pub, _ = make_publisher(producer, validate=bad)
    with pytest.raises(SchemaValidationError):
        pub.publish(event())
    assert producer.calls == []


def test_backoff_is_capped():
    assert backoff_delay(20, 0.5, 8, random.Random(0)) <= 8


def test_is_retriable():
    from confluent_kafka import KafkaException
    assert is_retriable(KafkaException(RETRIABLE))
    assert not is_retriable(KafkaException(FATAL))
    assert is_retriable(BufferError())
    assert not is_retriable(ValueError())
