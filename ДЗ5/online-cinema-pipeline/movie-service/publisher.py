"""Публикация событий в Kafka.

- валидация по Avro-схеме из Schema Registry (п.1) перед отправкой;
- ключ сообщения = user_id (порядок событий пользователя сохраняется);
- acks=all + идемпотентный producer;
- повторные попытки с exponential backoff и jitter на уровне приложения
  (поверх внутренних ретраев librdkafka);
- логирование каждой публикации: event_id, event_type, timestamp, partition, offset.
"""
from __future__ import annotations

import json
import logging
import random
import threading
import time
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FutureTimeout
from dataclasses import dataclass
from typing import Callable

from confluent_kafka import KafkaException, Producer

import config
from models import MovieEvent

log = logging.getLogger("publisher")


class SchemaValidationError(ValueError):
    """Событие не соответствует Avro-схеме."""


class PublishError(RuntimeError):
    """Событие не удалось опубликовать после всех попыток."""


@dataclass(frozen=True)
class DeliveryResult:
    event_id: str
    partition: int
    offset: int
    attempts: int


def is_retriable(exc: BaseException) -> bool:
    if isinstance(exc, (BufferError, TimeoutError, FutureTimeout, ConnectionError)):
        return True  # очередь producer'а переполнена / нет ответа / сеть
    if isinstance(exc, KafkaException) and exc.args:
        err = exc.args[0]
        return bool(getattr(err, "retriable", lambda: False)())
    return False


def backoff_delay(attempt: int, base: float, cap: float, rng: random.Random | None = None) -> float:
    """Экспоненциальная задержка с «full jitter»: случайное значение в [0, min(cap, base*2^(attempt-1))]."""
    rng = rng or random
    return rng.uniform(0, min(cap, base * (2 ** (attempt - 1))))


class EventPublisher:
    def __init__(
        self,
        producer,
        serialize: Callable[[dict], bytes],
        validate: Callable[[dict], None],
        topic: str = config.TOPIC,
        max_attempts: int = config.PUBLISH_MAX_ATTEMPTS,
        backoff_base: float = config.PUBLISH_BACKOFF_BASE,
        backoff_max: float = config.PUBLISH_BACKOFF_MAX,
        delivery_timeout: float = config.DELIVERY_TIMEOUT_MS / 1000 + 5,
        sleep: Callable[[float], None] = time.sleep,
        start_poll_thread: bool = True,
    ):
        self._producer = producer
        self._serialize = serialize
        self._validate = validate
        self.topic = topic
        self.max_attempts = max_attempts
        self.backoff_base = backoff_base
        self.backoff_max = backoff_max
        self.delivery_timeout = delivery_timeout
        self._sleep = sleep
        self._stop = threading.Event()
        self._poller = None
        if start_poll_thread:
            # Отдельный поток обслуживает delivery-callback'и producer'а
            self._poller = threading.Thread(target=self._poll_loop, name="kafka-poll", daemon=True)
            self._poller.start()

    # ---------- фабрика с настоящими Kafka/Schema Registry ----------
    @classmethod
    def from_config(cls) -> "EventPublisher":
        from fastavro import parse_schema
        from fastavro.validation import ValidationError, validate
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroSerializer
        from confluent_kafka.serialization import MessageField, SerializationContext

        schema_str = config.load_schema_str()
        parsed = parse_schema(json.loads(schema_str))

        def validate_avro(record: dict) -> None:
            try:
                validate(record, parsed, raise_errors=True)
            except ValidationError as e:
                raise SchemaValidationError(str(e)) from e

        sr = SchemaRegistryClient({"url": config.SCHEMA_REGISTRY_URL})
        # Схему регистрирует kafka-init, producer только находит её ID
        avro = AvroSerializer(sr, schema_str, conf={"auto.register.schemas": False})
        ctx = SerializationContext(config.TOPIC, MessageField.VALUE)

        producer = Producer({
            "bootstrap.servers": config.BOOTSTRAP_SERVERS,
            "acks": "all",                       # запись подтверждают все ISR-реплики
            "enable.idempotence": True,          # без дублей при внутренних ретраях
            "delivery.timeout.ms": config.DELIVERY_TIMEOUT_MS,
            "retry.backoff.ms": 100,             # внутренние ретраи librdkafka тоже
            "retry.backoff.max.ms": 1000,        # с экспоненциальной задержкой
            "linger.ms": 10,
            "compression.type": "lz4",
        })
        return cls(producer, lambda rec: avro(rec, ctx), validate_avro)

    # ---------- публикация ----------
    def publish(self, event: MovieEvent) -> DeliveryResult:
        record = event.to_avro()
        self._validate(record)  # SchemaValidationError — не ретраим, это ошибка клиента

        key = event.user_id.encode("utf-8")
        value: bytes | None = None
        last_exc: BaseException | None = None

        for attempt in range(1, self.max_attempts + 1):
            try:
                if value is None:  # сериализация обращается к Schema Registry — тоже может упасть
                    value = self._serialize(record)
                partition, offset = self._produce_and_wait(key, value)
                log.info(
                    "published event_id=%s event_type=%s timestamp=%s partition=%s offset=%s attempt=%s",
                    record["event_id"], record["event_type"], event.timestamp.isoformat(),
                    partition, offset, attempt,
                )
                return DeliveryResult(record["event_id"], partition, offset, attempt)
            except SchemaValidationError:
                raise
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                retriable = is_retriable(exc) or value is None  # ошибки Registry тоже ретраим
                if not retriable or attempt == self.max_attempts:
                    break
                delay = backoff_delay(attempt, self.backoff_base, self.backoff_max)
                log.warning(
                    "publish failed event_id=%s attempt=%s/%s error=%r, retry in %.2fs",
                    record["event_id"], attempt, self.max_attempts, exc, delay,
                )
                self._sleep(delay)

        log.error("publish gave up event_id=%s event_type=%s error=%r",
                  record["event_id"], record["event_type"], last_exc)
        raise PublishError(f"failed to publish {record['event_id']}: {last_exc}") from last_exc

    def publish_batch(self, events: list[MovieEvent]) -> dict:
        """Массовая отправка (backfill): не ждём подтверждения каждого сообщения,
        а отправляем пачкой и ждём всех в конце (flush). Ошибки доставки считаются."""
        stats = {"sent": 0, "delivered": 0, "failed": 0}

        def on_delivery(err, msg):
            if err is None:
                stats["delivered"] += 1
            else:
                stats["failed"] += 1
                log.error("batch delivery failed: %s", err)

        for event in events:
            record = event.to_avro()
            self._validate(record)
            value = self._serialize(record)
            for attempt in range(1, self.max_attempts + 1):
                try:
                    self._producer.produce(self.topic, key=event.user_id.encode("utf-8"),
                                           value=value, on_delivery=on_delivery)
                    break
                except BufferError:  # очередь переполнена — даём ей разгрузиться
                    self._producer.poll(backoff_delay(attempt, self.backoff_base, self.backoff_max))
            else:
                stats["failed"] += 1
                continue
            stats["sent"] += 1
            log.debug("queued event_id=%s event_type=%s timestamp=%s",
                      record["event_id"], record["event_type"], event.timestamp.isoformat())
        self._producer.flush(60)
        log.info("batch published: %s", stats)
        return stats

    def _produce_and_wait(self, key: bytes, value: bytes) -> tuple[int, int]:
        fut: Future = Future()

        def on_delivery(err, msg):
            if err is not None:
                fut.set_exception(KafkaException(err))
            else:
                fut.set_result((msg.partition(), msg.offset()))

        self._producer.produce(self.topic, key=key, value=value, on_delivery=on_delivery)
        if self._poller is None:
            self._producer.poll(0)
        return fut.result(timeout=self.delivery_timeout)

    def _poll_loop(self) -> None:
        while not self._stop.is_set():
            self._producer.poll(0.1)

    def close(self) -> None:
        self._stop.set()
        if self._poller:
            self._poller.join(timeout=2)
        self._producer.flush(10)
