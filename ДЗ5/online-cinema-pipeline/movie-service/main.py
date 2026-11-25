"""Movie Service — HTTP API для приёма событий + встроенный генератор синтетических событий."""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, Request, status
from pydantic import BaseModel

import config
import random

from generator import EventGenerator, generate_history
from models import MovieEvent
from publisher import EventPublisher, PublishError, SchemaValidationError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("movie-service")


@asynccontextmanager
async def lifespan(app: FastAPI):
    publisher = EventPublisher.from_config()
    generator = EventGenerator(publisher, config.EVENTS_PER_SECOND, config.GENERATOR_CONCURRENCY)
    app.state.publisher = publisher
    app.state.generator = generator
    if config.GENERATOR_ENABLED:
        generator.start()
    yield
    generator.stop()
    publisher.close()


app = FastAPI(title="Movie Service", version="1.0.0", lifespan=lifespan)


def get_publisher(request: Request) -> EventPublisher:
    return request.app.state.publisher


def get_generator(request: Request) -> EventGenerator:
    return request.app.state.generator


class PublishResponse(BaseModel):
    event_id: UUID
    partition: int
    offset: int


class GeneratorStatus(BaseModel):
    running: bool
    published: int
    failed: int


@app.post("/events", status_code=status.HTTP_201_CREATED, response_model=PublishResponse)
def create_event(event: MovieEvent, publisher: EventPublisher = Depends(get_publisher)):
    """Принимает событие в JSON, валидирует и публикует в Kafka.

    - 422 — событие не прошло валидацию (типы, enum, обязательность progress_seconds и т.д.);
    - 400 — не прошло проверку по Avro-схеме;
    - 503 — Kafka недоступна после всех повторных попыток;
    - 201 — опубликовано, в ответе event_id, partition и offset.
    """
    # Обычный def: FastAPI выполнит его в пуле потоков, ожидание подтверждения не блокирует event loop
    try:
        result = publisher.publish(event)
    except SchemaValidationError as e:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=f"schema validation failed: {e}")
    except PublishError as e:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(e))
    return PublishResponse(event_id=result.event_id, partition=result.partition, offset=result.offset)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/generator", response_model=GeneratorStatus)
def generator_status(gen: EventGenerator = Depends(get_generator)):
    return GeneratorStatus(running=gen.running, published=gen.published, failed=gen.failed)


@app.post("/generator/start", response_model=GeneratorStatus)
def generator_start(gen: EventGenerator = Depends(get_generator)):
    gen.start()
    return generator_status(gen)


@app.post("/generator/backfill")
def generator_backfill(days: int = 14, new_users_per_day: int = 40, seed: int = 42,
                       publisher: EventPublisher = Depends(get_publisher)):
    """Сгенерировать историю за `days` прошедших дней (нужна для метрик по дням и retention).
    С тем же seed повтор безопасен: event_id те же, ClickHouse схлопнет дубли."""
    if not 1 <= days <= 60:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, detail="days must be in 1..60")
    events = generate_history(random.Random(seed), days, new_users_per_day)
    stats = publisher.publish_batch(events)
    return {"days": days, "events": len(events), **stats}


@app.post("/generator/stop", response_model=GeneratorStatus)
def generator_stop(gen: EventGenerator = Depends(get_generator)):
    gen.stop()
    return generator_status(gen)
