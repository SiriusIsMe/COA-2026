"""Генератор синтетических событий.

Каждая сессия — правдоподобная последовательность:
    [SEARCHED] → VIEW_STARTED → (VIEW_PAUSED → VIEW_RESUMED)* → VIEW_FINISHED | VIEW_PAUSED (бросил) → [LIKED]

Инварианты (проверяются тестами):
- VIEW_STARTED всегда раньше VIEW_FINISHED в рамках сессии;
- progress_seconds не убывает, а между событиями просмотра растёт;
- время событий в сессии строго возрастает;
- у одного пользователя одновременно не больше одной активной сессии,
  поэтому порядок его событий в Kafka совпадает с порядком по времени.

Одновременно идут несколько сессий разных пользователей, их события перемежаются,
как в реальном потоке.
"""
from __future__ import annotations

import logging
import random
import threading
import uuid
from collections import deque
from datetime import datetime, timedelta, timezone

from models import DeviceType, EventType, MovieEvent

log = logging.getLogger("generator")

USERS = [f"user_{i:05d}" for i in range(1, 1001)]
MOVIES = [f"movie_{i:04d}" for i in range(1, 201)]
DEVICES = [DeviceType.MOBILE, DeviceType.DESKTOP, DeviceType.TV, DeviceType.TABLET]
DEVICE_WEIGHTS = [0.40, 0.25, 0.25, 0.10]


def generate_session(rng: random.Random, user_id: str, now: datetime | None = None,
                     start_at: datetime | None = None) -> list[MovieEvent]:
    now = now or datetime.now(timezone.utc)
    device = rng.choices(DEVICES, DEVICE_WEIGHTS)[0]
    session_id = str(uuid.UUID(int=rng.getrandbits(128), version=4))
    movie_id = rng.choice(MOVIES)
    duration = rng.randint(80, 150) * 60  # длина фильма, сек

    # Сессия «уже прошла»: начинаем ~3 часа назад, чтобы время не уходило в будущее
    ts = start_at or now - timedelta(hours=3, seconds=rng.uniform(0, 600))
    events: list[MovieEvent] = []

    def emit(event_type: EventType, after_seconds: float, progress: int | None = None):
        nonlocal ts
        ts = ts + timedelta(seconds=max(after_seconds, 0.001))
        events.append(MovieEvent(
            event_id=uuid.UUID(int=rng.getrandbits(128), version=4),
            user_id=user_id, movie_id=movie_id, event_type=event_type, timestamp=ts,
            device_type=device, session_id=session_id, progress_seconds=progress,
        ))

    if rng.random() < 0.6:
        emit(EventType.SEARCHED, 0)

    progress = 0
    emit(EventType.VIEW_STARTED, rng.uniform(1, 10), progress)

    for _ in range(rng.randint(0, 3)):
        watched = rng.randint(60, duration // 3)
        if progress + watched >= duration - 1:
            break
        progress += watched
        emit(EventType.VIEW_PAUSED, watched, progress)
        emit(EventType.VIEW_RESUMED, rng.uniform(5, 300), progress)

    if rng.random() < 0.7:  # досмотрел до конца
        watched, progress = duration - progress, duration
        emit(EventType.VIEW_FINISHED, watched, progress)
    else:  # бросил — сессия заканчивается паузой
        watched = rng.randint(1, duration - progress - 1)
        progress += watched  # строго меньше duration
        emit(EventType.VIEW_PAUSED, watched, progress)

    if rng.random() < 0.3:
        emit(EventType.LIKED, rng.uniform(1, 30))

    return events


def generate_history(rng: random.Random, days: int, new_users_per_day: int = 40,
                     return_prob: float = 0.35, now: datetime | None = None) -> list[MovieEvent]:
    """История за `days` прошедших дней (без сегодняшнего) для метрик по дням и retention.

    Каждый день приходят новые пользователи, а каждый уже знакомый возвращается
    с вероятностью `return_prob`. С одинаковым seed результат одинаковый, включая event_id,
    поэтому повторный backfill даёт дубли, которые ReplacingMergeTree схлопнет.
    """
    now = now or datetime.now(timezone.utc)
    today = now.date()
    seen: list[str] = []
    events: list[MovieEvent] = []
    for days_ago in range(days, 0, -1):
        day = today - timedelta(days=days_ago)
        day_start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
        new_users = [f"hist_{day:%Y%m%d}_{i:03d}" for i in range(new_users_per_day)]
        returning = [u for u in seen if rng.random() < return_prob]
        seen.extend(new_users)
        for user in new_users + returning:
            for _ in range(rng.choice([1, 1, 2])):
                start = day_start + timedelta(seconds=rng.uniform(0, 18 * 3600))
                events.extend(generate_session(rng, user, now=now, start_at=start))
    return events


class SessionInterleaver:
    """Держит N активных сессий разных пользователей и выдаёт их события вперемешку."""

    def __init__(self, rng: random.Random, concurrency: int):
        self.rng = rng
        self.concurrency = concurrency
        self.active: dict[str, deque[MovieEvent]] = {}  # user_id -> оставшиеся события

    def next_event(self) -> MovieEvent:
        while len(self.active) < self.concurrency:
            user = self.rng.choice(USERS)
            if user not in self.active:
                self.active[user] = deque(generate_session(self.rng, user))
        user = self.rng.choice(list(self.active))
        queue = self.active[user]
        event = queue.popleft()
        if not queue:
            del self.active[user]
        return event


class EventGenerator:
    """Фоновый поток: берёт события из SessionInterleaver и публикует с заданной скоростью."""

    def __init__(self, publisher, events_per_second: float, concurrency: int, seed: int | None = None):
        self.publisher = publisher
        self.interval = 1.0 / events_per_second
        self.source = SessionInterleaver(random.Random(seed), concurrency)
        self.published = 0
        self.failed = 0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self) -> None:
        if self.running:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="event-generator", daemon=True)
        self._thread.start()
        log.info("generator started: %.1f events/s", 1 / self.interval)

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
        log.info("generator stopped: published=%s failed=%s", self.published, self.failed)

    def _run(self) -> None:
        while not self._stop.is_set():
            event = self.source.next_event()
            try:
                self.publisher.publish(event)
                self.published += 1
            except Exception as exc:  # noqa: BLE001 — генератор не должен падать
                self.failed += 1
                log.error("generator: event %s not published: %s", event.event_id, exc)
            self._stop.wait(self.interval)
