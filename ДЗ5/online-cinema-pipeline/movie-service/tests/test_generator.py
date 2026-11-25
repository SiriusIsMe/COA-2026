import random
from collections import defaultdict
from datetime import datetime, timezone

from generator import USERS, SessionInterleaver, generate_session
from models import EventType, VIEW_EVENTS

N_SESSIONS = 2000


def sessions():
    rng = random.Random(42)
    for _ in range(N_SESSIONS):
        yield generate_session(rng, rng.choice(USERS))


def test_session_invariants():
    for events in sessions():
        types = [e.event_type for e in events]
        # одна сессия: один пользователь, фильм, устройство
        assert len({(e.user_id, e.movie_id, e.device_type, e.session_id) for e in events}) == 1
        # время строго растёт
        ts = [e.timestamp for e in events]
        assert ts == sorted(ts) and len(set(ts)) == len(ts)
        # ровно один старт; финиш (если есть) — после старта и последний из просмотровых
        assert types.count(EventType.VIEW_STARTED) == 1
        start = types.index(EventType.VIEW_STARTED)
        if EventType.VIEW_FINISHED in types:
            assert types.index(EventType.VIEW_FINISHED) > start
        view = [e for e in events if e.event_type in VIEW_EVENTS]
        assert view[0].event_type is EventType.VIEW_STARTED and view[0].progress_seconds == 0
        # прогресс не убывает
        progress = [e.progress_seconds for e in view]
        assert progress == sorted(progress)
        # PAUSED и RESUMED чередуются: продолжить можно только после паузы
        for prev, cur in zip(view, view[1:]):
            if cur.event_type is EventType.VIEW_RESUMED:
                assert prev.event_type is EventType.VIEW_PAUSED
                assert cur.progress_seconds == prev.progress_seconds
            if cur.event_type is EventType.VIEW_PAUSED:
                assert cur.progress_seconds > prev.progress_seconds
        # поиск — первым, лайк — последним, у них нет прогресса
        if EventType.SEARCHED in types:
            assert types[0] is EventType.SEARCHED
        if EventType.LIKED in types:
            assert types[-1] is EventType.LIKED
        for e in events:
            if e.event_type not in VIEW_EVENTS:
                assert e.progress_seconds is None


def test_all_event_types_generated():
    seen = {e.event_type for s in sessions() for e in s}
    assert seen == set(EventType)


def test_interleaver_keeps_per_user_order():
    src = SessionInterleaver(random.Random(1), concurrency=20)
    per_user = defaultdict(list)
    for _ in range(5000):
        e = src.next_event()
        per_user[e.user_id].append(e)
    for events in per_user.values():
        ts = [e.timestamp for e in events]
        # сессии одного пользователя не перекрываются, внутри сессии время растёт
        for a, b in zip(events, events[1:]):
            if a.session_id == b.session_id:
                assert b.timestamp > a.timestamp
        sessions_order = [e.session_id for e in events]
        # события одной сессии идут подряд (нет двух сессий одновременно)
        compressed = [s for i, s in enumerate(sessions_order) if i == 0 or s != sessions_order[i - 1]]
        assert len(compressed) == len(set(compressed))
        assert ts  # не пусто


def test_generator_is_deterministic_with_seed():
    now = datetime(2026, 11, 25, 12, tzinfo=timezone.utc)
    a = generate_session(random.Random(7), "user_00001", now)
    b = generate_session(random.Random(7), "user_00001", now)
    assert [e.model_dump() for e in a] == [e.model_dump() for e in b]
