"""Изоляция: каждый прогон пишет контрольные события на свою случайную дату в 2000-х годах
(реальных данных там нет) с пользователями `agg_<run_id>_*` и после теста всё удаляет."""
import os
import random
import uuid
from datetime import date, timedelta

import httpx
import psycopg
import pytest

from app.clickhouse import ClickHouse

AGG_API_URL = os.getenv("AGG_API_URL", "http://localhost:8001")


def pytest_collection_modifyitems(config, items):
    if os.getenv("RUN_INTEGRATION") == "1":
        return
    skip = pytest.mark.skip(reason="нужен поднятый стек: docker compose run --rm aggregation-integration-tests")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)


@pytest.fixture(scope="session")
def ch():
    client = ClickHouse()
    client.ping()
    yield client
    client.close()


@pytest.fixture(scope="session")
def pg():
    from app import config
    with psycopg.connect(config.POSTGRES_DSN, autocommit=True) as conn:
        yield conn


@pytest.fixture(scope="session")
def api():
    with httpx.Client(base_url=AGG_API_URL, timeout=60) as c:
        assert c.get("/health").status_code == 200
        yield c


@pytest.fixture
def isolated(ch, pg):
    """Случайная дата D и префикс пользователей; после теста — очистка ClickHouse и PostgreSQL."""
    run = uuid.uuid4().hex[:10]
    d = date(2000, 1, 1) + timedelta(days=random.randint(0, 3000))
    prefix = f"agg_{run}"
    yield d, prefix
    dates = [d.isoformat(), (d + timedelta(days=1)).isoformat(), (d + timedelta(days=7)).isoformat()]
    in_dates = ", ".join(f"'{x}'" for x in dates)  # даты сгенерированы тестом
    sync = {"mutations_sync": 2}
    ch.execute(f"ALTER TABLE cinema.movie_events DELETE WHERE startsWith(user_id, '{prefix}')", settings=sync)
    ch.execute(f"ALTER TABLE cinema.daily_metrics DELETE WHERE metric_date IN ({in_dates})", settings=sync)
    pg.execute("DELETE FROM daily_metrics WHERE metric_date = ANY(%s::date[])", (dates,))
    pg.execute("DELETE FROM aggregation_runs WHERE metric_date = ANY(%s::date[])", (dates,))
    pg.execute("DELETE FROM s3_exports WHERE metric_date = ANY(%s::date[])", (dates,))
    try:
        from app.exporter import S3Storage, object_key
        S3Storage().delete(object_key(d))
    except Exception:  # noqa: BLE001 — объекта может не быть
        pass
