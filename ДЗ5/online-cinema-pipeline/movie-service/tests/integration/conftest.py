"""Общие фикстуры для сквозных тестов Kafka -> ClickHouse.

Изоляция: каждый прогон получает уникальный run_id, все тестовые события имеют
user_id/session_id с префиксом `it_<run_id>`. Тесты ищут только свои события,
поэтому параллельный поток генератора им не мешает. После прогона тестовые строки удаляются.
"""
import json
import os
import time
import uuid

import httpx
import pytest

CH_URL = os.getenv("CLICKHOUSE_URL", "http://localhost:8123")
CH_AUTH = (os.getenv("CLICKHOUSE_USER", "cinema"), os.getenv("CLICKHOUSE_PASSWORD", "cinema"))
API_URL = os.getenv("API_URL", "http://localhost:8000")
WAIT_TIMEOUT = float(os.getenv("IT_WAIT_TIMEOUT", "60"))


def pytest_collection_modifyitems(config, items):
    if os.getenv("RUN_INTEGRATION") == "1":
        return
    skip = pytest.mark.skip(reason="нужен поднятый стек: docker compose run --rm integration-tests")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)


class ClickHouse:
    def __init__(self):
        self.http = httpx.Client(base_url=CH_URL, auth=CH_AUTH, timeout=30)

    def query(self, sql: str, settings: dict | None = None, **params) -> httpx.Response:
        """params -> параметры запроса {name:Type}; settings -> настройки ClickHouse."""
        r = self.http.post("/", content=sql, params={
            "output_format_json_quote_64bit_integers": 0,
            **(settings or {}),
            **{f"param_{k}": v for k, v in params.items()},
        })
        if r.status_code != 200:  # показываем текст ошибки ClickHouse, а не только код
            raise AssertionError(f"ClickHouse {r.status_code}: {r.text.strip()}\n{sql}")
        return r

    def rows(self, sql: str, **params) -> list[dict]:
        text = self.query(sql + " FORMAT JSONEachRow", **params).text
        return [json.loads(line) for line in text.splitlines() if line]

    def wait_rows(self, sql: str, expected: int = 1, timeout: float = WAIT_TIMEOUT, **params) -> list[dict]:
        """Kafka Engine сбрасывает пачки раз в несколько секунд — ждём с поллингом."""
        deadline = time.monotonic() + timeout
        rows: list[dict] = []
        while time.monotonic() < deadline:
            rows = self.rows(sql, **params)
            if len(rows) >= expected:
                return rows
            time.sleep(1)
        raise AssertionError(f"за {timeout:.0f} c получено {len(rows)} строк из {expected}:\n{sql}")


@pytest.fixture(scope="session")
def clickhouse():
    ch = ClickHouse()
    ch.query("SELECT 1")
    return ch


@pytest.fixture(scope="session")
def run_id(clickhouse):
    def cleanup(prefix: str) -> None:
        # prefix — сгенерированный hex или константа, подставлять в SQL безопасно
        clickhouse.query(f"ALTER TABLE cinema.movie_events DELETE WHERE startsWith(user_id, '{prefix}')",
                         settings={"mutations_sync": 2})
        clickhouse.query(f"ALTER TABLE cinema.movie_events_errors "
                         f"DELETE WHERE position(raw_message, '{prefix}') > 0",
                         settings={"mutations_sync": 2})

    cleanup("it_")          # остатки прерванных прошлых прогонов
    rid = f"it_{uuid.uuid4().hex[:12]}"
    yield rid
    cleanup(rid)            # всё, что создал этот прогон


@pytest.fixture(scope="session")
def publisher():
    from publisher import EventPublisher
    pub = EventPublisher.from_config()
    yield pub
    pub.close()


@pytest.fixture(scope="session")
def api():
    with httpx.Client(base_url=API_URL, timeout=30) as client:
        assert client.get("/health").status_code == 200
        yield client
