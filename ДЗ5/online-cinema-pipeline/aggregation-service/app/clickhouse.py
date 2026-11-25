"""Минимальный клиент ClickHouse поверх HTTP-интерфейса (порт 8123).
Параметры запроса передаются как {name:Type} -> param_name, без склейки строк."""
from __future__ import annotations

import json

import httpx

from app import config


class ClickHouseError(RuntimeError):
    pass


class ClickHouse:
    def __init__(self, url: str = config.CLICKHOUSE_URL, user: str = config.CLICKHOUSE_USER,
                 password: str = config.CLICKHOUSE_PASSWORD, timeout: float = config.CLICKHOUSE_TIMEOUT):
        self._http = httpx.Client(base_url=url, auth=(user, password), timeout=timeout)

    def execute(self, sql: str, settings: dict | None = None, content: bytes | None = None,
                **params) -> str:
        """settings — настройки ClickHouse; content — данные для INSERT ... FORMAT (тогда sql идёт в URL)."""
        sql = sql.strip().rstrip(";")  # HTTP-интерфейс принимает один запрос
        query_params = {
            "output_format_json_quote_64bit_integers": 0,
            **(settings or {}),
            **{f"param_{k}": v for k, v in params.items()},
        }
        if content is not None:
            query_params["query"] = sql
            r = self._http.post("/", content=content, params=query_params)
        else:
            r = self._http.post("/", content=sql.encode("utf-8"), params=query_params)
        if r.status_code != 200:
            raise ClickHouseError(f"ClickHouse {r.status_code}: {r.text.strip()[:2000]}")
        return r.text

    def rows(self, sql: str, **params) -> list[dict]:
        text = self.execute(f"{sql.rstrip().rstrip(';')} FORMAT JSONEachRow", **params)
        return [json.loads(line) for line in text.splitlines() if line.strip()]

    def ping(self) -> None:
        self.execute("SELECT 1")

    def close(self) -> None:
        self._http.close()
