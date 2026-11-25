"""Схема ClickHouse соответствует Avro-схеме из п.1: те же поля, в том же порядке, совместимые типы.
Проверяется по тексту миграции, ClickHouse для теста не нужен."""
import json
import re
from pathlib import Path

import pytest

import config

_here = Path(__file__).resolve().parent
MIGRATIONS_DIR = next(p for p in [
    _here.parent / "clickhouse_migrations",               # в контейнере
    _here.parent.parent / "clickhouse" / "migrations",    # в репозитории
] if p.exists())
MIGRATION = MIGRATIONS_DIR / "001_movie_events.sql"


def split_top_level(s: str) -> list[str]:
    parts, depth, cur = [], 0, ""
    for ch in s:
        depth += ch == "("
        depth -= ch == ")"
        if ch == "," and depth == 0:
            parts.append(cur)
            cur = ""
        else:
            cur += ch
    parts.append(cur)
    return [p.strip() for p in parts if p.strip()]


def table_columns(table: str, migration=None) -> dict[str, str]:
    sql = re.sub(r"--[^\n]*", "", (migration or MIGRATION).read_text(encoding="utf-8"))
    m = re.search(rf"CREATE TABLE (?:IF NOT EXISTS )?cinema\.{table}\s*\((.*?)\)\s*ENGINE", sql, re.S)
    assert m, f"table {table} not found"
    cols = {}
    for col in split_top_level(m.group(1)):
        name, ctype = col.split(None, 1)
        ctype = re.split(r"\s+(DEFAULT|MATERIALIZED|ALIAS|CODEC)\b", ctype)[0]
        cols[name] = re.sub(r"\s+", " ", ctype).strip()
    return cols


def expected_ch_type(avro_type) -> str:
    if avro_type == "string":
        return "String"
    if avro_type == ["null", "int"]:
        return "Nullable(Int32)"
    if isinstance(avro_type, dict):
        if avro_type.get("logicalType") == "uuid":
            return "UUID"
        if avro_type.get("logicalType") == "timestamp-millis":
            return "DateTime64(3, 'UTC')"
        if avro_type["type"] == "enum":
            items = ", ".join(f"'{s}' = {i}" for i, s in enumerate(avro_type["symbols"], 1))
            return f"Enum8({items})"
    raise AssertionError(f"нет сопоставления для {avro_type}")


AVRO_FIELDS = json.loads(config.load_schema_str())["fields"]


@pytest.mark.parametrize("table", ["movie_events_queue", "movie_events"])
def test_clickhouse_matches_avro(table):
    cols = table_columns(table)
    names = [f["name"] for f in AVRO_FIELDS]
    # все поля схемы есть и идут первыми в том же порядке
    assert list(cols)[: len(names)] == names
    for f in AVRO_FIELDS:
        assert cols[f["name"]] == expected_ch_type(f["type"]), f["name"]


def test_kafka_table_has_only_schema_fields():
    assert list(table_columns("movie_events_queue")) == [f["name"] for f in AVRO_FIELDS]


def test_recreated_kafka_table_matches_avro():
    """004 пересоздаёт Kafka-таблицу под кластер — схема та же, брокеры все три."""
    mig = MIGRATIONS_DIR / "004_kafka_cluster.sql"
    cols = table_columns("movie_events_queue", mig)
    assert list(cols) == [f["name"] for f in AVRO_FIELDS]
    for f in AVRO_FIELDS:
        assert cols[f["name"]] == expected_ch_type(f["type"])
    assert "kafka-1:29092,kafka-2:29092,kafka-3:29092" in mig.read_text(encoding="utf-8")
