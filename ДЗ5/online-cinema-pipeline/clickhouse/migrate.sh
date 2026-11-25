#!/usr/bin/env bash
# Применяет SQL-миграции из /migrations по порядку имён.
# Применённые миграции записываются в cinema.schema_migrations и повторно не выполняются.
set -euo pipefail

CH=(clickhouse-client --host "${CH_HOST:-clickhouse}" --user "${CH_USER:-cinema}" --password "${CH_PASSWORD:-cinema}")

for i in $(seq 1 60); do
  if "${CH[@]}" -q "SELECT 1" >/dev/null 2>&1; then break; fi
  echo "[migrate] ждём ClickHouse ($i/60)..."
  sleep 2
done

"${CH[@]}" -q "CREATE DATABASE IF NOT EXISTS cinema"
"${CH[@]}" -q "CREATE TABLE IF NOT EXISTS cinema.schema_migrations
               (name String, applied_at DateTime DEFAULT now())
               ENGINE = MergeTree ORDER BY name"

for f in $(ls /migrations/*.sql | sort); do
  name=$(basename "$f")
  applied=$("${CH[@]}" -q "SELECT count() FROM cinema.schema_migrations WHERE name = '${name}'")
  if [[ "$applied" == "0" ]]; then
    echo "[migrate] применяем ${name}"
    "${CH[@]}" --multiquery < "$f"
    "${CH[@]}" -q "INSERT INTO cinema.schema_migrations (name) VALUES ('${name}')"
  else
    echo "[migrate] ${name} уже применена"
  fi
done

echo "[migrate] готово"
