"""Сквозная проверка: HTTP API -> Kafka -> ClickHouse (Kafka Engine -> MV -> MergeTree).

Запуск:  docker compose exec movie-service python e2e_clickhouse.py
"""
import json
import os
import sys
import time
import urllib.parse
import urllib.request
import uuid

API = os.getenv("API_URL", "http://localhost:8000")
CH = os.getenv("CLICKHOUSE_URL", "http://localhost:8123")
CH_AUTH = {"X-ClickHouse-User": os.getenv("CLICKHOUSE_USER", "cinema"),
           "X-ClickHouse-Key": os.getenv("CLICKHOUSE_PASSWORD", "cinema")}


def ch(query: str) -> str:
    req = urllib.request.Request(f"{CH}/?{urllib.parse.urlencode({'query': query})}", headers=CH_AUTH)
    with urllib.request.urlopen(req, timeout=10) as r:
        return r.read().decode().strip()


def main() -> int:
    session = f"e2e-{uuid.uuid4()}"
    event = {"user_id": "user_e2e", "movie_id": "movie_e2e", "event_type": "VIEW_PAUSED",
             "device_type": "DESKTOP", "session_id": session, "progress_seconds": 1234}
    req = urllib.request.Request(f"{API}/events", data=json.dumps(event).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=30) as r:
        resp = json.loads(r.read())
    event_id = resp["event_id"]
    print(f"1. API принял событие: {resp}")

    for i in range(30):  # Kafka Engine сбрасывает пачки раз в ~7.5 c
        row = ch(f"SELECT event_type, progress_seconds, device_type, kafka_partition, kafka_offset "
                 f"FROM cinema.movie_events WHERE event_id = '{event_id}' FORMAT TSV")
        if row:
            print(f"2. Событие в cinema.movie_events через {i * 2} c: {row}")
            break
        time.sleep(2)
    else:
        print("ОШИБКА: событие не дошло до ClickHouse за 60 c")
        return 1

    print("3. Всего событий:", ch("SELECT count() FROM cinema.movie_events"))
    print("   По типам:\n" + ch("SELECT event_type, count() FROM cinema.movie_events "
                                "GROUP BY event_type ORDER BY event_type FORMAT TSV"))
    print("   Ошибок разбора:", ch("SELECT count() FROM cinema.movie_events_errors"))
    print("OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
