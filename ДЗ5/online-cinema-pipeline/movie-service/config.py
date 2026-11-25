import os
from pathlib import Path


def _bool(name: str, default: str) -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092,localhost:9093,localhost:9094")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

TOPIC = os.getenv("TOPIC", "movie-events")
TOPIC_PARTITIONS = int(os.getenv("TOPIC_PARTITIONS", "3"))
TOPIC_REPLICATION = int(os.getenv("TOPIC_REPLICATION", "3"))
TOPIC_MIN_ISR = int(os.getenv("TOPIC_MIN_ISR", "2"))

# Стратегия TopicNameStrategy: subject = <topic>-value
SUBJECT = f"{TOPIC}-value"

_here = Path(__file__).parent
# В контейнере схемы лежат в /app/schemas, при локальном запуске — в ../schemas
_default_schema = _here / "schemas" / "movie_event.avsc"
if not _default_schema.exists():
    _default_schema = _here.parent / "schemas" / "movie_event.avsc"
SCHEMA_PATH = Path(os.getenv("SCHEMA_PATH", _default_schema))

# --- Публикация ---
PUBLISH_MAX_ATTEMPTS = int(os.getenv("PUBLISH_MAX_ATTEMPTS", "5"))
PUBLISH_BACKOFF_BASE = float(os.getenv("PUBLISH_BACKOFF_BASE", "0.5"))   # сек
PUBLISH_BACKOFF_MAX = float(os.getenv("PUBLISH_BACKOFF_MAX", "8"))       # сек
DELIVERY_TIMEOUT_MS = int(os.getenv("DELIVERY_TIMEOUT_MS", "15000"))

# --- Генератор ---
GENERATOR_ENABLED = _bool("GENERATOR_ENABLED", "true")
EVENTS_PER_SECOND = float(os.getenv("EVENTS_PER_SECOND", "5"))
GENERATOR_CONCURRENCY = int(os.getenv("GENERATOR_CONCURRENCY", "20"))  # одновременных сессий


def load_schema_str() -> str:
    return SCHEMA_PATH.read_text(encoding="utf-8")
