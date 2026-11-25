import os
from pathlib import Path

CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "http://localhost:8123")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "cinema")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "cinema")
CLICKHOUSE_TIMEOUT = float(os.getenv("CLICKHOUSE_TIMEOUT", "60"))

TOP_MOVIES_LIMIT = int(os.getenv("TOP_MOVIES_LIMIT", "10"))

SQL_DIR = Path(os.getenv("SQL_DIR", Path(__file__).resolve().parent.parent / "sql"))

POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://metrics:metrics@localhost:5432/metrics")
PG_RETRY_ATTEMPTS = int(os.getenv("PG_RETRY_ATTEMPTS", "5"))
PG_RETRY_BASE = float(os.getenv("PG_RETRY_BASE", "0.5"))
PG_RETRY_MAX = float(os.getenv("PG_RETRY_MAX", "8"))

_here = Path(__file__).resolve().parent.parent
PG_MIGRATIONS_DIR = Path(os.getenv("PG_MIGRATIONS_DIR", next(
    (p for p in [_here / "pg_migrations", _here.parent / "postgres" / "migrations"] if p.exists()),
    _here / "pg_migrations",
)))

# --- Расписание ---
# Интервал между плановыми циклами, сек. Если задан AGG_CRON (например "*/5 * * * *"), он важнее.
AGG_INTERVAL_SECONDS = int(os.getenv("AGG_INTERVAL_SECONDS", "300"))
AGG_CRON = os.getenv("AGG_CRON", "").strip()
# Сколько прошедших дней пересчитывать в каждом плановом цикле, кроме сегодняшнего.
# 8 дней: retention D7 для дня d становится известен только после завершения дня d+7.
AGG_LOOKBACK_DAYS = int(os.getenv("AGG_LOOKBACK_DAYS", "8"))
AGG_RUN_ON_START = os.getenv("AGG_RUN_ON_START", "true").lower() in {"1", "true", "yes"}

# --- Экспорт в S3 ---
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "localhost:8333")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "cinema")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "cinema-secret")
S3_REGION = os.getenv("S3_REGION", "us-east-1")
S3_SECURE = os.getenv("S3_SECURE", "false").lower() in {"1", "true", "yes"}
S3_BUCKET = os.getenv("S3_BUCKET", "movie-analytics")
S3_PREFIX = os.getenv("S3_PREFIX", "daily")
S3_RETRY_ATTEMPTS = int(os.getenv("S3_RETRY_ATTEMPTS", "3"))
EXPORT_ENABLED = os.getenv("EXPORT_ENABLED", "true").lower() in {"1", "true", "yes"}
# Как часто проверять, есть ли что выгрузить (новый завершённый день, изменения, прошлые ошибки).
EXPORT_INTERVAL_SECONDS = int(os.getenv("EXPORT_INTERVAL_SECONDS", "300"))
EXPORT_CRON = os.getenv("EXPORT_CRON", "").strip()
# Сколько завершённых дней проверять: 10 > 8 дней, за которые досчитывается retention D7
EXPORT_LOOKBACK_DAYS = int(os.getenv("EXPORT_LOOKBACK_DAYS", "10"))
