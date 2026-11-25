"""CLI для ручного пересчёта.

    python -m app.cli compute 2026-11-20          # одна дата
    python -m app.cli compute --days 15           # последние 15 дней, включая сегодня
    python -m app.cli compute --days 15 --no-save # только ClickHouse, без PostgreSQL
"""
import argparse
import logging
from datetime import date, datetime, timedelta, timezone

from app.clickhouse import ClickHouse
from app.metrics import compute_day
from app.pipeline import run_cycle
from app.storage import PostgresStore

PERCENT = ("view_conversion", "retention_d1", "retention_d7")


def print_day(res) -> None:
    print(f"=== {res.metric_date}  (событий: {res.events_processed})")
    for r in res.rows:
        if r.metric_name == "top_movie_views":
            continue
        v = f"{r.value:.1%}" if r.metric_name in PERCENT else f"{r.value:,.1f}"
        print(f"  {r.metric_name:<24} {v}")
    top = [r for r in res.rows if r.metric_name == "top_movie_views"]
    if top:
        print("  top movies: " + ", ".join(f"{r.dimension} ({r.value:.0f})" for r in top[:5])
              + (" ..." if len(top) > 5 else ""))


def setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)


def main() -> None:
    setup_logging()
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("compute")
    c.add_argument("date", nargs="?", type=date.fromisoformat)
    c.add_argument("--days", type=int, default=1)
    c.add_argument("--no-save", action="store_true", help="не писать в PostgreSQL")
    c.add_argument("--quiet", action="store_true", help="только строки логов, без таблиц метрик")
    sub.add_parser("migrate")
    e = sub.add_parser("export", help="выгрузить метрики даты из PostgreSQL в S3")
    e.add_argument("date", type=date.fromisoformat)
    args = p.parse_args()

    store = PostgresStore()
    if args.cmd == "migrate":
        print("применены:", store.migrate() or "нет новых")
        return
    if args.cmd == "export":
        from app.exporter import Exporter, S3Storage
        res = Exporter(store, S3Storage()).export_date(args.date, "cli", force=True)
        print(res)
        return

    today = datetime.now(timezone.utc).date()
    dates = [args.date] if args.date else [today - timedelta(days=i) for i in range(args.days - 1, -1, -1)]
    ch = ClickHouse()
    try:
        if not args.no_save:
            store.migrate()
        for d in dates:
            if args.no_save:
                day = compute_day(ch, d, today)
            else:
                day = run_cycle(ch, store, d, trigger="cli", today=today).day
            if day and not args.quiet:
                print_day(day)
    finally:
        ch.close()


if __name__ == "__main__":
    main()
