from datetime import date

from app import config
from app.metrics import compute_day, load_sql


class FakeClickHouse:
    def __init__(self, rows):
        self.executed = []
        self._rows = rows

    def execute(self, sql, **params):
        self.executed.append(params)
        return ""

    def rows(self, sql, **params):
        return self._rows


ROWS = [
    {"metric_date": "2026-11-20", "metric_name": "dau", "dimension": "", "value": 170},
    {"metric_date": "2026-11-20", "metric_name": "events_total", "dimension": "", "value": 1234},
    {"metric_date": "2026-11-20", "metric_name": "top_movie_views", "dimension": "movie_0001", "value": 9},
]


def test_all_sql_files_executed_with_same_ts():
    ch = FakeClickHouse(ROWS)
    res = compute_day(ch, date(2026, 11, 20), today=date(2026, 11, 25))
    assert len(ch.executed) == len(load_sql()) == 5
    assert len({p["ts"] for p in ch.executed}) == 1          # один computed_at на цикл
    assert ch.executed[0]["top_n"] == config.TOP_MOVIES_LIMIT
    assert res.events_processed == 1234
    assert res.value("dau") == 170
    assert res.value("top_movie_views", "movie_0001") == 9


def test_retention_gating():
    def flags(d, today):
        ch = FakeClickHouse([])
        compute_day(ch, d, today=today)
        return ch.executed[0]["with_d1"], ch.executed[0]["with_d7"]

    today = date(2026, 11, 25)
    assert flags(date(2026, 11, 25), today) == (0, 0)   # сегодня — retention ещё нет
    assert flags(date(2026, 11, 24), today) == (0, 0)   # d+1 = сегодня, день не завершён
    assert flags(date(2026, 11, 23), today) == (1, 0)   # D1 есть, D7 нет
    assert flags(date(2026, 11, 18), today) == (1, 0)   # d+7 = сегодня — D7 ещё нет
    assert flags(date(2026, 11, 17), today) == (1, 1)


def test_sql_uses_aggregate_and_window_functions():
    sql = " ".join(text for _, text in load_sql())
    for fn in ("uniqExact(", "avgIf(", "uniqExactIf(", "row_number() OVER", "groupUniqArray(", "countIf(",
               "ARRAY JOIN range("):
        assert fn in sql


def test_cohort_max_day():
    def max_day(d, today):
        ch = FakeClickHouse([])
        compute_day(ch, d, today=today)
        return ch.executed[0]["max_day"]

    today = date(2026, 11, 25)
    assert max_day(today, today) == 0                    # сегодня — только day_0
    assert max_day(date(2026, 11, 24), today) == 0       # day_1 = сегодня, ещё не завершён
    assert max_day(date(2026, 11, 22), today) == 2
    assert max_day(date(2026, 11, 1), today) == 7        # не больше 7
