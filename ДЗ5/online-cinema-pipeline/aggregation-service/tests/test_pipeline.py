import logging
from datetime import date

from app.pipeline import run_cycle
from tests.test_metrics import ROWS, FakeClickHouse


class FakeStore:
    def __init__(self, fail=False):
        self.fail, self.saved, self.runs = fail, [], []

    def save_day(self, day):
        if self.fail:
            raise ConnectionError("pg down")
        self.saved.append(day)
        return len(day.rows)

    def record_run(self, *args):
        self.runs.append(args)


def test_cycle_success_logs_counts(caplog):
    store = FakeStore()
    with caplog.at_level(logging.INFO, logger="pipeline"):
        res = run_cycle(FakeClickHouse(ROWS), store, date(2026, 11, 20), "manual", today=date(2026, 11, 25))
    assert res.status == "success" and res.rows_written == 3 and res.events_processed == 1234
    msgs = [r.getMessage() for r in caplog.records]
    assert any(m.startswith("Цикл начат") for m in msgs)
    assert any("Цикл завершён" in m and "1234 событий" in m and "сек" in m for m in msgs)
    assert store.runs[0][2] == "success"


def test_cycle_failure_is_recorded():
    store = FakeStore(fail=True)
    res = run_cycle(FakeClickHouse(ROWS), store, date(2026, 11, 20), "schedule", today=date(2026, 11, 25))
    assert res.status == "failed" and "pg down" in res.error
    assert store.runs[0][2] == "failed"
