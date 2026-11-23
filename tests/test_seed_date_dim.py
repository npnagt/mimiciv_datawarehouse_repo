from datetime import date

from mimic_datawarehouse.seed_date_dim import (
    SEED_END_DATE,
    SEED_START_DATE,
    _BATCH_SIZE,
    generate_date_dim_rows,
    seed_date_dim,
)
from mimic_datawarehouse.warehouse import WAREHOUSE_TABLES

_NON_AUDIT_DIM_DATE_COLUMNS = {
    name for name, _ in WAREHOUSE_TABLES["dim_date"]["columns"] if name not in ("created_ts", "updated_ts", "created_by", "updated_by")
}


def test_default_range_matches_requested_seed_window():
    assert SEED_START_DATE == date(2100, 1, 1)
    # 2230, not 2210: the full MIMIC-IV 3.1 dataset date-shifts some events out to ~2215,
    # matching the lakehouse repo's dim_date spine.
    assert SEED_END_DATE == date(2230, 12, 31)


def test_row_count_matches_calendar_day_count():
    rows = generate_date_dim_rows(date(2100, 1, 1), date(2100, 1, 31))
    assert len(rows) == 31

    rows = generate_date_dim_rows(date(2100, 1, 1), date(2100, 12, 31))
    assert len(rows) == 365  # 2100 is not a leap year (divisible by 100, not 400)


def test_every_row_has_every_dim_date_column_except_audit():
    rows = generate_date_dim_rows(date(2100, 1, 1), date(2100, 1, 3))
    for row in rows:
        assert set(row.keys()) == _NON_AUDIT_DIM_DATE_COLUMNS


def test_date_key_is_yyyymmdd_integer():
    rows = generate_date_dim_rows(date(2100, 3, 5), date(2100, 3, 5))
    assert rows[0]["date_key"] == 21000305


def test_known_weekday_and_quarter():
    # 2100-03-05 falls on a Friday (2100 is not a leap year; verified via Python's own
    # calendar module independently of the production code path).
    rows = generate_date_dim_rows(date(2100, 3, 5), date(2100, 3, 5))
    row = rows[0]
    assert row["day_name"] == "Friday"
    assert row["day_name_short"] == "Fri"
    assert row["is_weekday"] is True
    assert row["is_weekend"] is False
    assert row["quarter_number"] == 1
    assert row["quarter_name"] == "Q1"
    assert row["month_name"] == "March"
    assert row["fiscal_year"] == 2100


def test_week_boundaries_span_monday_to_sunday():
    rows = generate_date_dim_rows(date(2100, 3, 5), date(2100, 3, 5))  # a Friday
    row = rows[0]
    assert row["week_start_date"].isoweekday() == 1
    assert row["week_end_date"].isoweekday() == 7
    assert row["week_start_date"] <= row["full_date"] <= row["week_end_date"]


def test_last_day_of_month_handles_leap_and_non_leap_february():
    rows = generate_date_dim_rows(date(2100, 2, 1), date(2100, 2, 1))
    assert rows[0]["last_day_of_month"] == date(2100, 2, 28)  # 2100 not a leap year

    rows = generate_date_dim_rows(date(2104, 2, 1), date(2104, 2, 1))
    assert rows[0]["last_day_of_month"] == date(2104, 2, 29)  # 2104 is a leap year


def test_rejects_end_before_start():
    import pytest

    with pytest.raises(ValueError):
        generate_date_dim_rows(date(2100, 1, 2), date(2100, 1, 1))


class _FakeConn:
    """Enough of a SQLAlchemy Connection for seed_date_dim: records every execute()
    call's bound params so tests can verify batching without a real database."""

    def __init__(self):
        self.calls: list[dict] = []

    def execute(self, _stmt, params):
        self.calls.append(params)

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False


class _FakeEngine:
    def __init__(self, conn):
        self._conn = conn

    def begin(self):
        return self._conn


def test_seed_date_dim_batches_into_few_round_trips_not_one_per_row():
    """The naive one-execute()-per-row loop this replaced took ~33 minutes against a
    real Aurora cluster for the default ~47,800-row range -- one network round trip per
    day. Batching must cut that down to roughly len(rows) / _BATCH_SIZE round trips."""
    conn = _FakeConn()
    engine = _FakeEngine(conn)

    days_seeded = seed_date_dim(engine, date(2100, 1, 1), date(2100, 12, 31))  # 365 days

    assert days_seeded == 365
    import math

    expected_batches = math.ceil(365 / _BATCH_SIZE)
    assert len(conn.calls) == expected_batches
    assert len(conn.calls) < 365  # the whole point: not one round trip per row


def test_seed_date_dim_includes_every_row_across_batches():
    conn = _FakeConn()
    engine = _FakeEngine(conn)

    seed_date_dim(engine, date(2100, 1, 1), date(2100, 1, 1) + (date(2101, 1, 1) - date(2100, 1, 1)))

    # every date_key bound across every batch call, regardless of its batch/position
    seen_date_keys = set()
    for params in conn.calls:
        for key, value in params.items():
            if key.startswith("date_key_"):
                seen_date_keys.add(value)
    assert len(seen_date_keys) == 366  # 2100-01-01 .. 2101-01-01 inclusive


def test_seed_date_dim_last_batch_can_be_smaller_than_batch_size():
    conn = _FakeConn()
    engine = _FakeEngine(conn)
    # _BATCH_SIZE + 1 days -> 2 batches, second one holding exactly 1 row.
    from datetime import timedelta

    end = SEED_START_DATE + timedelta(days=_BATCH_SIZE)
    seed_date_dim(engine, SEED_START_DATE, end)

    assert len(conn.calls) == 2
    last_batch_rows = sum(1 for k in conn.calls[-1] if k.startswith("date_key_"))
    assert last_batch_rows == 1
