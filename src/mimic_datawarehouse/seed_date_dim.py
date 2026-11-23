"""Seed `warehouse.dim_date`: one row per calendar day over a fixed range.

MIMIC-IV date-shifts every timestamp per patient for de-identification, so real event
dates in this dataset fall in the far future (see the module docstring in warehouse.py).
The default range, `2100-01-01` to `2230-12-31`, covers that shifted window. It ends at
2230, not 2210, because the full MIMIC-IV 3.1 dataset shifts some events out to ~2215
(max dischtime 2214-12-24) -- a 2210 endpoint dropped ~275 admissions (and their
transfers/services) on any dim_date join; 2230 leaves margin and the extra ~7,300
calendar rows cost ~nothing. (The 100-patient demo dataset stays within 2210.) Unlike the
staging/dim/fact loaders, this isn't sourced from any raw CSV -- it's a generated
calendar spine -- so it's upserted (INSERT ... ON CONFLICT DO UPDATE), not truncated:
fact tables hold FKs into it, and TRUNCATE would either fail or require CASCADE.
"""

from __future__ import annotations

import calendar
from datetime import date, timedelta

from sqlalchemy import Engine, text

from .warehouse import WAREHOUSE_SCHEMA, WAREHOUSE_TABLES

SEED_START_DATE = date(2100, 1, 1)
SEED_END_DATE = date(2230, 12, 31)

_MONTH_NAMES = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]  # fmt: skip
_DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

_DATE_DIM_COLUMNS = [name for name, _ in WAREHOUSE_TABLES["dim_date"]["columns"]]


def _date_dim_row(day: date) -> dict:
    iso_weekday = day.isoweekday()  # 1=Monday .. 7=Sunday
    week_start = day - timedelta(days=iso_weekday - 1)
    week_end = week_start + timedelta(days=6)
    quarter = (day.month - 1) // 3 + 1
    last_day = calendar.monthrange(day.year, day.month)[1]

    return {
        "date_key": int(day.strftime("%Y%m%d")),
        "full_date": day,
        "day_of_week": iso_weekday,
        "day_name": _DAY_NAMES[iso_weekday - 1],
        "day_name_short": _DAY_NAMES[iso_weekday - 1][:3],
        "day_of_month": day.day,
        "day_of_year": day.timetuple().tm_yday,
        "week_of_year": day.isocalendar()[1],
        "week_start_date": week_start,
        "week_end_date": week_end,
        "month_number": day.month,
        "month_name": _MONTH_NAMES[day.month - 1],
        "month_name_short": _MONTH_NAMES[day.month - 1][:3],
        "first_day_of_month": day.replace(day=1),
        "last_day_of_month": day.replace(day=last_day),
        "quarter_number": quarter,
        "quarter_name": f"Q{quarter}",
        "year": day.year,
        "is_weekday": iso_weekday <= 5,
        "is_weekend": iso_weekday > 5,
        # MIMIC-IV dates are shifted for de-identification, so calendar-holiday
        # attribution against them isn't meaningful -- see warehouse.py's dim_date note.
        "is_holiday": False,
        "holiday_name": None,
        # No separate fiscal calendar policy specified; fiscal year = calendar year.
        "fiscal_month": day.month,
        "fiscal_quarter": quarter,
        "fiscal_year": day.year,
    }


def generate_date_dim_rows(start: date = SEED_START_DATE, end: date = SEED_END_DATE) -> list[dict]:
    """Return one dict per calendar day in [start, end], keyed by every dim_date column
    except the audit columns (created_ts/updated_ts/created_by/updated_by)."""
    if end < start:
        raise ValueError(f"end ({end}) must not be before start ({start})")
    rows = []
    day = start
    one_day = timedelta(days=1)
    while day <= end:
        rows.append(_date_dim_row(day))
        day += one_day
    return rows


# Rows per multi-row INSERT statement. The naive one-execute()-per-row loop this
# replaced took ~33 minutes for the default ~47,800-row range against a real Aurora
# cluster (one network round trip per day) -- each round trip's latency dominates, not
# the database's own insert cost, so batching many rows into one multi-row VALUES
# clause cuts the round-trip count (and thus the wall-clock time) by ~_BATCH_SIZE.
_BATCH_SIZE = 1000


def seed_date_dim(engine: Engine, start: date = SEED_START_DATE, end: date = SEED_END_DATE) -> int:
    """Upsert one row per calendar day in [start, end] into warehouse.dim_date, in
    batches of _BATCH_SIZE rows per statement (one round trip per batch, not per row).

    Safe to rerun: existing rows are updated in place (their date_key never changes),
    new rows are inserted. Returns the number of days processed."""
    rows = generate_date_dim_rows(start, end)
    non_audit_cols = [c for c in _DATE_DIM_COLUMNS if c not in ("created_ts", "updated_ts", "created_by", "updated_by")]
    col_list = ", ".join(f'"{c}"' for c in non_audit_cols)
    update_list = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in non_audit_cols if c != "date_key")

    with engine.begin() as conn:
        for batch_start in range(0, len(rows), _BATCH_SIZE):
            batch = rows[batch_start : batch_start + _BATCH_SIZE]
            values_clauses = []
            params: dict = {}
            for i, row in enumerate(batch):
                placeholders = ", ".join(f":{c}_{i}" for c in non_audit_cols)
                values_clauses.append(f"({placeholders}, now(), now(), 'seed_date_dim', 'seed_date_dim')")
                params.update({f"{c}_{i}": row[c] for c in non_audit_cols})
            sql = text(
                f'INSERT INTO {WAREHOUSE_SCHEMA}."dim_date" ({col_list}, created_ts, updated_ts, created_by, updated_by)\n'
                f"VALUES {', '.join(values_clauses)}\n"
                f'ON CONFLICT ("date_key") DO UPDATE SET {update_list}, updated_ts = now(), updated_by = \'seed_date_dim\''
            )
            conn.execute(sql, params)
    return len(rows)
