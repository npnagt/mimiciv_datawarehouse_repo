"""ETL: truncate-and-reload the `staging.stg_<table>` tables from the raw CSV.gz files.

Unlike `loader.load_table` (which lets pandas create/replace ad hoc tables in the
hosp/icu schemas), staging tables already exist with a fixed structure (see staging.py:
explicit types + eventual PK/FK use downstream). So each table is TRUNCATEd -- not
dropped/recreated -- and then loaded with `if_exists="append"`, preserving that structure.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
from sqlalchemy import Engine, text

from .staging import STAGING_TABLE_COLUMNS, STAGING_SCHEMA, staging_table_name
from .tables import MODULES, MimicTable, discover_tables

logger = logging.getLogger(__name__)

DEFAULT_CHUNKSIZE = 50_000

# Any raw table with more than this many rows is truncated to this many on load. Mirrors
# the rule the lakehouse repo applies to its Gold fact tables (and glue_jobs/staging_load.py's
# DEFAULT_ROW_CAP): it keeps demo and full runs comparable and stops a full-dataset Aurora
# load from ballooning in cost/time. 0 disables the cap; tables under it are unaffected.
DEFAULT_ROW_CAP = 1_000_000


def _text_column_dtypes(table_name: str) -> dict[str, type]:
    """Force every column declared TEXT to be read as a raw string.

    Without this, pandas' own dtype inference runs independent of our target schema: a
    TEXT column whose values happen to look numeric but has at least one missing value
    (e.g. d_hcpcs.category) gets inferred as float64 and silently corrupted on load --
    "1" becomes "1.0". Columns declared BIGINT/DOUBLE PRECISION are left to normal
    inference since to_sql coerces those correctly.
    """
    return {col: str for col, pg_type in STAGING_TABLE_COLUMNS[table_name] if pg_type == "TEXT"}


def load_staging_table(
    engine: Engine,
    table: MimicTable,
    *,
    chunksize: int = DEFAULT_CHUNKSIZE,
    row_cap: int = DEFAULT_ROW_CAP,
) -> int:
    """Truncate `staging.stg_<table.name>` then stream `table.csv_path` into it.

    At most `row_cap` rows are loaded (0 = no limit); once the cap is reached the
    remaining CSV chunks are not read, so an oversized source never fully materializes.

    Returns the row count loaded."""
    target = staging_table_name(table.name)
    with engine.begin() as conn:
        conn.execute(text(f'TRUNCATE TABLE {STAGING_SCHEMA}."{target}"'))

    total_rows = 0
    reader = pd.read_csv(
        table.csv_path,
        compression="gzip",
        chunksize=chunksize,
        low_memory=False,
        dtype=_text_column_dtypes(table.name),
    )
    for chunk in reader:
        if row_cap and total_rows + len(chunk) > row_cap:
            chunk = chunk.iloc[: row_cap - total_rows]
        chunk.to_sql(target, engine, schema=STAGING_SCHEMA, if_exists="append", index=False)
        total_rows += len(chunk)
        logger.info("%s.%s: loaded %d rows (running total %d)", STAGING_SCHEMA, target, len(chunk), total_rows)
        if row_cap and total_rows >= row_cap:
            logger.info("%s.%s: reached row cap (%d) -- skipping the rest of the source", STAGING_SCHEMA, target, row_cap)
            break
    return total_rows


def load_all_staging_tables(
    engine: Engine,
    data_dir: Path,
    *,
    modules: tuple[str, ...] = MODULES,
    table_filter: str | None = None,
    chunksize: int = DEFAULT_CHUNKSIZE,
    row_cap: int = DEFAULT_ROW_CAP,
) -> dict[str, int]:
    """Truncate-and-reload every discovered raw table into its matching staging table,
    loading at most `row_cap` rows each (0 = no limit). Returns {raw_table_name: rows_loaded}."""
    tables = discover_tables(data_dir, modules)
    if table_filter:
        tables = [t for t in tables if t.name == table_filter]

    results: dict[str, int] = {}
    for table in tables:
        results[table.name] = load_staging_table(engine, table, chunksize=chunksize, row_cap=row_cap)
    return results
