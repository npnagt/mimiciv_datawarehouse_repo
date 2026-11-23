"""Fresh-reload: TRUNCATE every staging/warehouse data table so the next load starts
from empty, without dropping (and having to recreate) any table definition.

Deliberately excludes `warehouse.etl_process_log`: that table is the append-only
cross-run timing/row-count log (see warehouse.py's table comment and README.md's "Job
timing log") used to compare results across multiple loads -- a fresh-reload must reset
the *data*, never the history that measures it.
"""

from __future__ import annotations

from sqlalchemy import Engine, text

from .staging import STAGING_SCHEMA, STAGING_TABLE_COLUMNS, staging_table_name
from .warehouse import WAREHOUSE_SCHEMA, WAREHOUSE_TABLE_ORDER

# Never truncated by fresh-reload, no matter what -- see module docstring.
PRESERVED_TABLES = frozenset({"etl_process_log"})


def truncatable_tables() -> list[str]:
    """Every table a fresh-reload will TRUNCATE, as fully-qualified '"schema"."table"'
    strings: all of `staging.stg_*` plus all of `warehouse.*` except PRESERVED_TABLES.
    Exposed separately from truncate_all() so a caller can preview/log the list (e.g. a
    --dry-run) without actually touching the database."""
    staging_tables = [f'"{STAGING_SCHEMA}"."{staging_table_name(t)}"' for t in STAGING_TABLE_COLUMNS]
    warehouse_tables = [
        f'"{WAREHOUSE_SCHEMA}"."{t}"' for t in WAREHOUSE_TABLE_ORDER if t not in PRESERVED_TABLES
    ]
    return staging_tables + warehouse_tables


def truncate_all(engine: Engine) -> list[str]:
    """TRUNCATE every table truncatable_tables() lists, in a single statement --
    Postgres doesn't need FK dependency order when every FK-related table is truncated
    together in one TRUNCATE, which is why warehouse.etl_process_log (no FK dependents,
    per its own table comment) is the only table that's safe to leave out. RESTART
    IDENTITY resets the GENERATED ALWAYS AS IDENTITY surrogate keys (e.g.
    fact_prescription.prescription_id) so a fresh reload also gets clean sequence
    numbers, not a continuation of the old ones. CASCADE is a safety net for any future
    table not in this list that ends up with an FK into one that is -- a no-op today.

    Returns the list of tables truncated (same as truncatable_tables())."""
    tables = truncatable_tables()
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {', '.join(tables)} RESTART IDENTITY CASCADE"))
    return tables
