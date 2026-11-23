"""Tests for reload.truncatable_tables/truncate_all -- the --fresh-reload building
blocks. No real database: truncate_all takes a mock engine and asserts on the SQL text
it would execute (mirrors how test_warehouse.py checks render_ddl's output as a string)."""

from __future__ import annotations

from unittest.mock import MagicMock

from mimic_datawarehouse.reload import PRESERVED_TABLES, truncatable_tables, truncate_all
from mimic_datawarehouse.staging import STAGING_TABLE_COLUMNS
from mimic_datawarehouse.warehouse import WAREHOUSE_TABLE_ORDER


def test_preserves_etl_process_log_only():
    assert PRESERVED_TABLES == {"etl_process_log"}


def test_every_staging_table_is_included():
    tables = truncatable_tables()
    for raw_table in STAGING_TABLE_COLUMNS:
        assert f'"staging"."stg_{raw_table}"' in tables


def test_every_warehouse_table_except_etl_process_log_is_included():
    tables = truncatable_tables()
    for name in WAREHOUSE_TABLE_ORDER:
        qualified = f'"warehouse"."{name}"'
        if name == "etl_process_log":
            assert qualified not in tables
        else:
            assert qualified in tables


def test_etl_process_log_never_appears_anywhere_in_the_list():
    tables = truncatable_tables()
    assert not any("etl_process_log" in t for t in tables)


def test_truncate_all_issues_one_statement_naming_every_table_and_excluding_the_log():
    engine = MagicMock()
    conn = engine.begin.return_value.__enter__.return_value

    result = truncate_all(engine)

    conn.execute.assert_called_once()
    (executed,), _kwargs = conn.execute.call_args
    sql = str(executed)
    assert sql.startswith("TRUNCATE TABLE")
    assert "RESTART IDENTITY" in sql
    assert "CASCADE" in sql
    assert '"warehouse"."etl_process_log"' not in sql
    assert '"staging"."stg_admissions"' in sql
    assert '"warehouse"."fact_admission"' in sql
    assert result == truncatable_tables()
