from mimic_datawarehouse.etl_warehouse import (
    AGGREGATE_LOAD_ORDER,
    AGGREGATE_LOAD_SQL,
    DIMENSION_COLUMN_MAP,
    FACT_LOAD_SQL,
    _date_key,
    _hours_between,
    _minutes_between,
)
from mimic_datawarehouse.warehouse import WAREHOUSE_TABLES

_AUDIT_COLUMNS = {"created_ts", "updated_ts", "created_by", "updated_by"}
_ALL_DIM_TABLES = {name for name in WAREHOUSE_TABLES if name.startswith("dim_") and name != "dim_date"}
_ALL_FACT_TABLES = {name for name in WAREHOUSE_TABLES if name.startswith("fact_")}
_ALL_AGG_TABLES = {name for name in WAREHOUSE_TABLES if name.startswith("agg_")}


def test_dimension_column_map_covers_every_loadable_dimension():
    """dim_date is intentionally excluded -- it's seeded, not loaded from staging."""
    assert set(DIMENSION_COLUMN_MAP.keys()) == _ALL_DIM_TABLES


def test_dimension_column_map_covers_every_non_audit_column():
    for table_name, (_staging_table, column_map) in DIMENSION_COLUMN_MAP.items():
        mapped_cols = {c for c, _ in column_map}
        expected_cols = {c for c, _ in WAREHOUSE_TABLES[table_name]["columns"]} - _AUDIT_COLUMNS
        assert mapped_cols == expected_cols, f"{table_name}: column map doesn't match warehouse columns"


def test_dimension_natural_key_is_subset_of_mapped_columns():
    for table_name, (_staging_table, column_map) in DIMENSION_COLUMN_MAP.items():
        mapped_cols = {c for c, _ in column_map}
        natural_key = set(WAREHOUSE_TABLES[table_name]["primary_key"])
        assert natural_key <= mapped_cols


def test_fact_load_sql_covers_every_fact_table():
    assert set(FACT_LOAD_SQL.keys()) == _ALL_FACT_TABLES


def test_every_fact_insert_targets_its_own_table_and_conflict_handles():
    for table_name, statements in FACT_LOAD_SQL.items():
        insert_stmt = statements[-1]
        assert f"INSERT INTO warehouse.{table_name} (" in insert_stmt, f"{table_name}: unexpected INSERT target"
        assert "ON CONFLICT" in insert_stmt or "NOT EXISTS" in insert_stmt, (
            f"{table_name}: insert has neither ON CONFLICT nor a NOT EXISTS anti-join -- would not be insert-only"
        )


def test_date_key_clamps_to_seed_range():
    expr = _date_key("x")
    assert "2100-01-01" in expr
    assert "2230-12-31" in expr
    assert "YYYYMMDD" in expr


def test_minutes_and_hours_between_reference_both_columns():
    assert "'a'" not in _minutes_between("a", "b")  # sanity: no accidental quoting
    expr = _minutes_between("a", "b")
    assert "a" in expr and "b" in expr and "60.0" in expr
    expr = _hours_between("a", "b")
    assert "a" in expr and "b" in expr and "3600.0" in expr


def test_aggregate_load_sql_covers_every_aggregate_table():
    assert set(AGGREGATE_LOAD_SQL.keys()) == _ALL_AGG_TABLES


def test_aggregate_load_order_lists_every_aggregate_exactly_once():
    assert set(AGGREGATE_LOAD_ORDER) == _ALL_AGG_TABLES
    assert len(AGGREGATE_LOAD_ORDER) == len(set(AGGREGATE_LOAD_ORDER))


def test_agg_admission_monthly_runs_after_agg_admission_daily():
    """It rolls up FROM agg_admission_daily, not directly from fact_admission."""
    order = {name: i for i, name in enumerate(AGGREGATE_LOAD_ORDER)}
    assert order["agg_admission_daily"] < order["agg_admission_monthly"]
    assert "warehouse.agg_admission_daily" in AGGREGATE_LOAD_SQL["agg_admission_monthly"]


def test_every_aggregate_upsert_targets_its_own_table_with_on_conflict():
    for table_name, sql in AGGREGATE_LOAD_SQL.items():
        assert f"INSERT INTO warehouse.{table_name} (" in sql, f"{table_name}: unexpected INSERT target"
        assert "ON CONFLICT" in sql, f"{table_name}: aggregates must upsert (refresh in place), not insert-only"
