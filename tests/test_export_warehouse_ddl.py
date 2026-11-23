from pathlib import Path

from mimic_datawarehouse.warehouse import render_ddl

DDL_PATH = Path(__file__).resolve().parents[1] / "sql" / "warehouse_tables.sql"


def test_exported_ddl_file_matches_warehouse_py():
    """Guards against editing WAREHOUSE_TABLES without regenerating the exported file
    via scripts/export_warehouse_ddl.py."""
    assert DDL_PATH.is_file(), "sql/warehouse_tables.sql is missing -- run scripts/export_warehouse_ddl.py"
    assert DDL_PATH.read_text(encoding="utf-8") == render_ddl()
