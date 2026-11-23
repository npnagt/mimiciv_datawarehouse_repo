from pathlib import Path

from mimic_datawarehouse.staging import render_ddl

DDL_PATH = Path(__file__).resolve().parents[1] / "sql" / "staging_tables.sql"


def test_exported_ddl_file_matches_staging_py():
    """Guards against editing STAGING_TABLE_COLUMNS without regenerating the exported
    file via scripts/export_staging_ddl.py."""
    assert DDL_PATH.is_file(), "sql/staging_tables.sql is missing -- run scripts/export_staging_ddl.py"
    assert DDL_PATH.read_text(encoding="utf-8") == render_ddl()
