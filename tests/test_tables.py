from pathlib import Path

from mimic_datawarehouse.tables import discover_tables

DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "raw" / "mimic-iv-clinical-database-demo-2.2"


def test_discovers_hosp_and_icu_tables():
    tables = discover_tables(DATA_DIR)
    schemas = {t.schema for t in tables}
    assert schemas == {"hosp", "icu"}
    names = {t.name for t in tables}
    assert "patients" in names
    assert "icustays" in names


def test_each_table_csv_exists():
    for table in discover_tables(DATA_DIR):
        assert table.csv_path.is_file()
