from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd

from mimic_datawarehouse.etl import DEFAULT_ROW_CAP, load_all_staging_tables, load_staging_table
from mimic_datawarehouse.tables import discover_tables

DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "raw" / "mimic-iv-clinical-database-demo-2.2"


def test_loads_every_discovered_table_by_default():
    expected_names = {t.name for t in discover_tables(DATA_DIR)}

    with patch("mimic_datawarehouse.etl.load_staging_table", return_value=42) as mock_load:
        results = load_all_staging_tables(engine=object(), data_dir=DATA_DIR)

    assert set(results.keys()) == expected_names
    assert all(rows == 42 for rows in results.values())
    assert mock_load.call_count == len(expected_names)


def test_table_filter_limits_to_a_single_table():
    with patch("mimic_datawarehouse.etl.load_staging_table", return_value=7) as mock_load:
        results = load_all_staging_tables(engine=object(), data_dir=DATA_DIR, table_filter="patients")

    assert results == {"patients": 7}
    mock_load.assert_called_once()


def test_module_filter_limits_to_icu_tables():
    icu_names = {t.name for t in discover_tables(DATA_DIR, ("icu",))}

    with patch("mimic_datawarehouse.etl.load_staging_table", return_value=1):
        results = load_all_staging_tables(engine=object(), data_dir=DATA_DIR, modules=("icu",))

    assert set(results.keys()) == icu_names


def test_default_row_cap_is_forwarded_to_each_table():
    with patch("mimic_datawarehouse.etl.load_staging_table", return_value=5) as mock_load:
        load_all_staging_tables(engine=object(), data_dir=DATA_DIR, table_filter="patients")

    assert mock_load.call_args.kwargs["row_cap"] == DEFAULT_ROW_CAP


def _run_load(chunks, *, row_cap):
    table = SimpleNamespace(name="patients", csv_path=Path("patients.csv.gz"))
    with (
        patch("mimic_datawarehouse.etl.pd.read_csv", return_value=iter(chunks)),
        patch("pandas.DataFrame.to_sql"),
    ):
        return load_staging_table(MagicMock(), table, row_cap=row_cap)


def test_row_cap_truncates_an_oversized_source_and_stops_reading():
    chunks = [pd.DataFrame({"subject_id": range(400)}) for _ in range(4)]  # 1,600 rows
    assert _run_load(chunks, row_cap=1000) == 1000


def test_row_cap_leaves_a_source_under_the_cap_whole():
    chunks = [pd.DataFrame({"subject_id": range(300)})]
    assert _run_load(chunks, row_cap=1000) == 300


def test_row_cap_zero_disables_the_limit():
    chunks = [pd.DataFrame({"subject_id": range(800)}) for _ in range(3)]  # 2,400 rows
    assert _run_load(chunks, row_cap=0) == 2400
