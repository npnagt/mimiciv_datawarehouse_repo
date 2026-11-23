import csv
import gzip
import re
from pathlib import Path

import pandas as pd

from mimic_datawarehouse.staging import STAGING_TABLE_COLUMNS, staging_table_name
from mimic_datawarehouse.tables import discover_tables

DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "raw" / "mimic-iv-clinical-database-demo-2.2"
_INTEGER_RE = re.compile(r"^-?\d+$")


def test_covers_exactly_the_discovered_tables():
    discovered = {t.name for t in discover_tables(DATA_DIR)}
    assert set(STAGING_TABLE_COLUMNS.keys()) == discovered


def test_columns_match_raw_csv_header_name_and_order():
    for table in discover_tables(DATA_DIR):
        with gzip.open(table.csv_path, "rt", newline="") as f:
            header = next(csv.reader(f))
        defined = [name for name, _ in STAGING_TABLE_COLUMNS[table.name]]
        assert defined == header, f"{table.name}: column mismatch"


def test_staging_table_name_prefix():
    assert staging_table_name("patients") == "stg_patients"


def test_bigint_columns_actually_hold_only_integers_in_raw_data():
    """Guards against blindly trusting a source DDL's declared type: the raw Athena DDL
    this staging schema was originally transcribed from declared d_icd_procedures.icd_code
    as bigint (wrong -- ICD-10-PCS codes are alphanumeric, e.g. "001U3J6"), and
    inputevents.totalamount / prescriptions.form_val_disp as bigint (wrong -- real values
    include fractional amounts). All three only surfaced once real data was loaded."""
    tables_by_name = {t.name: t.csv_path for t in discover_tables(DATA_DIR)}
    violations = []
    for table_name, columns in STAGING_TABLE_COLUMNS.items():
        bigint_cols = [name for name, pg_type in columns if pg_type == "BIGINT"]
        if not bigint_cols:
            continue
        df = pd.read_csv(tables_by_name[table_name], compression="gzip", usecols=bigint_cols, dtype=str)
        for col in bigint_cols:
            non_null = df[col].dropna()
            bad = non_null[~non_null.str.match(_INTEGER_RE)]
            if len(bad):
                violations.append(f"{table_name}.{col}: e.g. {bad.iloc[0]!r}")
    assert not violations, "BIGINT columns with non-integer values in real data:\n" + "\n".join(violations)


def test_double_precision_columns_actually_hold_only_numbers_in_raw_data():
    """Same guard as the BIGINT check, for DOUBLE PRECISION: prescriptions.form_val_disp
    can hold dose *ranges* for variable/PRN dosing (e.g. "1-2"), which no numeric type
    can hold -- only surfaced once real data was loaded."""
    tables_by_name = {t.name: t.csv_path for t in discover_tables(DATA_DIR)}
    violations = []
    for table_name, columns in STAGING_TABLE_COLUMNS.items():
        double_cols = [name for name, pg_type in columns if pg_type == "DOUBLE PRECISION"]
        if not double_cols:
            continue
        df = pd.read_csv(tables_by_name[table_name], compression="gzip", usecols=double_cols, dtype=str)
        for col in double_cols:
            non_null = df[col].dropna()
            bad = non_null[pd.to_numeric(non_null, errors="coerce").isna()]
            if len(bad):
                violations.append(f"{table_name}.{col}: e.g. {bad.iloc[0]!r}")
    assert not violations, "DOUBLE PRECISION columns with non-numeric values in real data:\n" + "\n".join(violations)


def test_text_columns_are_not_silently_upcast_to_float_by_pandas():
    """Guards against a real corruption bug: reading a CSV with pandas' default dtype
    inference (independent of our declared schema) upcasts a TEXT column to float64 if
    its values happen to look numeric but have at least one missing value (e.g.
    d_hcpcs.category) -- "1" silently becomes "1.0" on load. etl.load_staging_table
    fixes this by forcing dtype=str for every TEXT column; this test fails if that
    dtype override is ever removed or bypassed."""
    from mimic_datawarehouse.etl import _text_column_dtypes

    tables_by_name = {t.name: t.csv_path for t in discover_tables(DATA_DIR)}
    violations = []
    for table_name, columns in STAGING_TABLE_COLUMNS.items():
        text_cols = [name for name, pg_type in columns if pg_type == "TEXT"]
        if not text_cols:
            continue
        df = pd.read_csv(
            tables_by_name[table_name],
            compression="gzip",
            usecols=text_cols,
            low_memory=False,
            dtype=_text_column_dtypes(table_name),
        )
        for col in text_cols:
            if df[col].dtype.kind == "f":
                violations.append(f"{table_name}.{col}")
    assert not violations, "TEXT columns upcast to float despite dtype override:\n" + "\n".join(violations)
