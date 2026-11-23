import re

import pandas as pd
import pytest

from mimic_datawarehouse.warehouse import WAREHOUSE_TABLE_ORDER, WAREHOUSE_TABLES, render_ddl

DATA_DIR = "data/raw/mimic-iv-clinical-database-demo-2.2"


def _unique_key_expr_columns(table_name: str) -> list[str]:
    """Column names referenced by a table's unique_key_exprs (each expression is
    COALESCE("col", sentinel) -- see warehouse.py's WAREHOUSE_TABLES key comment).
    Extracted from the real declared exprs, not copy-pasted, so a test using this can't
    silently drift from what the live expression index actually enforces."""
    _index_name, exprs = WAREHOUSE_TABLES[table_name]["unique_key_exprs"][0]
    return [re.match(r'COALESCE\("(\w+)"', e).group(1) for e in exprs]


def test_every_table_appears_exactly_once_in_order():
    assert set(WAREHOUSE_TABLE_ORDER) == set(WAREHOUSE_TABLES)
    assert len(WAREHOUSE_TABLE_ORDER) == len(set(WAREHOUSE_TABLE_ORDER))


def test_foreign_keys_reference_tables_created_earlier():
    """A table's FKs must all point at tables earlier in WAREHOUSE_TABLE_ORDER, or
    creation will fail with 'relation does not exist'."""
    position = {name: i for i, name in enumerate(WAREHOUSE_TABLE_ORDER)}
    for name in WAREHOUSE_TABLE_ORDER:
        for _local_cols, ref_table, _ref_cols in WAREHOUSE_TABLES[name]["foreign_keys"]:
            assert ref_table in position, f"{name} FK references unknown table {ref_table}"
            assert position[ref_table] < position[name], f"{name} FK to {ref_table} is out of dependency order"


def test_foreign_key_columns_exist_and_column_counts_match():
    for name, table_def in WAREHOUSE_TABLES.items():
        own_cols = {c for c, _ in table_def["columns"]}
        for local_cols, ref_table, ref_cols in table_def["foreign_keys"]:
            assert len(local_cols) == len(ref_cols), f"{name} -> {ref_table} FK column count mismatch"
            assert set(local_cols) <= own_cols, f"{name} FK references its own missing column(s) {local_cols}"
            ref_table_cols = {c for c, _ in WAREHOUSE_TABLES[ref_table]["columns"]}
            assert set(ref_cols) <= ref_table_cols, f"{name} -> {ref_table} FK references missing column(s) {ref_cols}"


def test_primary_key_columns_exist():
    for name, table_def in WAREHOUSE_TABLES.items():
        own_cols = {c for c, _ in table_def["columns"]}
        assert set(table_def["primary_key"]) <= own_cols, f"{name} primary key references missing column(s)"


def test_foreign_keys_target_the_referenced_tables_primary_key():
    """Every FK's ref_cols must equal (as a set) the referenced table's actual PK --
    Postgres requires a FK target to be a unique/PK constraint."""
    for name, table_def in WAREHOUSE_TABLES.items():
        for _local_cols, ref_table, ref_cols in table_def["foreign_keys"]:
            ref_pk = WAREHOUSE_TABLES[ref_table]["primary_key"]
            assert set(ref_cols) == set(ref_pk), (
                f"{name} -> {ref_table} FK columns {ref_cols} don't match {ref_table}'s primary key {ref_pk}"
            )


@pytest.mark.parametrize(
    "table_name,csv_path,key_columns",
    [
        ("fact_admission", "hosp/admissions.csv.gz", ["hadm_id"]),
        ("fact_diagnosis", "hosp/diagnoses_icd.csv.gz", ["hadm_id", "seq_num"]),
        ("fact_procedure", "hosp/procedures_icd.csv.gz", ["hadm_id", "seq_num"]),
        ("fact_hcpcs_event", "hosp/hcpcsevents.csv.gz", ["hadm_id", "seq_num"]),
        ("fact_drg_assignment", "hosp/drgcodes.csv.gz", ["hadm_id", "drg_type", "drg_code"]),
        ("fact_provider_order", "hosp/poe.csv.gz", ["poe_id"]),
        ("fact_provider_order_mini_detail", "hosp/poe_detail.csv.gz", ["poe_id", "field_name"]),
        ("fact_pharmacy_order", "hosp/pharmacy.csv.gz", ["pharmacy_id"]),
        ("fact_medication_administration", "hosp/emar.csv.gz", ["emar_id"]),
        ("fact_lab_result", "hosp/labevents.csv.gz", ["labevent_id"]),
        ("fact_microbiology_result", "hosp/microbiologyevents.csv.gz", ["microevent_id"]),
        ("fact_transfer", "hosp/transfers.csv.gz", ["transfer_id"]),
        ("fact_service_assignment", "hosp/services.csv.gz", ["hadm_id", "transfertime"]),
        ("fact_icu_stay_accumulating", "icu/icustays.csv.gz", ["stay_id"]),
        ("dim_patient", "hosp/patients.csv.gz", ["subject_id"]),
        ("dim_provider", "hosp/provider.csv.gz", ["provider_id"]),
        ("dim_caregiver", "icu/caregiver.csv.gz", ["caregiver_id"]),
        ("dim_lab_item", "hosp/d_labitems.csv.gz", ["itemid"]),
        ("dim_chart_item", "icu/d_items.csv.gz", ["itemid"]),
        ("dim_hcpcs", "hosp/d_hcpcs.csv.gz", ["code"]),
        ("dim_diagnosis", "hosp/d_icd_diagnoses.csv.gz", ["icd_code", "icd_version"]),
        ("dim_procedure", "hosp/d_icd_procedures.csv.gz", ["icd_code", "icd_version"]),
    ],
)
def test_chosen_primary_key_is_actually_unique_in_raw_data(table_name, csv_path, key_columns):
    df = pd.read_csv(f"{DATA_DIR}/{csv_path}", compression="gzip", usecols=key_columns)
    assert len(df) == len(df.drop_duplicates(subset=key_columns)), (
        f"{table_name}: {key_columns} is not unique in {csv_path}"
    )
    assert not df[key_columns].isna().any(axis=None), f"{table_name}: {key_columns} contains NULLs in {csv_path}"


@pytest.mark.parametrize(
    "table_name,csv_path,warehouse_to_raw_columns",
    [
        (
            "fact_medication_administration_mini_detail",
            "hosp/emar_detail.csv.gz",
            {"emar_id": "emar_id", "emar_seq": "emar_seq", "parent_field_ordinal": "parent_field_ordinal"},
        ),
        (
            "fact_prescription",
            "hosp/prescriptions.csv.gz",
            {
                "subject_id": "subject_id",
                "hadm_id": "hadm_id",
                "pharmacy_id": "pharmacy_id",
                "poe_id": "poe_id",
                "poe_seq": "poe_seq",
                "start_time": "starttime",
                "drug": "drug",
            },
        ),
        (
            "fact_datetime_event",
            "icu/datetimeevents.csv.gz",
            {"stay_id": "stay_id", "item_id": "itemid", "chart_time": "charttime", "store_time": "storetime"},
        ),
        (
            "fact_input_event",
            "icu/inputevents.csv.gz",
            {
                "stay_id": "stay_id",
                "item_id": "itemid",
                "start_time": "starttime",
                "end_time": "endtime",
                "order_id": "orderid",
            },
        ),
        (
            "fact_ingredient_event",
            "icu/ingredientevents.csv.gz",
            {
                "stay_id": "stay_id",
                "item_id": "itemid",
                "start_time": "starttime",
                "end_time": "endtime",
                "order_id": "orderid",
            },
        ),
        (
            "fact_output_event",
            "icu/outputevents.csv.gz",
            {"stay_id": "stay_id", "item_id": "itemid", "chart_time": "charttime", "store_time": "storetime"},
        ),
        (
            "fact_procedure_event",
            "icu/procedureevents.csv.gz",
            {
                "stay_id": "stay_id",
                "item_id": "itemid",
                "start_time": "starttime",
                "end_time": "endtime",
                "order_id": "orderid",
            },
        ),
    ],
)
def test_declared_unique_key_is_actually_unique_in_raw_data(table_name, csv_path, warehouse_to_raw_columns):
    """Every unique_key_exprs entry on a surrogate-PK fact table should be unique in this
    (demo-scale) raw CSV -- a demo-scale sanity check, not a full guarantee: at
    full-dataset scale these grains DO occasionally collide for real (verified 64 exact
    duplicates in emar_detail.csv.gz, 66 in prescriptions.csv.gz -- see
    ux_med_admin_mini_detail_grain / ux_fact_prescription_grain's comments), which is
    exactly why the loader uses `ON CONFLICT ... DO NOTHING` against a NULL-safe
    COALESCE expression index instead of assuming the grain can never collide."""
    unique_key = _unique_key_expr_columns(table_name)
    assert unique_key == list(warehouse_to_raw_columns), (
        f"{table_name}: unique_key_exprs columns {unique_key} don't match this test's "
        f"expected grain {list(warehouse_to_raw_columns)} -- update whichever one is stale."
    )
    raw_columns = [warehouse_to_raw_columns[c] for c in unique_key]
    df = pd.read_csv(f"{DATA_DIR}/{csv_path}", compression="gzip", usecols=raw_columns, low_memory=False)
    assert len(df) == len(df.drop_duplicates(subset=raw_columns)), (
        f"{table_name}: declared unique_key_exprs grain {unique_key} is not unique in "
        f"{csv_path} even at demo scale"
    )


def test_fact_chart_observation_deliberately_has_no_unique_key_because_raw_data_collides():
    """Documents *why* fact_chart_observation is the one surrogate-PK fact with no
    unique_keys/unique_key_exprs entry: unlike its 7 siblings, its natural grain
    genuinely collides in the raw data at a rate (~12%) far above the occasional
    duplicate the other 7 tolerate via ON CONFLICT DO NOTHING. Guards against silently
    adding a unique constraint/index here later without re-verifying -- that would break
    the pipeline the moment two colliding rows load in the same batch."""
    assert WAREHOUSE_TABLES["fact_chart_observation"].get("unique_keys", []) == []
    assert WAREHOUSE_TABLES["fact_chart_observation"].get("unique_key_exprs", []) == []

    df = pd.read_csv(
        f"{DATA_DIR}/icu/chartevents.csv.gz",
        compression="gzip",
        usecols=["stay_id", "itemid", "charttime", "storetime"],
        low_memory=False,
    )
    key_cols = ["stay_id", "itemid", "charttime", "storetime"]
    assert len(df) != len(df.drop_duplicates(subset=key_cols)), (
        "(stay_id, item_id, chart_time, store_time) is now unique in chartevents.csv.gz -- "
        "if this holds on the full dataset too, fact_chart_observation could get a real "
        "unique_key_exprs entry like its siblings; see warehouse.py's WAREHOUSE_TABLES comment."
    )


def test_surrogate_key_tables_have_no_reliable_natural_key():
    """Documents *why* these specific tables use a surrogate identity PK: their only
    natural-key candidate has NULLs (fact_medication_administration_mini_detail) or no
    candidate at all. Guards against silently reverting to a natural key that isn't
    actually safe."""
    df = pd.read_csv(
        f"{DATA_DIR}/hosp/emar_detail.csv.gz", compression="gzip", usecols=["emar_id", "parent_field_ordinal"]
    )
    assert df["parent_field_ordinal"].isna().any(), (
        "parent_field_ordinal no longer has NULLs -- reconsider using "
        "(emar_id, parent_field_ordinal) as a natural PK for fact_medication_administration_mini_detail"
    )


def test_render_ddl_contains_every_table_and_a_foreign_key_clause():
    ddl = render_ddl()
    for name in WAREHOUSE_TABLE_ORDER:
        assert f'"{name}"' in ddl
    assert "FOREIGN KEY" in ddl
    assert "PRIMARY KEY" in ddl


def test_render_ddl_emits_unique_expr_index_only_for_tables_that_declare_one():
    ddl = render_ddl()
    # The 7 surrogate-PK facts with a nullable-column grain get a NULL-safe CREATE
    # UNIQUE INDEX on COALESCE expressions (see warehouse.py's WAREHOUSE_TABLES key
    # comment for why a plain inline UNIQUE (...) constraint isn't used here).
    assert 'CREATE UNIQUE INDEX IF NOT EXISTS "ux_med_admin_mini_detail_grain"' in ddl
    assert 'COALESCE("emar_id", \'\')' in ddl
    assert 'CREATE UNIQUE INDEX IF NOT EXISTS "ux_fact_input_event_grain"' in ddl
    assert 'COALESCE("order_id", -1)' in ddl
    # fact_chart_observation's table block must not contain a UNIQUE clause at all, and
    # no CREATE UNIQUE INDEX naming it must exist anywhere in the file -- slice out just
    # its CREATE TABLE statement for the inline check so this can't false-positive on
    # another table's UNIQUE clause appearing elsewhere in the file.
    start = ddl.index('CREATE TABLE IF NOT EXISTS warehouse."fact_chart_observation"')
    end = ddl.index(");", start)
    assert "UNIQUE" not in ddl[start:end]
    assert all(
        "fact_chart_observation" not in line
        for line in ddl.splitlines()
        if line.strip().startswith("CREATE UNIQUE INDEX")
    )
