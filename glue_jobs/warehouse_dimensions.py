"""Glue job 2: `staging.stg_*` -> `warehouse.dim_*` (upsert / SCD Type 1).

PySpark equivalent of mimic_datawarehouse.etl_warehouse.load_all_dimensions.
DIMENSION_COLUMN_MAP below is a deliberate copy of the same-named table in
src/mimic_datawarehouse/etl_warehouse.py, translated from SQL select-expressions to
Spark `selectExpr` strings (mostly identical -- Spark SQL accepts the same `::type`
Postgres cast syntax used there). Keep the two in sync if column mappings change.

`dim_date` is excluded: it's a generated calendar spine with no staging source (see
seed_date_dim.py), seeded from the local CLI, not from Glue.

Job arguments:
  --RDS_HOST / --RDS_PORT / --RDS_DATABASE / --RDS_USER / --RDS_PASSWORD
  --TABLE   a single warehouse dimension table name, or the sentinel "none" for all of
             them (see glue_common.NO_VALUE -- this can't be a truly optional argument)
  --FULL_RUN_ID / --DATASET   logged per table to warehouse.etl_process_log, see
             glue_common.log_timed_call and cli.py's _next_full_run_id.
"""

from __future__ import annotations

import sys

from glue_common import (
    ensure_landing_schema,
    init_glue_job,
    log_timed_call,
    merge_into_target,
    optional_arg,
    read_table,
)
from pyspark.sql import functions as F

WAREHOUSE_SCHEMA = "warehouse"
STAGING_SCHEMA = "staging"
JOB_NAME = "glue_load_warehouse_dimensions"

# warehouse_table -> (staging_table, primary_key, [(warehouse_col, staging_selectExpr), ...])
DIMENSION_COLUMN_MAP: dict[str, tuple[str, tuple[str, ...], list[tuple[str, str]]]] = {
    "dim_hcpcs": (
        "stg_d_hcpcs",
        ("code",),
        [
            ("code", "code"),
            ("category", "cast(category as bigint)"),
            ("long_description", "long_description"),
            ("short_description", "short_description"),
        ],
    ),
    "dim_diagnosis": (
        "stg_d_icd_diagnoses",
        ("icd_code", "icd_version"),
        [("icd_code", "icd_code"), ("icd_version", "icd_version"), ("long_title", "long_title")],
    ),
    "dim_procedure": (
        "stg_d_icd_procedures",
        ("icd_code", "icd_version"),
        [("icd_code", "icd_code"), ("icd_version", "icd_version"), ("long_title", "long_title")],
    ),
    "dim_lab_item": (
        "stg_d_labitems",
        ("item_id",),
        [("item_id", "itemid"), ("label", "label"), ("fluid", "fluid"), ("category", "category")],
    ),
    "dim_patient": (
        "stg_patients",
        ("subject_id",),
        [
            ("subject_id", "subject_id"),
            ("gender", "gender"),
            ("anchor_age", "anchor_age"),
            ("anchor_year", "anchor_year"),
            ("anchor_year_group", "anchor_year_group"),
            ("dod", "cast(dod as date)"),
        ],
    ),
    "dim_provider": ("stg_provider", ("provider_id",), [("provider_id", "provider_id")]),
    "dim_caregiver": ("stg_caregiver", ("caregiver_id",), [("caregiver_id", "caregiver_id")]),
    "dim_chart_item": (
        "stg_d_items",
        ("item_id",),
        [
            ("item_id", "itemid"),
            ("label", "label"),
            ("abbreviation", "abbreviation"),
            ("links_to", "linksto"),
            ("category", "category"),
            ("unit_name", "unitname"),
            ("param_type", "param_type"),
            ("low_normal_value", "cast(lownormalvalue as double)"),
            ("high_normal_value", "cast(highnormalvalue as double)"),
        ],
    ),
}


def load_dimension(spark, args: dict, warehouse_table: str) -> int:
    staging_table, primary_key, column_map = DIMENSION_COLUMN_MAP[warehouse_table]
    src = read_table(spark, args, STAGING_SCHEMA, staging_table)

    select_exprs = [f"{expr} AS `{col}`" for col, expr in column_map]
    df = src.selectExpr(*select_exprs)
    df = df.withColumn("created_ts", F.current_timestamp()).withColumn("updated_ts", F.current_timestamp())
    df = df.withColumn("created_by", F.lit(JOB_NAME)).withColumn("updated_by", F.lit(JOB_NAME))

    update_cols = [col for col, _ in column_map if col not in primary_key]
    return merge_into_target(
        df,
        args,
        schema=WAREHOUSE_SCHEMA,
        target_table=warehouse_table,
        conflict_cols=list(primary_key),
        update_cols=update_cols,
        job_name=JOB_NAME,
    )


def main() -> None:
    _glue_context, spark, job, args = init_glue_job(
        sys.argv,
        [
            "RDS_HOST", "RDS_PORT", "RDS_DATABASE", "RDS_USER", "RDS_PASSWORD", "RDS_AUTH_MODE", "AWS_REGION",
            "TABLE", "FULL_RUN_ID", "DATASET",
        ],
    )
    ensure_landing_schema(args)
    table_filter = optional_arg(args, "TABLE")
    full_run_id = int(args["FULL_RUN_ID"])

    tables = [table_filter] if table_filter else list(DIMENSION_COLUMN_MAP)
    for warehouse_table in tables:
        staging_table = DIMENSION_COLUMN_MAP[warehouse_table][0]
        rows = log_timed_call(
            args,
            full_run_id=full_run_id,
            phase="B:dimensions",
            process_type="dim",
            process_name=warehouse_table,
            fn=lambda t=warehouse_table: load_dimension(spark, args, t),
            source_table=(STAGING_SCHEMA, staging_table),
            target_table=(WAREHOUSE_SCHEMA, warehouse_table),
        )
        print(f"Upserted {warehouse_table}: {rows} rows")

    job.commit()


if __name__ == "__main__":
    main()
