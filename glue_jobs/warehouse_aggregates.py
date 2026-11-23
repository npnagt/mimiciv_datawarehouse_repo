"""Glue job 4: `warehouse.fact_*` -> `warehouse.agg_*` (rollup refresh).

PySpark equivalent of mimic_datawarehouse.etl_warehouse.AGGREGATE_LOAD_SQL /
refresh_all_aggregates. Each rollup is a full recompute (groupBy/agg) over the
already-loaded fact tables, upserted via the same merge_into_target helper the
dimension/fact jobs use.

agg_admission_monthly rolls up from agg_admission_daily, not directly from
fact_admission (coarser rolls up from finer, per the source design -- see
etl_warehouse.py) -- so AGGREGATE_LOAD_ORDER runs agg_admission_daily first, in-process,
same as today.

Job arguments:
  --RDS_HOST / --RDS_PORT / --RDS_DATABASE / --RDS_USER / --RDS_PASSWORD
  --TABLE   a single aggregate table name, or the sentinel "none" for all of them (see
             glue_common.NO_VALUE -- this can't be a truly optional argument)
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
JOB_NAME = "glue_refresh_warehouse_aggregates"


def audit_cols(df, job_name: str):
    return df.withColumn("created_ts", F.current_timestamp()).withColumn("updated_ts", F.current_timestamp())


def refresh_agg_admission_daily(spark, args) -> int:
    fact_admission = read_table(spark, args, WAREHOUSE_SCHEMA, "fact_admission")

    admits = (
        fact_admission.where(F.col("admit_date_key").isNotNull())
        .groupBy(F.col("admit_date_key").alias("date_key"), "admission_type")
        .agg(F.count("*").alias("admit_count"))
    )
    discharges = (
        fact_admission.where(F.col("disch_date_key").isNotNull())
        .groupBy(F.col("disch_date_key").alias("date_key"), "admission_type")
        .agg(F.count("*").alias("discharge_count"))
    )
    deaths = (
        fact_admission.where(F.col("death_date_key").isNotNull())
        .groupBy(F.col("death_date_key").alias("date_key"), "admission_type")
        .agg(F.count("*").alias("death_count"))
    )

    keys = (
        admits.select("date_key", "admission_type")
        .union(discharges.select("date_key", "admission_type"))
        .union(deaths.select("date_key", "admission_type"))
        .distinct()
    )

    df = (
        keys.join(admits, on=["date_key", "admission_type"], how="left")
        .join(discharges, on=["date_key", "admission_type"], how="left")
        .join(deaths, on=["date_key", "admission_type"], how="left")
        .select(
            "date_key",
            "admission_type",
            F.coalesce(F.col("admit_count"), F.lit(0)).alias("admit_count"),
            F.coalesce(F.col("discharge_count"), F.lit(0)).alias("discharge_count"),
            F.coalesce(F.col("death_count"), F.lit(0)).alias("death_count"),
        )
    )
    df = audit_cols(df, JOB_NAME)
    return merge_into_target(
        df,
        args,
        schema=WAREHOUSE_SCHEMA,
        target_table="agg_admission_daily",
        conflict_cols=["date_key", "admission_type"],
        update_cols=["admit_count", "discharge_count", "death_count"],
        job_name=JOB_NAME,
    )


def refresh_agg_icu_fluid_balance_daily(spark, args) -> int:
    fact_input_event = read_table(spark, args, WAREHOUSE_SCHEMA, "fact_input_event")
    fact_output_event = read_table(spark, args, WAREHOUSE_SCHEMA, "fact_output_event")

    # amount_uom filter: fact_input_event mixes volumes (ml) with medication/electrolyte
    # doses (mg, mcg, units, mEq, mmol, grams) in the same column -- only ml is a fluid
    # volume. fact_ingredient_event is deliberately excluded (see etl_warehouse.py): it's
    # a child-grain decomposition of fact_input_event's own amount, so including it would
    # double-count.
    intake = (
        fact_input_event.where(
            F.col("start_date_key").isNotNull() & F.col("stay_id").isNotNull() & (F.upper(F.col("amount_uom")) == "ML")
        )
        .groupBy(F.col("start_date_key").alias("date_key"), "stay_id")
        .agg(F.sum("amount").alias("total_intake_ml"))
    )
    output = (
        fact_output_event.where(F.col("chart_date_key").isNotNull() & F.col("stay_id").isNotNull())
        .groupBy(F.col("chart_date_key").alias("date_key"), "stay_id")
        .agg(F.sum("value").alias("total_output_ml"))
    )

    keys = intake.select("date_key", "stay_id").union(output.select("date_key", "stay_id")).distinct()

    df = (
        keys.join(intake, on=["date_key", "stay_id"], how="left")
        .join(output, on=["date_key", "stay_id"], how="left")
        .select(
            "date_key",
            "stay_id",
            F.coalesce(F.col("total_intake_ml"), F.lit(0.0)).alias("total_intake_ml"),
            F.coalesce(F.col("total_output_ml"), F.lit(0.0)).alias("total_output_ml"),
        )
        .withColumn("net_balance_ml", F.col("total_intake_ml") - F.col("total_output_ml"))
    )
    df = audit_cols(df, JOB_NAME)
    return merge_into_target(
        df,
        args,
        schema=WAREHOUSE_SCHEMA,
        target_table="agg_icu_fluid_balance_daily",
        conflict_cols=["date_key", "stay_id"],
        update_cols=["total_intake_ml", "total_output_ml", "net_balance_ml"],
        job_name=JOB_NAME,
    )


def refresh_agg_admission_monthly(spark, args) -> int:
    agg_admission_daily = read_table(spark, args, WAREHOUSE_SCHEMA, "agg_admission_daily")

    df = agg_admission_daily.groupBy((F.col("date_key") / 100).cast("int").alias("year_month"), "admission_type").agg(
        F.sum("admit_count").alias("admit_count"),
        F.sum("discharge_count").alias("discharge_count"),
        F.sum("death_count").alias("death_count"),
    )
    df = audit_cols(df, JOB_NAME)
    return merge_into_target(
        df,
        args,
        schema=WAREHOUSE_SCHEMA,
        target_table="agg_admission_monthly",
        conflict_cols=["year_month", "admission_type"],
        update_cols=["admit_count", "discharge_count", "death_count"],
        job_name=JOB_NAME,
    )


# agg_admission_monthly must run after agg_admission_daily -- it rolls up from it.
AGGREGATE_LOAD_ORDER: list[str] = ["agg_admission_daily", "agg_icu_fluid_balance_daily", "agg_admission_monthly"]

AGGREGATE_REFRESHERS = {
    "agg_admission_daily": refresh_agg_admission_daily,
    "agg_icu_fluid_balance_daily": refresh_agg_icu_fluid_balance_daily,
    "agg_admission_monthly": refresh_agg_admission_monthly,
}


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

    tables = [table_filter] if table_filter else AGGREGATE_LOAD_ORDER
    for table_name in tables:
        rows = log_timed_call(
            args,
            full_run_id=full_run_id,
            phase="D:aggregates",
            process_type="agg",
            process_name=table_name,
            # No source_table: each rollup reads from fact tables (agg_admission_monthly
            # reads agg_admission_daily instead), not a single staging table.
            fn=lambda t=table_name: AGGREGATE_REFRESHERS[t](spark, args),
            target_table=(WAREHOUSE_SCHEMA, table_name),
        )
        print(f"Refreshed {table_name}: {rows} rows")

    job.commit()


if __name__ == "__main__":
    main()
