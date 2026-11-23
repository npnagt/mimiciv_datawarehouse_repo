"""Glue job 3: `staging.stg_*` (+ already-loaded facts/dims) -> `warehouse.fact_*`.

PySpark equivalent of mimic_datawarehouse.etl_warehouse's FACT_LOAD_SQL / load_all_facts.
One function per fact table, each reproducing that table's CTE from etl_warehouse.py:

  - `LAG(...) OVER (PARTITION BY ... ORDER BY ...)`      -> pyspark.sql.Window + F.lag
  - `EXTRACT(EPOCH FROM (b - a)) / 60.0` / `/ 3600.0`     -> _minutes_between / _hours_between
  - `to_char(x,'YYYYMMDD')::integer` clamped to the seed  -> _date_key (SEED_START_DATE/
    range                                                    SEED_END_DATE duplicated from
                                                               seed_date_dim.py -- keep in sync)
  - LEFT JOINs for FK resolution                          -> DataFrame.join(..., "left")

Columns with no explicit `::type` cast in the source SQL are passed through unchanged and
rely on Postgres's own assignment casts during the merge INSERT ... SELECT, exactly as
the existing SQL already does -- e.g. fact_procedure_event.original_amount is BIGINT but
sourced from a DOUBLE PRECISION staging column with no explicit cast in etl_warehouse.py.

Every fact ends with `merge_into_target` (tables with a real primary key, ON CONFLICT DO
NOTHING) or `insert_only_with_anti_join` (surrogate-key tables that dedup via a NOT
EXISTS anti-join instead), from glue_common.py -- see that module's docstring for why.

Job arguments:
  --RDS_HOST / --RDS_PORT / --RDS_DATABASE / --RDS_USER / --RDS_PASSWORD
  --TABLE   a single fact table name, or the sentinel "none" for all of them (see
             glue_common.NO_VALUE -- this can't be a truly optional argument)
  --FULL_RUN_ID / --DATASET   logged per table to warehouse.etl_process_log, see
             glue_common.log_timed_call and cli.py's _next_full_run_id.
"""

from __future__ import annotations

import sys

from glue_common import (
    ensure_landing_schema,
    init_glue_job,
    insert_only_with_anti_join,
    log_timed_call,
    merge_into_target,
    optional_arg,
    read_table,
)
from pyspark.sql import functions as F
from pyspark.sql.window import Window

WAREHOUSE_SCHEMA = "warehouse"
STAGING_SCHEMA = "staging"
JOB_NAME = "glue_load_warehouse_facts"

# Duplicated from seed_date_dim.SEED_START_DATE / SEED_END_DATE -- see that module's
# docstring for why the range is 2100-2230 (MIMIC-IV's per-patient date-shifting; the
# full dataset reaches ~2215, so 2210 would drop ~275 admissions on a dim_date join).
SEED_START_DATE = "2100-01-01"
SEED_END_DATE = "2230-12-31"


def date_key(col):
    """yyyymmdd integer for `col`, NULL if `col` is NULL or outside the seeded dim_date
    range -- mirrors etl_warehouse._date_key."""
    return F.when(
        col.isNull() | (F.to_date(col) < F.lit(SEED_START_DATE)) | (F.to_date(col) > F.lit(SEED_END_DATE)),
        F.lit(None).cast("int"),
    ).otherwise(F.date_format(col, "yyyyMMdd").cast("int"))


def minutes_between(start_col, end_col):
    return (F.unix_timestamp(end_col) - F.unix_timestamp(start_col)) / 60.0


def hours_between(start_col, end_col):
    return (F.unix_timestamp(end_col) - F.unix_timestamp(start_col)) / 3600.0


def audit_cols(df, job_name: str):
    return (
        df.withColumn("created_ts", F.current_timestamp())
        .withColumn("updated_ts", F.current_timestamp())
        .withColumn("created_by", F.lit(job_name))
        .withColumn("updated_by", F.lit(job_name))
    )


# --------------------------------------------------------------------------- HOSP FACTS


def load_fact_admission(spark, args) -> int:
    stg = read_table(spark, args, STAGING_SCHEMA, "stg_admissions")
    dim_patient = read_table(spark, args, WAREHOUSE_SCHEMA, "dim_patient").select(
        "subject_id", "anchor_age", "anchor_year"
    )

    df = (
        stg.withColumn("admit_time", F.to_timestamp("admittime"))
        .withColumn("disch_time", F.to_timestamp("dischtime"))
        .withColumn("death_time", F.to_timestamp("deathtime"))
        .withColumn("ed_reg_time", F.to_timestamp("edregtime"))
        .withColumn("ed_out_time", F.to_timestamp("edouttime"))
    )
    df = df.join(dim_patient, on="subject_id", how="left")

    w = Window.partitionBy("subject_id").orderBy("admit_time")
    df = df.withColumn("prior_admit_time", F.lag("admit_time").over(w))
    df = df.withColumn("prior_disch_time", F.lag("disch_time").over(w))

    df = df.select(
        "subject_id",
        "hadm_id",
        date_key(F.col("admit_time")).alias("admit_date_key"),
        date_key(F.col("disch_time")).alias("disch_date_key"),
        date_key(F.col("death_time")).alias("death_date_key"),
        date_key(F.col("ed_reg_time")).alias("ed_reg_date_key"),
        date_key(F.col("ed_out_time")).alias("ed_out_date_key"),
        "admit_time",
        "disch_time",
        "death_time",
        "admission_type",
        "admit_provider_id",
        "admission_location",
        "discharge_location",
        "insurance",
        "language",
        "marital_status",
        "race",
        "ed_reg_time",
        "ed_out_time",
        "hospital_expire_flag",
        hours_between(F.col("admit_time"), F.col("disch_time")).alias("hospital_los_hours"),
        minutes_between(F.col("ed_reg_time"), F.col("ed_out_time")).alias("ed_los_minutes"),
        hours_between(F.col("admit_time"), F.col("death_time")).alias("time_to_death_hours"),
        (F.col("anchor_age") + (F.year("admit_time") - F.col("anchor_year"))).alias("age_at_admission"),
        F.col("prior_admit_time").isNotNull().alias("is_readmission_flag"),
        F.when(F.col("prior_disch_time").isNull(), F.lit(None).cast("int"))
        .otherwise(
            F.floor((F.unix_timestamp("admit_time") - F.unix_timestamp("prior_disch_time")) / 86400.0).cast("int")
        )
        .alias("days_since_prior_discharge"),
    )
    df = audit_cols(df, JOB_NAME)
    return merge_into_target(
        df,
        args,
        schema=WAREHOUSE_SCHEMA,
        target_table="fact_admission",
        conflict_cols=["hadm_id"],
        update_cols=[],
        job_name=JOB_NAME,
    )


def load_fact_diagnosis(spark, args) -> int:
    df = read_table(spark, args, STAGING_SCHEMA, "stg_diagnoses_icd").select(
        "subject_id", "hadm_id", "seq_num", "icd_code", "icd_version"
    )
    df = audit_cols(df, JOB_NAME)
    return merge_into_target(
        df,
        args,
        schema=WAREHOUSE_SCHEMA,
        target_table="fact_diagnosis",
        conflict_cols=["hadm_id", "seq_num"],
        update_cols=[],
        job_name=JOB_NAME,
    )


def load_fact_drg_assignment(spark, args) -> int:
    df = read_table(spark, args, STAGING_SCHEMA, "stg_drgcodes").select(
        "subject_id", "hadm_id", "drg_type", "drg_code", "description", "drg_severity", "drg_mortality"
    )
    df = audit_cols(df, JOB_NAME)
    return merge_into_target(
        df,
        args,
        schema=WAREHOUSE_SCHEMA,
        target_table="fact_drg_assignment",
        conflict_cols=["hadm_id", "drg_type", "drg_code"],
        update_cols=[],
        job_name=JOB_NAME,
    )


def load_fact_provider_order(spark, args) -> int:
    stg = read_table(spark, args, STAGING_SCHEMA, "stg_poe")
    df = stg.withColumn("order_time", F.to_timestamp("ordertime"))
    df = df.select(
        "poe_id",
        "poe_seq",
        "subject_id",
        "hadm_id",
        date_key(F.col("order_time")).alias("order_date_key"),
        "order_time",
        "order_type",
        "order_subtype",
        "transaction_type",
        "discontinue_of_poe_id",
        "discontinued_by_poe_id",
        "order_provider_id",
        "order_status",
    )
    df = audit_cols(df, JOB_NAME)
    return merge_into_target(
        df,
        args,
        schema=WAREHOUSE_SCHEMA,
        target_table="fact_provider_order",
        conflict_cols=["poe_id"],
        update_cols=[],
        job_name=JOB_NAME,
    )


def load_fact_pharmacy_order(spark, args) -> int:
    stg = read_table(spark, args, STAGING_SCHEMA, "stg_pharmacy")
    fpo = read_table(spark, args, WAREHOUSE_SCHEMA, "fact_provider_order").select(F.col("poe_id").alias("_fpo_poe_id"))

    df = (
        stg.withColumn("start_time", F.to_timestamp("starttime"))
        .withColumn("stop_time", F.to_timestamp("stoptime"))
        .withColumn("enter_time", F.to_timestamp("entertime"))
        .withColumn("verified_time", F.to_timestamp("verifiedtime"))
    )
    # LEFT JOIN, not a plain column reference: ~0.1% of pharmacy rows reference a poe_id
    # that doesn't exist in poe.csv (a real MIMIC-IV data-completeness gap, verified
    # against the raw data, not an ETL bug) -- same reasoning as etl_warehouse.py.
    df = df.join(fpo, df["poe_id"] == fpo["_fpo_poe_id"], "left")

    df = df.select(
        "subject_id",
        "hadm_id",
        "pharmacy_id",
        F.col("_fpo_poe_id").alias("poe_id"),
        date_key(F.col("start_time")).alias("start_date_key"),
        date_key(F.col("stop_time")).alias("stop_date_key"),
        date_key(F.col("enter_time")).alias("enter_date_key"),
        date_key(F.col("verified_time")).alias("verified_date_key"),
        "start_time",
        "stop_time",
        "medication",
        "proc_type",
        "status",
        "enter_time",
        "verified_time",
        "route",
        "frequency",
        "disp_sched",
        "infusion_type",
        "sliding_scale",
        F.col("lockout_interval").cast("double").alias("lockout_interval"),
        F.col("basal_rate").cast("double").alias("basal_rate"),
        F.col("one_hr_max").cast("double").alias("one_hr_max"),
        "doses_per_24_hrs",
        "duration",
        "duration_interval",
        "expiration_value",
        "expiration_unit",
        F.col("expirationdate").cast("date").alias("expiration_date"),
        "dispensation",
        F.col("fill_quantity").cast("double").alias("fill_quantity"),
    )
    df = audit_cols(df, JOB_NAME)
    return merge_into_target(
        df,
        args,
        schema=WAREHOUSE_SCHEMA,
        target_table="fact_pharmacy_order",
        conflict_cols=["pharmacy_id"],
        update_cols=[],
        job_name=JOB_NAME,
    )


def load_fact_medication_administration(spark, args) -> int:
    stg = read_table(spark, args, STAGING_SCHEMA, "stg_emar")
    fpo = read_table(spark, args, WAREHOUSE_SCHEMA, "fact_provider_order").select(F.col("poe_id").alias("_fpo_poe_id"))
    fpho = read_table(spark, args, WAREHOUSE_SCHEMA, "fact_pharmacy_order").select(
        F.col("pharmacy_id").alias("_fpho_pharmacy_id")
    )

    df = (
        stg.withColumn("chart_time", F.to_timestamp("charttime"))
        .withColumn("schedule_time", F.to_timestamp("scheduletime"))
        .withColumn("store_time", F.to_timestamp("storetime"))
    )
    # LEFT JOINs, not plain column references: ~3% of emar rows reference a poe_id and
    # ~2% a pharmacy_id that doesn't exist in poe.csv/pharmacy.csv (a real MIMIC-IV
    # data-completeness gap, verified against the raw data, not an ETL bug).
    df = df.join(fpo, df["poe_id"] == fpo["_fpo_poe_id"], "left")
    df = df.join(fpho, df["pharmacy_id"] == fpho["_fpho_pharmacy_id"], "left")

    df = df.select(
        "subject_id",
        "hadm_id",
        "emar_id",
        "emar_seq",
        F.col("_fpo_poe_id").alias("poe_id"),
        F.col("_fpho_pharmacy_id").alias("pharmacy_id"),
        "enter_provider_id",
        date_key(F.col("chart_time")).alias("chart_date_key"),
        date_key(F.col("schedule_time")).alias("schedule_date_key"),
        date_key(F.col("store_time")).alias("store_date_key"),
        "chart_time",
        "medication",
        "event_txt",
        "schedule_time",
        "store_time",
        minutes_between(F.col("schedule_time"), F.col("chart_time")).alias("admin_delay_minutes"),
    )
    df = audit_cols(df, JOB_NAME)
    return merge_into_target(
        df,
        args,
        schema=WAREHOUSE_SCHEMA,
        target_table="fact_medication_administration",
        conflict_cols=["emar_id"],
        update_cols=[],
        job_name=JOB_NAME,
    )


def load_fact_medication_administration_mini_detail(spark, args) -> int:
    stg = read_table(spark, args, STAGING_SCHEMA, "stg_emar_detail")
    fpho = read_table(spark, args, WAREHOUSE_SCHEMA, "fact_pharmacy_order").select(
        F.col("pharmacy_id").alias("_fpho_pharmacy_id")
    )
    # LEFT JOIN, not a plain column reference: ~2% of emar_detail rows reference a
    # pharmacy_id that doesn't exist in pharmacy.csv (real data-completeness gap).
    df = stg.join(fpho, stg["pharmacy_id"] == fpho["_fpho_pharmacy_id"], "left")

    df = df.select(
        "subject_id",
        "emar_id",
        "emar_seq",
        "parent_field_ordinal",
        "administration_type",
        F.col("_fpho_pharmacy_id").alias("pharmacy_id"),
        "barcode_type",
        "reason_for_no_barcode",
        "complete_dose_not_given",
        "dose_due",
        "dose_due_unit",
        "dose_given",
        "dose_given_unit",
        "will_remainder_of_dose_be_given",
        "product_amount_given",
        "product_unit",
        "product_code",
        "product_description",
        "product_description_other",
        "prior_infusion_rate",
        "infusion_rate",
        "infusion_rate_adjustment",
        F.col("infusion_rate_adjustment_amount").cast("double").alias("infusion_rate_adjustment_amount"),
        "infusion_rate_unit",
        "route",
        "infusion_complete",
        "completion_interval",
        "new_iv_bag_hung",
        "continued_infusion_in_other_location",
        "restart_interval",
        "side",
        "site",
        "non_formulary_visual_verification",
    )
    df = audit_cols(df, JOB_NAME)
    # ON CONFLICT against warehouse.py's ux_med_admin_mini_detail_grain expression index
    # (NULL-safe: COALESCE'd emar_id/emar_seq/parent_field_ordinal) -- was
    # insert_only_with_anti_join, which only guards against rows already committed to
    # the target, not duplicates within the same landing table batch. Real
    # emar_detail.csv.gz data has exact-duplicate rows (verified 64 pairs at
    # full-dataset scale) that collided with each other under the anti-join and aborted
    # the whole load with a UniqueViolation.
    return merge_into_target(
        df,
        args,
        schema=WAREHOUSE_SCHEMA,
        target_table="fact_medication_administration_mini_detail",
        conflict_exprs=[
            '(COALESCE("emar_id", \'\'))', '(COALESCE("emar_seq", -1))',
            '(COALESCE("parent_field_ordinal", -1))',
        ],
        update_cols=[],
        job_name=JOB_NAME,
    )


def load_fact_hcpcs_event(spark, args) -> int:
    stg = read_table(spark, args, STAGING_SCHEMA, "stg_hcpcsevents")
    df = stg.withColumn("chart_date_d", F.to_date("chartdate"))
    df = df.select(
        "subject_id",
        "hadm_id",
        date_key(F.col("chart_date_d")).alias("chart_date_key"),
        F.col("chart_date_d").alias("chart_date"),
        "hcpcs_cd",
        "seq_num",
        "short_description",
    )
    df = audit_cols(df, JOB_NAME)
    return merge_into_target(
        df,
        args,
        schema=WAREHOUSE_SCHEMA,
        target_table="fact_hcpcs_event",
        conflict_cols=["hadm_id", "seq_num"],
        update_cols=[],
        job_name=JOB_NAME,
    )


def load_fact_lab_result(spark, args) -> int:
    stg = read_table(spark, args, STAGING_SCHEMA, "stg_labevents")
    df = stg.withColumn("chart_time", F.to_timestamp("charttime")).withColumn("store_time", F.to_timestamp("storetime"))
    df = df.select(
        "labevent_id",
        "subject_id",
        "hadm_id",
        "specimen_id",
        F.col("itemid").alias("item_id"),
        "order_provider_id",
        date_key(F.col("chart_time")).alias("chart_date_key"),
        date_key(F.col("store_time")).alias("store_date_key"),
        "chart_time",
        "store_time",
        "value",
        F.col("valuenum").alias("value_num"),
        F.col("valueuom").alias("value_uom"),
        "ref_range_lower",
        "ref_range_upper",
        "flag",
        "priority",
        "comments",
        minutes_between(F.col("chart_time"), F.col("store_time")).alias("result_turnaround_minutes"),
        F.when(
            F.col("valuenum").isNull() | F.col("ref_range_lower").isNull() | F.col("ref_range_upper").isNull(),
            F.lit(None).cast("boolean"),
        )
        .otherwise((F.col("valuenum") < F.col("ref_range_lower")) | (F.col("valuenum") > F.col("ref_range_upper")))
        .alias("is_abnormal_flag"),
    )
    df = audit_cols(df, JOB_NAME)
    return merge_into_target(
        df,
        args,
        schema=WAREHOUSE_SCHEMA,
        target_table="fact_lab_result",
        conflict_cols=["labevent_id"],
        update_cols=[],
        job_name=JOB_NAME,
    )


def load_fact_microbiology_result(spark, args) -> int:
    stg = read_table(spark, args, STAGING_SCHEMA, "stg_microbiologyevents")
    df = (
        stg.withColumn("chart_date_ts", F.to_timestamp("chartdate"))
        .withColumn("chart_time_ts", F.to_timestamp("charttime"))
        .withColumn("store_date_ts", F.to_timestamp("storedate"))
        .withColumn("store_time_ts", F.to_timestamp("storetime"))
    )
    df = df.select(
        "microevent_id",
        "subject_id",
        "hadm_id",
        "micro_specimen_id",
        "order_provider_id",
        date_key(F.col("chart_time_ts")).alias("chart_date_key"),
        date_key(F.col("store_time_ts")).alias("store_date_key"),
        F.col("chart_date_ts").alias("chart_date"),
        F.col("chart_time_ts").alias("chart_time"),
        "spec_itemid",
        "spec_type_desc",
        "test_seq",
        F.col("store_date_ts").alias("store_date"),
        F.col("store_time_ts").alias("store_time"),
        "test_itemid",
        "test_name",
        "org_itemid",
        "org_name",
        "isolate_num",
        F.col("quantity").cast("double").alias("quantity"),
        "ab_itemid",
        "ab_name",
        "dilution_text",
        "dilution_comparison",
        "dilution_value",
        "interpretation",
        "comments",
        minutes_between(F.col("chart_time_ts"), F.col("store_time_ts")).alias("result_turnaround_minutes"),
        ((F.col("org_name").isNotNull()) & (F.col("org_name") != F.lit(""))).alias("is_positive_culture_flag"),
    )
    df = audit_cols(df, JOB_NAME)
    return merge_into_target(
        df,
        args,
        schema=WAREHOUSE_SCHEMA,
        target_table="fact_microbiology_result",
        conflict_cols=["microevent_id"],
        update_cols=[],
        job_name=JOB_NAME,
    )


def load_fact_outpatient_measurement(spark, args) -> int:
    stg = read_table(spark, args, STAGING_SCHEMA, "stg_omr")
    df = stg.withColumn("chart_date_d", F.to_date("chartdate"))
    df = df.select(
        "subject_id",
        date_key(F.col("chart_date_d")).alias("chart_date_key"),
        F.col("chart_date_d").alias("chart_date"),
        "seq_num",
        "result_name",
        "result_value",
    )
    df = audit_cols(df, JOB_NAME)
    return merge_into_target(
        df,
        args,
        schema=WAREHOUSE_SCHEMA,
        target_table="fact_outpatient_measurement",
        conflict_cols=["subject_id", "chart_date", "seq_num", "result_name"],
        update_cols=[],
        job_name=JOB_NAME,
    )


def load_fact_provider_order_mini_detail(spark, args) -> int:
    stg = read_table(spark, args, STAGING_SCHEMA, "stg_poe_detail").select(
        "poe_id", "poe_seq", "subject_id", "field_name", "field_value"
    )
    fpo = read_table(spark, args, WAREHOUSE_SCHEMA, "fact_provider_order").select(
        F.col("poe_id").alias("_fpo_poe_id")
    )
    # INNER JOIN, not LEFT: poe_id is NOT NULL + FK on this table, so a poe_id absent
    # from fact_provider_order can't be nulled out the way pharmacy_id/poe_id are
    # elsewhere (those columns are nullable) -- it has to be dropped instead. At the
    # current --row-cap this isn't a small edge case: stg_poe and stg_poe_detail are
    # capped independently (1,000,000 rows each by raw file order), so their poe_id
    # sets only partially overlap -- verified 840,769 of 1,000,000 (84%) stg_poe_detail
    # rows unresolved against fact_provider_order, far above the ~2-3% real MIMIC-IV
    # completeness gaps documented for the nullable-FK tables.
    df = stg.join(fpo, stg["poe_id"] == fpo["_fpo_poe_id"], "inner").drop("_fpo_poe_id")
    df = audit_cols(df, JOB_NAME)
    return merge_into_target(
        df,
        args,
        schema=WAREHOUSE_SCHEMA,
        target_table="fact_provider_order_mini_detail",
        conflict_cols=["poe_id", "field_name"],
        update_cols=[],
        job_name=JOB_NAME,
    )


def load_fact_prescription(spark, args) -> int:
    stg = read_table(spark, args, STAGING_SCHEMA, "stg_prescriptions")
    fpho = read_table(spark, args, WAREHOUSE_SCHEMA, "fact_pharmacy_order").select(
        F.col("pharmacy_id").alias("_fpho_pharmacy_id")
    )
    fpo = read_table(spark, args, WAREHOUSE_SCHEMA, "fact_provider_order").select(F.col("poe_id").alias("_fpo_poe_id"))

    df = stg.withColumn("start_time", F.to_timestamp("starttime")).withColumn("stop_time", F.to_timestamp("stoptime"))
    # LEFT JOINs, not plain column references: a small fraction of prescription rows
    # reference a poe_id/pharmacy_id that doesn't exist in poe.csv/pharmacy.csv (real
    # data-completeness gap, not an ETL bug).
    df = df.join(fpho, df["pharmacy_id"] == fpho["_fpho_pharmacy_id"], "left")
    df = df.join(fpo, df["poe_id"] == fpo["_fpo_poe_id"], "left")

    df = df.select(
        "subject_id",
        "hadm_id",
        F.col("_fpho_pharmacy_id").alias("pharmacy_id"),
        F.col("_fpo_poe_id").alias("poe_id"),
        "poe_seq",
        "order_provider_id",
        date_key(F.col("start_time")).alias("start_date_key"),
        date_key(F.col("stop_time")).alias("stop_date_key"),
        "start_time",
        "stop_time",
        "drug_type",
        "drug",
        "formulary_drug_cd",
        "gsn",
        "ndc",
        "prod_strength",
        "form_rx",
        "dose_val_rx",
        "dose_unit_rx",
        "form_val_disp",
        "form_unit_disp",
        "doses_per_24_hrs",
        "route",
    )
    df = audit_cols(df, JOB_NAME)
    # ON CONFLICT against warehouse.py's ux_fact_prescription_grain expression index --
    # see load_fact_medication_administration_mini_detail for why this replaces
    # insert_only_with_anti_join (verified 66 duplicate groups in prescriptions.csv.gz
    # at full-dataset scale).
    return merge_into_target(
        df,
        args,
        schema=WAREHOUSE_SCHEMA,
        target_table="fact_prescription",
        conflict_exprs=[
            '(COALESCE("subject_id", -1))', '(COALESCE("hadm_id", -1))', '(COALESCE("pharmacy_id", -1))',
            '(COALESCE("poe_id", \'\'))', '(COALESCE("poe_seq", -1))',
            "(COALESCE(\"start_time\", TIMESTAMP 'epoch'))", '(COALESCE("drug", \'\'))',
        ],
        update_cols=[],
        job_name=JOB_NAME,
    )


def load_fact_procedure(spark, args) -> int:
    stg = read_table(spark, args, STAGING_SCHEMA, "stg_procedures_icd")
    df = stg.withColumn("chart_date_d", F.to_date("chartdate"))
    df = df.select(
        "subject_id",
        "hadm_id",
        "seq_num",
        date_key(F.col("chart_date_d")).alias("chart_date_key"),
        F.col("chart_date_d").alias("chart_date"),
        "icd_code",
        "icd_version",
    )
    df = audit_cols(df, JOB_NAME)
    return merge_into_target(
        df,
        args,
        schema=WAREHOUSE_SCHEMA,
        target_table="fact_procedure",
        conflict_cols=["hadm_id", "seq_num"],
        update_cols=[],
        job_name=JOB_NAME,
    )


def load_fact_service_assignment(spark, args) -> int:
    stg = read_table(spark, args, STAGING_SCHEMA, "stg_services")
    df = stg.withColumn("transfer_time", F.to_timestamp("transfertime"))
    df = df.select(
        "subject_id",
        "hadm_id",
        date_key(F.col("transfer_time")).alias("transfer_date_key"),
        "transfer_time",
        "prev_service",
        "curr_service",
    )
    df = audit_cols(df, JOB_NAME)
    return merge_into_target(
        df,
        args,
        schema=WAREHOUSE_SCHEMA,
        target_table="fact_service_assignment",
        conflict_cols=["hadm_id", "transfer_time"],
        update_cols=[],
        job_name=JOB_NAME,
    )


def load_fact_transfer(spark, args) -> int:
    stg = read_table(spark, args, STAGING_SCHEMA, "stg_transfers")
    df = stg.withColumn("in_time", F.to_timestamp("intime")).withColumn("out_time", F.to_timestamp("outtime"))
    df = df.select(
        "subject_id",
        "hadm_id",
        "transfer_id",
        F.col("eventtype").alias("event_type"),
        F.col("careunit").alias("care_unit"),
        date_key(F.col("in_time")).alias("in_date_key"),
        date_key(F.col("out_time")).alias("out_date_key"),
        "in_time",
        "out_time",
        hours_between(F.col("in_time"), F.col("out_time")).alias("transfer_duration_hours"),
    )
    df = audit_cols(df, JOB_NAME)
    return merge_into_target(
        df,
        args,
        schema=WAREHOUSE_SCHEMA,
        target_table="fact_transfer",
        conflict_cols=["transfer_id"],
        update_cols=[],
        job_name=JOB_NAME,
    )


# ---------------------------------------------------------------------------- ICU FACTS


def load_fact_icu_stay_accumulating(spark, args) -> int:
    stg = read_table(spark, args, STAGING_SCHEMA, "stg_icustays")
    fa = read_table(spark, args, WAREHOUSE_SCHEMA, "fact_admission").select(
        F.col("hadm_id").alias("_fa_hadm_id"), F.col("admit_time").alias("_fa_admit_time")
    )

    df = stg.withColumn("in_time", F.to_timestamp("intime")).withColumn("out_time", F.to_timestamp("outtime"))
    df = df.join(fa, df["hadm_id"] == fa["_fa_hadm_id"], "left")

    w = Window.partitionBy("subject_id").orderBy("in_time")
    df = df.withColumn("prior_in_time", F.lag("in_time").over(w))

    df = df.select(
        "subject_id",
        "hadm_id",
        "stay_id",
        "first_careunit",
        "last_careunit",
        date_key(F.col("in_time")).alias("in_date_key"),
        date_key(F.col("out_time")).alias("out_date_key"),
        "in_time",
        "out_time",
        "los",
        hours_between(F.col("_fa_admit_time"), F.col("in_time")).alias("time_to_icu_hours"),
        F.col("prior_in_time").isNotNull().alias("is_icu_readmission_flag"),
    )
    df = audit_cols(df, JOB_NAME)
    return merge_into_target(
        df,
        args,
        schema=WAREHOUSE_SCHEMA,
        target_table="fact_icu_stay_accumulating",
        conflict_cols=["stay_id"],
        update_cols=[],
        job_name=JOB_NAME,
    )


def load_fact_chart_observation(spark, args) -> int:
    stg = read_table(spark, args, STAGING_SCHEMA, "stg_chartevents")
    dci = read_table(spark, args, WAREHOUSE_SCHEMA, "dim_chart_item").select(
        F.col("item_id").alias("_dci_item_id"),
        F.col("low_normal_value").alias("_dci_low"),
        F.col("high_normal_value").alias("_dci_high"),
    )

    df = stg.withColumn("chart_time", F.to_timestamp("charttime")).withColumn("store_time", F.to_timestamp("storetime"))
    df = df.join(dci, df["itemid"] == dci["_dci_item_id"], "left")

    df = df.select(
        "subject_id",
        "hadm_id",
        "stay_id",
        "caregiver_id",
        date_key(F.col("chart_time")).alias("chart_date_key"),
        date_key(F.col("store_time")).alias("store_date_key"),
        "chart_time",
        "store_time",
        F.col("itemid").alias("item_id"),
        "value",
        F.col("valuenum").alias("value_num"),
        F.col("valueuom").alias("value_uom"),
        "warning",
        minutes_between(F.col("chart_time"), F.col("store_time")).alias("charting_delay_minutes"),
        F.when(
            F.col("valuenum").isNull() | F.col("_dci_low").isNull() | F.col("_dci_high").isNull(),
            F.lit(None).cast("boolean"),
        )
        .otherwise((F.col("valuenum") < F.col("_dci_low")) | (F.col("valuenum") > F.col("_dci_high")))
        .alias("is_abnormal_flag"),
    )
    df = audit_cols(df, JOB_NAME)
    return insert_only_with_anti_join(
        df,
        args,
        schema=WAREHOUSE_SCHEMA,
        target_table="fact_chart_observation",
        dedup_cols=["stay_id", "item_id", "chart_time", "store_time"],
    )


def load_fact_datetime_event(spark, args) -> int:
    stg = read_table(spark, args, STAGING_SCHEMA, "stg_datetimeevents")
    df = stg.withColumn("chart_time", F.to_timestamp("charttime")).withColumn("store_time", F.to_timestamp("storetime"))
    df = df.select(
        "subject_id",
        "hadm_id",
        "stay_id",
        "caregiver_id",
        date_key(F.col("chart_time")).alias("chart_date_key"),
        date_key(F.col("store_time")).alias("store_date_key"),
        "chart_time",
        "store_time",
        F.col("itemid").alias("item_id"),
        "value",
        F.col("valueuom").alias("value_uom"),
        "warning",
        minutes_between(F.col("chart_time"), F.col("store_time")).alias("charting_delay_minutes"),
    )
    df = audit_cols(df, JOB_NAME)
    # ON CONFLICT against warehouse.py's ux_fact_datetime_event_grain expression index --
    # see load_fact_medication_administration_mini_detail for why this replaces
    # insert_only_with_anti_join.
    return merge_into_target(
        df,
        args,
        schema=WAREHOUSE_SCHEMA,
        target_table="fact_datetime_event",
        conflict_exprs=[
            '(COALESCE("stay_id", -1))', '(COALESCE("item_id", -1))',
            "(COALESCE(\"chart_time\", TIMESTAMP 'epoch'))", "(COALESCE(\"store_time\", TIMESTAMP 'epoch'))",
        ],
        update_cols=[],
        job_name=JOB_NAME,
    )


def load_fact_input_event(spark, args) -> int:
    stg = read_table(spark, args, STAGING_SCHEMA, "stg_inputevents")
    df = (
        stg.withColumn("start_time", F.to_timestamp("starttime"))
        .withColumn("end_time", F.to_timestamp("endtime"))
        .withColumn("store_time", F.to_timestamp("storetime"))
    )
    df = df.select(
        "subject_id",
        "hadm_id",
        "stay_id",
        "caregiver_id",
        date_key(F.col("start_time")).alias("start_date_key"),
        date_key(F.col("end_time")).alias("end_date_key"),
        date_key(F.col("store_time")).alias("store_date_key"),
        "start_time",
        "end_time",
        "store_time",
        minutes_between(F.col("start_time"), F.col("end_time")).alias("duration_minutes"),
        F.col("itemid").alias("item_id"),
        "amount",
        F.col("amountuom").alias("amount_uom"),
        "rate",
        F.col("rateuom").alias("rate_uom"),
        F.col("orderid").alias("order_id"),
        F.col("linkorderid").alias("link_order_id"),
        F.col("ordercategoryname").alias("order_category_name"),
        F.col("secondaryordercategoryname").alias("secondary_order_category_name"),
        F.col("ordercomponenttypedescription").alias("order_component_type_description"),
        F.col("ordercategorydescription").alias("order_category_description"),
        F.col("patientweight").alias("patient_weight"),
        F.col("totalamount").alias("total_amount"),
        F.col("totalamountuom").alias("total_amount_uom"),
        F.col("isopenbag").alias("is_open_bag"),
        F.col("continueinnextdept").alias("continue_in_next_dept"),
        F.col("statusdescription").alias("status_description"),
        F.col("originalamount").alias("original_amount"),
        F.col("originalrate").alias("original_rate"),
    )
    df = audit_cols(df, JOB_NAME)
    # ON CONFLICT against warehouse.py's ux_fact_input_event_grain expression index --
    # see load_fact_medication_administration_mini_detail for why this replaces
    # insert_only_with_anti_join.
    return merge_into_target(
        df,
        args,
        schema=WAREHOUSE_SCHEMA,
        target_table="fact_input_event",
        conflict_exprs=[
            '(COALESCE("stay_id", -1))', '(COALESCE("item_id", -1))',
            "(COALESCE(\"start_time\", TIMESTAMP 'epoch'))", "(COALESCE(\"end_time\", TIMESTAMP 'epoch'))",
            '(COALESCE("order_id", -1))',
        ],
        update_cols=[],
        job_name=JOB_NAME,
    )


def load_fact_ingredient_event(spark, args) -> int:
    stg = read_table(spark, args, STAGING_SCHEMA, "stg_ingredientevents")
    df = (
        stg.withColumn("start_time", F.to_timestamp("starttime"))
        .withColumn("end_time", F.to_timestamp("endtime"))
        .withColumn("store_time", F.to_timestamp("storetime"))
    )
    df = df.select(
        "subject_id",
        "hadm_id",
        "stay_id",
        "caregiver_id",
        date_key(F.col("start_time")).alias("start_date_key"),
        date_key(F.col("end_time")).alias("end_date_key"),
        date_key(F.col("store_time")).alias("store_date_key"),
        "start_time",
        "end_time",
        "store_time",
        minutes_between(F.col("start_time"), F.col("end_time")).alias("duration_minutes"),
        F.col("itemid").alias("item_id"),
        "amount",
        F.col("amountuom").alias("amount_uom"),
        "rate",
        F.col("rateuom").alias("rate_uom"),
        F.col("orderid").alias("order_id"),
        F.col("linkorderid").alias("link_order_id"),
        F.col("statusdescription").alias("status_description"),
        F.col("originalamount").alias("original_amount"),
        F.col("originalrate").alias("original_rate"),
    )
    df = audit_cols(df, JOB_NAME)
    # ON CONFLICT against warehouse.py's ux_fact_ingredient_event_grain expression index
    # -- see load_fact_medication_administration_mini_detail for why this replaces
    # insert_only_with_anti_join.
    return merge_into_target(
        df,
        args,
        schema=WAREHOUSE_SCHEMA,
        target_table="fact_ingredient_event",
        conflict_exprs=[
            '(COALESCE("stay_id", -1))', '(COALESCE("item_id", -1))',
            "(COALESCE(\"start_time\", TIMESTAMP 'epoch'))", "(COALESCE(\"end_time\", TIMESTAMP 'epoch'))",
            '(COALESCE("order_id", -1))',
        ],
        update_cols=[],
        job_name=JOB_NAME,
    )


def load_fact_output_event(spark, args) -> int:
    stg = read_table(spark, args, STAGING_SCHEMA, "stg_outputevents")
    df = stg.withColumn("chart_time", F.to_timestamp("charttime")).withColumn("store_time", F.to_timestamp("storetime"))
    df = df.select(
        "subject_id",
        "hadm_id",
        "stay_id",
        "caregiver_id",
        date_key(F.col("chart_time")).alias("chart_date_key"),
        date_key(F.col("store_time")).alias("store_date_key"),
        "chart_time",
        "store_time",
        F.col("itemid").alias("item_id"),
        "value",
        F.col("valueuom").alias("value_uom"),
        minutes_between(F.col("chart_time"), F.col("store_time")).alias("charting_delay_minutes"),
    )
    df = audit_cols(df, JOB_NAME)
    # ON CONFLICT against warehouse.py's ux_fact_output_event_grain expression index --
    # see load_fact_medication_administration_mini_detail for why this replaces
    # insert_only_with_anti_join.
    return merge_into_target(
        df,
        args,
        schema=WAREHOUSE_SCHEMA,
        target_table="fact_output_event",
        conflict_exprs=[
            '(COALESCE("stay_id", -1))', '(COALESCE("item_id", -1))',
            "(COALESCE(\"chart_time\", TIMESTAMP 'epoch'))", "(COALESCE(\"store_time\", TIMESTAMP 'epoch'))",
        ],
        update_cols=[],
        job_name=JOB_NAME,
    )


def load_fact_procedure_event(spark, args) -> int:
    stg = read_table(spark, args, STAGING_SCHEMA, "stg_procedureevents")
    df = (
        stg.withColumn("start_time", F.to_timestamp("starttime"))
        .withColumn("end_time", F.to_timestamp("endtime"))
        .withColumn("store_time", F.to_timestamp("storetime"))
    )
    df = df.select(
        "subject_id",
        "hadm_id",
        "stay_id",
        "caregiver_id",
        date_key(F.col("start_time")).alias("start_date_key"),
        date_key(F.col("end_time")).alias("end_date_key"),
        date_key(F.col("store_time")).alias("store_date_key"),
        "start_time",
        "end_time",
        "store_time",
        minutes_between(F.col("start_time"), F.col("end_time")).alias("duration_minutes"),
        F.col("itemid").alias("item_id"),
        "value",
        F.col("valueuom").alias("value_uom"),
        "location",
        F.col("locationcategory").alias("location_category"),
        F.col("orderid").alias("order_id"),
        F.col("linkorderid").alias("link_order_id"),
        F.col("ordercategoryname").alias("order_category_name"),
        F.col("ordercategorydescription").alias("order_category_description"),
        F.col("patientweight").alias("patient_weight"),
        F.col("isopenbag").alias("is_open_bag"),
        F.col("continueinnextdept").alias("continue_in_next_dept"),
        F.col("statusdescription").alias("status_description"),
        # Quoted uppercase in staging (see staging_load.py's STAGING_TABLE_COLUMNS for
        # stg_procedureevents) -- must be referenced with matching case.
        F.col("ORIGINALAMOUNT").alias("original_amount"),
        F.col("ORIGINALRATE").alias("original_rate"),
    )
    df = audit_cols(df, JOB_NAME)
    # ON CONFLICT against warehouse.py's ux_fact_procedure_event_grain expression index
    # -- see load_fact_medication_administration_mini_detail for why this replaces
    # insert_only_with_anti_join.
    return merge_into_target(
        df,
        args,
        schema=WAREHOUSE_SCHEMA,
        target_table="fact_procedure_event",
        conflict_exprs=[
            '(COALESCE("stay_id", -1))', '(COALESCE("item_id", -1))',
            "(COALESCE(\"start_time\", TIMESTAMP 'epoch'))", "(COALESCE(\"end_time\", TIMESTAMP 'epoch'))",
            '(COALESCE("order_id", -1))',
        ],
        update_cols=[],
        job_name=JOB_NAME,
    )


# FK dependency order -- same subset/order as warehouse.WAREHOUSE_TABLE_ORDER's facts.
FACT_LOAD_ORDER: list[str] = [
    "fact_admission",
    "fact_diagnosis",
    "fact_drg_assignment",
    "fact_provider_order",
    "fact_pharmacy_order",
    "fact_medication_administration",
    "fact_medication_administration_mini_detail",
    "fact_provider_order_mini_detail",
    "fact_prescription",
    "fact_hcpcs_event",
    "fact_lab_result",
    "fact_microbiology_result",
    "fact_outpatient_measurement",
    "fact_procedure",
    "fact_service_assignment",
    "fact_transfer",
    "fact_icu_stay_accumulating",
    "fact_chart_observation",
    "fact_datetime_event",
    "fact_input_event",
    "fact_ingredient_event",
    "fact_output_event",
    "fact_procedure_event",
]

FACT_LOADERS = {
    "fact_admission": load_fact_admission,
    "fact_diagnosis": load_fact_diagnosis,
    "fact_drg_assignment": load_fact_drg_assignment,
    "fact_provider_order": load_fact_provider_order,
    "fact_pharmacy_order": load_fact_pharmacy_order,
    "fact_medication_administration": load_fact_medication_administration,
    "fact_medication_administration_mini_detail": load_fact_medication_administration_mini_detail,
    "fact_provider_order_mini_detail": load_fact_provider_order_mini_detail,
    "fact_prescription": load_fact_prescription,
    "fact_hcpcs_event": load_fact_hcpcs_event,
    "fact_lab_result": load_fact_lab_result,
    "fact_microbiology_result": load_fact_microbiology_result,
    "fact_outpatient_measurement": load_fact_outpatient_measurement,
    "fact_procedure": load_fact_procedure,
    "fact_service_assignment": load_fact_service_assignment,
    "fact_transfer": load_fact_transfer,
    "fact_icu_stay_accumulating": load_fact_icu_stay_accumulating,
    "fact_chart_observation": load_fact_chart_observation,
    "fact_datetime_event": load_fact_datetime_event,
    "fact_input_event": load_fact_input_event,
    "fact_ingredient_event": load_fact_ingredient_event,
    "fact_output_event": load_fact_output_event,
    "fact_procedure_event": load_fact_procedure_event,
}

assert set(FACT_LOAD_ORDER) == set(FACT_LOADERS), "FACT_LOAD_ORDER must list every fact loader exactly once"

# The single primary/driving staging table each load_fact_* function reads via
# read_table(spark, args, STAGING_SCHEMA, ...) -- the FROM of its query, not the
# warehouse tables it LEFT JOINs afterward for FK resolution (e.g.
# fact_medication_administration_mini_detail also joins fact_pharmacy_order, but
# stg_emar_detail is what determines row count and grain). Used so log_timed_call can
# report raw_row_count for facts too, the same way it already does for staging/dims --
# mirrors the lakehouse orchestrator's _FACT_RAW_TABLE (run_lakehouse_pipeline.py).
FACT_PRIMARY_STAGING_TABLE = {
    "fact_admission": "stg_admissions",
    "fact_diagnosis": "stg_diagnoses_icd",
    "fact_drg_assignment": "stg_drgcodes",
    "fact_provider_order": "stg_poe",
    "fact_pharmacy_order": "stg_pharmacy",
    "fact_medication_administration": "stg_emar",
    "fact_medication_administration_mini_detail": "stg_emar_detail",
    "fact_provider_order_mini_detail": "stg_poe_detail",
    "fact_prescription": "stg_prescriptions",
    "fact_hcpcs_event": "stg_hcpcsevents",
    "fact_lab_result": "stg_labevents",
    "fact_microbiology_result": "stg_microbiologyevents",
    "fact_outpatient_measurement": "stg_omr",
    "fact_procedure": "stg_procedures_icd",
    "fact_service_assignment": "stg_services",
    "fact_transfer": "stg_transfers",
    "fact_icu_stay_accumulating": "stg_icustays",
    "fact_chart_observation": "stg_chartevents",
    "fact_datetime_event": "stg_datetimeevents",
    "fact_input_event": "stg_inputevents",
    "fact_ingredient_event": "stg_ingredientevents",
    "fact_output_event": "stg_outputevents",
    "fact_procedure_event": "stg_procedureevents",
}
assert set(FACT_PRIMARY_STAGING_TABLE) == set(FACT_LOADERS), (
    "FACT_PRIMARY_STAGING_TABLE must list every fact loader exactly once"
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

    tables = [table_filter] if table_filter else FACT_LOAD_ORDER
    for table_name in tables:
        rows = log_timed_call(
            args,
            full_run_id=full_run_id,
            phase="C:facts",
            process_type="fact",
            process_name=table_name,
            # The primary/driving staging table (see FACT_PRIMARY_STAGING_TABLE) --
            # several fact loaders also LEFT JOIN a warehouse table for FK resolution,
            # but that's enrichment, not the source whose row count/grain this reports.
            source_table=(STAGING_SCHEMA, FACT_PRIMARY_STAGING_TABLE[table_name]),
            fn=lambda t=table_name: FACT_LOADERS[t](spark, args),
            target_table=(WAREHOUSE_SCHEMA, table_name),
        )
        print(f"Inserted {table_name}: {rows} new rows")

    job.commit()


if __name__ == "__main__":
    main()
