"""Glue job 1: S3 raw CSV.gz -> `staging.stg_<table>` (truncate-and-reload).

PySpark equivalent of mimic_datawarehouse.etl.load_all_staging_tables. Self-contained
(no dependency on the local `mimic_datawarehouse` package) so it can be uploaded and run
as a single Glue script -- STAGING_TABLE_COLUMNS below is a deliberate copy of
src/mimic_datawarehouse/staging.py's table of the same name; keep the two in sync if the
staging schema changes.

Column types are declared explicitly (not inferred) for the same reason as
etl._text_column_dtypes: a TEXT column whose values happen to look numeric, with at
least one missing value (e.g. d_hcpcs.category), would otherwise get inferred as a
double and silently corrupted ("1" -> "1.0"). Declaring every column's type up front
sidesteps that entirely.

Job arguments:
  --S3_BUCKET     bucket holding <module>/<table>.csv.gz (uploaded by `mimic-load aws upload-data`)
  --RDS_HOST / --RDS_PORT / --RDS_DATABASE / --RDS_USER / --RDS_PASSWORD
  --MODULES       comma-separated, e.g. "hosp,icu"
  --TABLE         a single table name, or the sentinel "none" for every table in --MODULES
                   (see glue_common.NO_VALUE -- this can't be a truly optional argument)
  --ROW_CAP       max rows to load per staging table; "0" (or NO_VALUE) means unlimited.
                   Defaults to 1,000,000 -- the same rule the lakehouse repo applies to its
                   Gold fact tables: any source table with more than a million rows is
                   truncated to a million so demo and full runs stay comparable and the
                   full-dataset Aurora load doesn't balloon in cost. Tables already under
                   the cap are unaffected.
  --FULL_RUN_ID   one integer shared by every job of a single `glue run-all` invocation
                   (see cli.py's _next_full_run_id); logged per table to
                   warehouse.etl_process_log via glue_common.log_timed_call.
  --DATASET       "demodataset" or "fulldataset" -- logged alongside each row for the
                   same table above.
"""

from __future__ import annotations

import sys

from glue_common import (
    init_glue_job,
    jdbc_properties,
    jdbc_url,
    log_timed_call,
    optional_arg,
    truncate_table,
)
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

STAGING_SCHEMA = "staging"
MODULE_SCHEMAS = ("hosp", "icu")

# Default per-table row cap -- keep in sync with mimic_datawarehouse.etl.DEFAULT_ROW_CAP
# and the lakehouse repo's fact-table row limit.
DEFAULT_ROW_CAP = 1_000_000

# table_name -> [(column_name, postgres_type), ...] -- mirrors staging.STAGING_TABLE_COLUMNS.
STAGING_TABLE_COLUMNS: dict[str, list[tuple[str, str]]] = {
    "admissions": [
        ("subject_id", "BIGINT"),
        ("hadm_id", "BIGINT"),
        ("admittime", "TEXT"),
        ("dischtime", "TEXT"),
        ("deathtime", "TEXT"),
        ("admission_type", "TEXT"),
        ("admit_provider_id", "TEXT"),
        ("admission_location", "TEXT"),
        ("discharge_location", "TEXT"),
        ("insurance", "TEXT"),
        ("language", "TEXT"),
        ("marital_status", "TEXT"),
        ("race", "TEXT"),
        ("edregtime", "TEXT"),
        ("edouttime", "TEXT"),
        ("hospital_expire_flag", "BIGINT"),
    ],
    "caregiver": [("caregiver_id", "BIGINT")],
    "chartevents": [
        ("subject_id", "BIGINT"),
        ("hadm_id", "BIGINT"),
        ("stay_id", "BIGINT"),
        ("caregiver_id", "BIGINT"),
        ("charttime", "TEXT"),
        ("storetime", "TEXT"),
        ("itemid", "BIGINT"),
        ("value", "TEXT"),
        ("valuenum", "DOUBLE PRECISION"),
        ("valueuom", "TEXT"),
        ("warning", "BIGINT"),
    ],
    "d_hcpcs": [("code", "TEXT"), ("category", "TEXT"), ("long_description", "TEXT"), ("short_description", "TEXT")],
    "d_icd_diagnoses": [("icd_code", "TEXT"), ("icd_version", "BIGINT"), ("long_title", "TEXT")],
    "d_icd_procedures": [("icd_code", "TEXT"), ("icd_version", "BIGINT"), ("long_title", "TEXT")],
    "d_items": [
        ("itemid", "BIGINT"),
        ("label", "TEXT"),
        ("abbreviation", "TEXT"),
        ("linksto", "TEXT"),
        ("category", "TEXT"),
        ("unitname", "TEXT"),
        ("param_type", "TEXT"),
        ("lownormalvalue", "TEXT"),
        ("highnormalvalue", "TEXT"),
    ],
    "d_labitems": [("itemid", "BIGINT"), ("label", "TEXT"), ("fluid", "TEXT"), ("category", "TEXT")],
    "datetimeevents": [
        ("subject_id", "BIGINT"),
        ("hadm_id", "BIGINT"),
        ("stay_id", "BIGINT"),
        ("caregiver_id", "BIGINT"),
        ("charttime", "TEXT"),
        ("storetime", "TEXT"),
        ("itemid", "BIGINT"),
        ("value", "TEXT"),
        ("valueuom", "TEXT"),
        ("warning", "BIGINT"),
    ],
    "diagnoses_icd": [
        ("subject_id", "BIGINT"),
        ("hadm_id", "BIGINT"),
        ("seq_num", "BIGINT"),
        ("icd_code", "TEXT"),
        ("icd_version", "BIGINT"),
    ],
    "drgcodes": [
        ("subject_id", "BIGINT"),
        ("hadm_id", "BIGINT"),
        ("drg_type", "TEXT"),
        ("drg_code", "BIGINT"),
        ("description", "TEXT"),
        ("drg_severity", "BIGINT"),
        ("drg_mortality", "BIGINT"),
    ],
    "emar_detail": [
        ("subject_id", "BIGINT"),
        ("emar_id", "TEXT"),
        ("emar_seq", "BIGINT"),
        ("parent_field_ordinal", "DOUBLE PRECISION"),
        ("administration_type", "TEXT"),
        ("pharmacy_id", "BIGINT"),
        ("barcode_type", "TEXT"),
        ("reason_for_no_barcode", "TEXT"),
        ("complete_dose_not_given", "TEXT"),
        ("dose_due", "TEXT"),
        ("dose_due_unit", "TEXT"),
        ("dose_given", "TEXT"),
        ("dose_given_unit", "TEXT"),
        ("will_remainder_of_dose_be_given", "TEXT"),
        ("product_amount_given", "DOUBLE PRECISION"),
        ("product_unit", "TEXT"),
        ("product_code", "TEXT"),
        ("product_description", "TEXT"),
        ("product_description_other", "TEXT"),
        ("prior_infusion_rate", "DOUBLE PRECISION"),
        ("infusion_rate", "DOUBLE PRECISION"),
        ("infusion_rate_adjustment", "TEXT"),
        ("infusion_rate_adjustment_amount", "TEXT"),
        ("infusion_rate_unit", "TEXT"),
        ("route", "TEXT"),
        ("infusion_complete", "TEXT"),
        ("completion_interval", "TEXT"),
        ("new_iv_bag_hung", "TEXT"),
        ("continued_infusion_in_other_location", "TEXT"),
        ("restart_interval", "TEXT"),
        ("side", "TEXT"),
        ("site", "TEXT"),
        ("non_formulary_visual_verification", "TEXT"),
    ],
    "emar": [
        ("subject_id", "BIGINT"),
        ("hadm_id", "BIGINT"),
        ("emar_id", "TEXT"),
        ("emar_seq", "BIGINT"),
        ("poe_id", "TEXT"),
        ("pharmacy_id", "BIGINT"),
        ("enter_provider_id", "TEXT"),
        ("charttime", "TEXT"),
        ("medication", "TEXT"),
        ("event_txt", "TEXT"),
        ("scheduletime", "TEXT"),
        ("storetime", "TEXT"),
    ],
    "hcpcsevents": [
        ("subject_id", "BIGINT"),
        ("hadm_id", "BIGINT"),
        ("chartdate", "TEXT"),
        ("hcpcs_cd", "TEXT"),
        ("seq_num", "BIGINT"),
        ("short_description", "TEXT"),
    ],
    "icustays": [
        ("subject_id", "BIGINT"),
        ("hadm_id", "BIGINT"),
        ("stay_id", "BIGINT"),
        ("first_careunit", "TEXT"),
        ("last_careunit", "TEXT"),
        ("intime", "TEXT"),
        ("outtime", "TEXT"),
        ("los", "DOUBLE PRECISION"),
    ],
    "ingredientevents": [
        ("subject_id", "BIGINT"),
        ("hadm_id", "BIGINT"),
        ("stay_id", "BIGINT"),
        ("caregiver_id", "BIGINT"),
        ("starttime", "TEXT"),
        ("endtime", "TEXT"),
        ("storetime", "TEXT"),
        ("itemid", "BIGINT"),
        ("amount", "DOUBLE PRECISION"),
        ("amountuom", "TEXT"),
        ("rate", "DOUBLE PRECISION"),
        ("rateuom", "TEXT"),
        ("orderid", "BIGINT"),
        ("linkorderid", "BIGINT"),
        ("statusdescription", "TEXT"),
        ("originalamount", "BIGINT"),
        ("originalrate", "DOUBLE PRECISION"),
    ],
    "inputevents": [
        ("subject_id", "BIGINT"),
        ("hadm_id", "BIGINT"),
        ("stay_id", "BIGINT"),
        ("caregiver_id", "BIGINT"),
        ("starttime", "TEXT"),
        ("endtime", "TEXT"),
        ("storetime", "TEXT"),
        ("itemid", "BIGINT"),
        ("amount", "DOUBLE PRECISION"),
        ("amountuom", "TEXT"),
        ("rate", "DOUBLE PRECISION"),
        ("rateuom", "TEXT"),
        ("orderid", "BIGINT"),
        ("linkorderid", "BIGINT"),
        ("ordercategoryname", "TEXT"),
        ("secondaryordercategoryname", "TEXT"),
        ("ordercomponenttypedescription", "TEXT"),
        ("ordercategorydescription", "TEXT"),
        ("patientweight", "DOUBLE PRECISION"),
        ("totalamount", "DOUBLE PRECISION"),
        ("totalamountuom", "TEXT"),
        ("isopenbag", "BIGINT"),
        ("continueinnextdept", "BIGINT"),
        ("statusdescription", "TEXT"),
        ("originalamount", "DOUBLE PRECISION"),
        ("originalrate", "DOUBLE PRECISION"),
    ],
    "labevents": [
        ("labevent_id", "BIGINT"),
        ("subject_id", "BIGINT"),
        ("hadm_id", "BIGINT"),
        ("specimen_id", "BIGINT"),
        ("itemid", "BIGINT"),
        ("order_provider_id", "TEXT"),
        ("charttime", "TEXT"),
        ("storetime", "TEXT"),
        ("value", "TEXT"),
        ("valuenum", "DOUBLE PRECISION"),
        ("valueuom", "TEXT"),
        ("ref_range_lower", "DOUBLE PRECISION"),
        ("ref_range_upper", "DOUBLE PRECISION"),
        ("flag", "TEXT"),
        ("priority", "TEXT"),
        ("comments", "TEXT"),
    ],
    "microbiologyevents": [
        ("microevent_id", "BIGINT"),
        ("subject_id", "BIGINT"),
        ("hadm_id", "BIGINT"),
        ("micro_specimen_id", "BIGINT"),
        ("order_provider_id", "TEXT"),
        ("chartdate", "TEXT"),
        ("charttime", "TEXT"),
        ("spec_itemid", "BIGINT"),
        ("spec_type_desc", "TEXT"),
        ("test_seq", "BIGINT"),
        ("storedate", "TEXT"),
        ("storetime", "TEXT"),
        ("test_itemid", "BIGINT"),
        ("test_name", "TEXT"),
        ("org_itemid", "BIGINT"),
        ("org_name", "TEXT"),
        ("isolate_num", "BIGINT"),
        ("quantity", "TEXT"),
        ("ab_itemid", "BIGINT"),
        ("ab_name", "TEXT"),
        ("dilution_text", "TEXT"),
        ("dilution_comparison", "TEXT"),
        ("dilution_value", "DOUBLE PRECISION"),
        ("interpretation", "TEXT"),
        ("comments", "TEXT"),
    ],
    "omr": [
        ("subject_id", "BIGINT"),
        ("chartdate", "TEXT"),
        ("seq_num", "BIGINT"),
        ("result_name", "TEXT"),
        ("result_value", "TEXT"),
    ],
    "outputevents": [
        ("subject_id", "BIGINT"),
        ("hadm_id", "BIGINT"),
        ("stay_id", "BIGINT"),
        ("caregiver_id", "BIGINT"),
        ("charttime", "TEXT"),
        ("storetime", "TEXT"),
        ("itemid", "BIGINT"),
        ("value", "BIGINT"),
        ("valueuom", "TEXT"),
    ],
    "patients": [
        ("subject_id", "BIGINT"),
        ("gender", "TEXT"),
        ("anchor_age", "BIGINT"),
        ("anchor_year", "BIGINT"),
        ("anchor_year_group", "TEXT"),
        ("dod", "TEXT"),
    ],
    "pharmacy": [
        ("subject_id", "BIGINT"),
        ("hadm_id", "BIGINT"),
        ("pharmacy_id", "BIGINT"),
        ("poe_id", "TEXT"),
        ("starttime", "TEXT"),
        ("stoptime", "TEXT"),
        ("medication", "TEXT"),
        ("proc_type", "TEXT"),
        ("status", "TEXT"),
        ("entertime", "TEXT"),
        ("verifiedtime", "TEXT"),
        ("route", "TEXT"),
        ("frequency", "TEXT"),
        ("disp_sched", "TEXT"),
        ("infusion_type", "TEXT"),
        ("sliding_scale", "TEXT"),
        ("lockout_interval", "TEXT"),
        ("basal_rate", "TEXT"),
        ("one_hr_max", "TEXT"),
        ("doses_per_24_hrs", "BIGINT"),
        ("duration", "BIGINT"),
        ("duration_interval", "TEXT"),
        ("expiration_value", "BIGINT"),
        ("expiration_unit", "TEXT"),
        ("expirationdate", "TEXT"),
        ("dispensation", "TEXT"),
        ("fill_quantity", "TEXT"),
    ],
    "poe_detail": [
        ("poe_id", "TEXT"),
        ("poe_seq", "BIGINT"),
        ("subject_id", "BIGINT"),
        ("field_name", "TEXT"),
        ("field_value", "TEXT"),
    ],
    "poe": [
        ("poe_id", "TEXT"),
        ("poe_seq", "BIGINT"),
        ("subject_id", "BIGINT"),
        ("hadm_id", "BIGINT"),
        ("ordertime", "TEXT"),
        ("order_type", "TEXT"),
        ("order_subtype", "TEXT"),
        ("transaction_type", "TEXT"),
        ("discontinue_of_poe_id", "TEXT"),
        ("discontinued_by_poe_id", "TEXT"),
        ("order_provider_id", "TEXT"),
        ("order_status", "TEXT"),
    ],
    "prescriptions": [
        ("subject_id", "BIGINT"),
        ("hadm_id", "BIGINT"),
        ("pharmacy_id", "BIGINT"),
        ("poe_id", "TEXT"),
        ("poe_seq", "BIGINT"),
        ("order_provider_id", "TEXT"),
        ("starttime", "TEXT"),
        ("stoptime", "TEXT"),
        ("drug_type", "TEXT"),
        ("drug", "TEXT"),
        ("formulary_drug_cd", "TEXT"),
        ("gsn", "TEXT"),
        ("ndc", "BIGINT"),
        ("prod_strength", "TEXT"),
        ("form_rx", "TEXT"),
        ("dose_val_rx", "TEXT"),
        ("dose_unit_rx", "TEXT"),
        ("form_val_disp", "TEXT"),
        ("form_unit_disp", "TEXT"),
        ("doses_per_24_hrs", "BIGINT"),
        ("route", "TEXT"),
    ],
    "procedureevents": [
        ("subject_id", "BIGINT"),
        ("hadm_id", "BIGINT"),
        ("stay_id", "BIGINT"),
        ("caregiver_id", "BIGINT"),
        ("starttime", "TEXT"),
        ("endtime", "TEXT"),
        ("storetime", "TEXT"),
        ("itemid", "BIGINT"),
        ("value", "DOUBLE PRECISION"),
        ("valueuom", "TEXT"),
        ("location", "TEXT"),
        ("locationcategory", "TEXT"),
        ("orderid", "BIGINT"),
        ("linkorderid", "BIGINT"),
        ("ordercategoryname", "TEXT"),
        ("ordercategorydescription", "TEXT"),
        ("patientweight", "DOUBLE PRECISION"),
        ("isopenbag", "BIGINT"),
        ("continueinnextdept", "BIGINT"),
        ("statusdescription", "TEXT"),
        ("ORIGINALAMOUNT", "DOUBLE PRECISION"),
        ("ORIGINALRATE", "BIGINT"),
    ],
    "procedures_icd": [
        ("subject_id", "BIGINT"),
        ("hadm_id", "BIGINT"),
        ("seq_num", "BIGINT"),
        ("chartdate", "TEXT"),
        ("icd_code", "TEXT"),
        ("icd_version", "BIGINT"),
    ],
    "provider": [("provider_id", "TEXT")],
    "services": [
        ("subject_id", "BIGINT"),
        ("hadm_id", "BIGINT"),
        ("transfertime", "TEXT"),
        ("prev_service", "TEXT"),
        ("curr_service", "TEXT"),
    ],
    "transfers": [
        ("subject_id", "BIGINT"),
        ("hadm_id", "BIGINT"),
        ("transfer_id", "BIGINT"),
        ("eventtype", "TEXT"),
        ("careunit", "TEXT"),
        ("intime", "TEXT"),
        ("outtime", "TEXT"),
    ],
}

# Tables that live under icu/ rather than hosp/ (everything else is hosp/).
ICU_TABLES = {
    "caregiver",
    "chartevents",
    "d_items",
    "datetimeevents",
    "icustays",
    "ingredientevents",
    "inputevents",
    "outputevents",
    "procedureevents",
}

_PG_TO_SPARK = {"BIGINT": LongType(), "TEXT": StringType(), "DOUBLE PRECISION": DoubleType()}


def _spark_schema(columns: list[tuple[str, str]]) -> StructType:
    return StructType([StructField(name, _PG_TO_SPARK[pg_type], True) for name, pg_type in columns])


def module_of(table_name: str) -> str:
    return "icu" if table_name in ICU_TABLES else "hosp"


def _row_cap(args: dict) -> int:
    """Per-table row cap from --ROW_CAP; 0 (or the NO_VALUE sentinel) means unlimited."""
    raw = optional_arg(args, "ROW_CAP")
    if raw is None:
        return DEFAULT_ROW_CAP
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return DEFAULT_ROW_CAP
    return max(value, 0)


def load_staging_table(spark, args: dict, table_name: str) -> int:
    module = module_of(table_name)
    columns = STAGING_TABLE_COLUMNS[table_name]
    s3_path = f"s3://{args['S3_BUCKET']}/{module}/{table_name}.csv.gz"

    df = spark.read.option("header", True).option("compression", "gzip").schema(_spark_schema(columns)).csv(s3_path)

    cap = _row_cap(args)
    if cap:
        # .limit() is pushed down, so an oversized gzip CSV is only read up to `cap` rows
        # (each .csv.gz is a single non-splittable partition read from the top). Tables
        # already under the cap are returned whole.
        df = df.limit(cap)

    truncate_table(args, STAGING_SCHEMA, f"stg_{table_name}")
    df.write.jdbc(
        url=jdbc_url(args),
        table=f'{STAGING_SCHEMA}."stg_{table_name}"',
        mode="append",
        properties=jdbc_properties(args),
    )
    return df.count()


def main() -> None:
    _glue_context, spark, job, args = init_glue_job(
        sys.argv,
        [
            "S3_BUCKET",
            "RDS_HOST",
            "RDS_PORT",
            "RDS_DATABASE",
            "RDS_USER",
            "RDS_PASSWORD",
            "RDS_AUTH_MODE",
            "AWS_REGION",
            "MODULES",
            "TABLE",
            "ROW_CAP",
            "FULL_RUN_ID",
            "DATASET",
        ],
    )
    modules = tuple(m.strip() for m in args["MODULES"].split(",") if m.strip())
    table_filter = optional_arg(args, "TABLE")
    full_run_id = int(args["FULL_RUN_ID"])

    tables = [t for t in STAGING_TABLE_COLUMNS if module_of(t) in modules]
    if table_filter:
        tables = [t for t in tables if t == table_filter]

    cap = _row_cap(args)
    print(f"Row cap per staging table: {cap or 'unlimited'}")

    for table_name in tables:
        rows = log_timed_call(
            args,
            full_run_id=full_run_id,
            phase="A:staging",
            process_type="staging",
            process_name=table_name,
            fn=lambda t=table_name: load_staging_table(spark, args, t),
            target_table=(STAGING_SCHEMA, f"stg_{table_name}"),
        )
        capped = " (capped)" if cap and rows == cap else ""
        print(f"Loaded {STAGING_SCHEMA}.stg_{table_name}: {rows} rows{capped}")

    job.commit()


if __name__ == "__main__":
    main()
