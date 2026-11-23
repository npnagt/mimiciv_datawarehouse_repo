"""Business-layer (Gold/dimensional-model) tables: dimensions, facts, and aggregates
for the MIMIC-IV data warehouse, translated from the lakehouse's Athena/Iceberg Gold DDL
(mimic_iv_ddl_gold_combined_v4.sql, schema `mimic4_db_business`) into Postgres.

Differences from the source Iceberg DDL, deliberately:
  - No S3 LOCATION / TBLPROPERTIES / Iceberg PARTITIONED BY -- those are storage-layer
    concerns specific to Iceberg-on-S3 and have no Postgres equivalent. At this dataset's
    volume (demo: ~275 admissions), native Postgres partitioning would add operational
    complexity for no query-performance benefit, so it's intentionally omitted.
  - Primary keys and foreign keys ARE added (the source has none -- Athena doesn't
    enforce them). Grain choices below were verified against the actual raw CSVs in
    data/raw/mimic-iv-clinical-database-demo-2.2, not assumed:
      - Tables with a pre-existing unique source ID use it as PK: fact_admission.hadm_id,
        fact_icu_stay_accumulating.stay_id, fact_provider_order.poe_id,
        fact_pharmacy_order.pharmacy_id, fact_lab_result.labevent_id,
        fact_microbiology_result.microevent_id, fact_transfer.transfer_id,
        fact_medication_administration.emar_id.
      - Tables whose natural grain is a composite that verified unique (see
        tests/test_warehouse.py) use it: fact_diagnosis/fact_procedure/fact_hcpcs_event
        (hadm_id, seq_num); fact_drg_assignment (hadm_id, drg_type, drg_code);
        fact_provider_order_mini_detail (poe_id, field_name); fact_service_assignment
        (hadm_id, transfer_time); fact_outpatient_measurement (subject_id, chart_date,
        seq_num, result_name).
      - Tables with no reliable natural key (event-grain ICU tables with no ID column,
        fact_prescription, and fact_medication_administration_mini_detail -- whose only
        candidate composite, (emar_id, parent_field_ordinal), has parent_field_ordinal
        NULL in half the real rows and so can't be a PK) get a surrogate
        `GENERATED ALWAYS AS IDENTITY` key. Most of these DO have a composite natural
        key that's reliably unique -- just not usable as the PK because it isn't a
        single pre-existing ID -- and get it declared as a `unique_keys` entry instead
        (see below): this is exactly the column set etl_warehouse.py's `WHERE NOT
        EXISTS (...)` anti-join already dedups on for each of those tables, so
        `unique_keys` just gives that same grain a real DB-enforced constraint (and a
        supporting index) instead of leaving it purely application-enforced. One
        exception: fact_chart_observation's equivalent candidate,
        (stay_id, item_id, chart_time, store_time), is genuinely NOT unique in the raw
        data -- 79,782 of 668,862 chartevents.csv.gz rows (~12%) share a value with
        another row on exactly those four columns (verified directly against
        data/raw/mimic-iv-clinical-database-demo-2.2/icu/chartevents.csv.gz, not
        assumed) -- so it keeps only a plain (non-unique) supporting index, same as
        today.
  - fact_provider_order.discontinue_of_poe_id / discontinued_by_poe_id are NOT
    self-referencing FKs to poe_id: that would impose an insert order within the table
    itself (a discontinuing order referencing one not yet inserted), which is an
    unnecessary operational hazard for a same-table self-reference. Left as plain columns.
  - fact_microbiology_result's spec_itemid/test_itemid/org_itemid/ab_itemid have no FK:
    the source defines no corresponding dimension for organism/antibiotic code spaces
    (only dim_lab_item and dim_chart_item exist), so no target table exists to reference.

Enforcing FK constraints means dimensions must be loaded before the facts that reference
them, and facts in dependency order (e.g. fact_admission before fact_diagnosis) -- see
WAREHOUSE_TABLE_ORDER, which `create_warehouse_tables` executes in.
"""

from __future__ import annotations

from sqlalchemy import Engine, text

WAREHOUSE_SCHEMA = "warehouse"

_AUDIT_COLUMNS: list[tuple[str, str]] = [
    ("created_ts", "TIMESTAMP"),
    ("updated_ts", "TIMESTAMP"),
    ("created_by", "TEXT"),
    ("updated_by", "TEXT"),
]

# The source DDL gives aggregate tables only created_ts/updated_ts (no created_by/
# updated_by) -- preserved here for fidelity, unlike every dim/fact above.
_AGG_AUDIT_COLUMNS: list[tuple[str, str]] = [
    ("created_ts", "TIMESTAMP"),
    ("updated_ts", "TIMESTAMP"),
]


def _cols(*pairs: tuple[str, str]) -> list[tuple[str, str]]:
    return [*pairs, *_AUDIT_COLUMNS]


def _agg_cols(*pairs: tuple[str, str]) -> list[tuple[str, str]]:
    return [*pairs, *_AGG_AUDIT_COLUMNS]


# name -> {"columns": [(col, pg_type), ...], "primary_key": (col, ...),
#          "foreign_keys": [((local_col, ...), ref_table, (ref_col, ...)), ...],
#          "unique_keys": [(col, ...), ...],  (optional, default []) -- additional
#              UNIQUE constraints beyond the primary key; see module docstring.
#          "unique_key_exprs": [(index_name, (expr, ...)), ...],  (optional, default [])
#              -- NULL-safe alternative to unique_keys: a plain UNIQUE (col, ...)
#              constraint treats NULL as never-equal-to-NULL, so a table whose dedup
#              grain has a nullable column (e.g. fact_medication_administration_mini_
#              detail's parent_field_ordinal, NULL in ~half of real rows) needs a
#              CREATE UNIQUE INDEX on COALESCE(col, sentinel) expressions instead, so
#              two NULL-grain rows collide the same way etl_warehouse.py's `ON CONFLICT`
#              loader expects (see module docstring's dedup-grain discussion).
#          "comment": str, "source_file": str}
WAREHOUSE_TABLES: dict[str, dict] = {
    # ---------------------------------------------------------------- DIMENSIONS ----
    "dim_hcpcs": {
        "columns": _cols(
            ("code", "TEXT"),
            ("category", "BIGINT"),
            ("long_description", "TEXT"),
            ("short_description", "TEXT"),
        ),
        "primary_key": ("code",),
        "foreign_keys": [],
        "comment": "Dimension table: HCPCS billing code definitions (long/short descriptions).",
        "source_file": "hosp/d_hcpcs.csv.gz",
    },
    "dim_diagnosis": {
        "columns": _cols(
            ("icd_code", "TEXT"),
            ("icd_version", "INTEGER"),
            ("long_title", "TEXT"),
        ),
        "primary_key": ("icd_code", "icd_version"),
        "foreign_keys": [],
        "comment": "Dimension table: ICD-9/ICD-10 diagnosis code definitions (title text) referenced by fact_diagnosis.",
        "source_file": "hosp/d_icd_diagnoses.csv.gz",
    },
    "dim_procedure": {
        "columns": _cols(
            ("icd_code", "TEXT"),
            ("icd_version", "INTEGER"),
            ("long_title", "TEXT"),
        ),
        "primary_key": ("icd_code", "icd_version"),
        "foreign_keys": [],
        "comment": "Dimension table: ICD-9/ICD-10 procedure code definitions (title text) referenced by fact_procedure.",
        "source_file": "hosp/d_icd_procedures.csv.gz",
    },
    "dim_lab_item": {
        "columns": _cols(
            ("item_id", "BIGINT"),
            ("label", "TEXT"),
            ("fluid", "TEXT"),
            ("category", "TEXT"),
        ),
        "primary_key": ("item_id",),
        "foreign_keys": [],
        "comment": "Dimension table: laboratory test (itemid) definitions -- label, fluid type, and category -- referenced by fact_lab_result.",
        "source_file": "hosp/d_labitems.csv.gz",
    },
    "dim_patient": {
        "columns": _cols(
            ("subject_id", "BIGINT"),
            ("gender", "TEXT"),
            ("anchor_age", "INTEGER"),
            ("anchor_year", "INTEGER"),
            ("anchor_year_group", "TEXT"),
            ("dod", "DATE"),
        ),
        "primary_key": ("subject_id",),
        "foreign_keys": [],
        "comment": "Patient-level demographics: gender, anonymized age/year anchors, and date of death (if applicable). One row per subject_id.",
        "source_file": "hosp/patients.csv.gz",
    },
    "dim_provider": {
        "columns": _cols(("provider_id", "TEXT")),
        "primary_key": ("provider_id",),
        "foreign_keys": [],
        "comment": "Dimension table: anonymized caregiver/provider identifiers referenced throughout the hosp module.",
        "source_file": "hosp/provider.csv.gz",
    },
    "dim_caregiver": {
        "columns": _cols(("caregiver_id", "BIGINT")),
        "primary_key": ("caregiver_id",),
        "foreign_keys": [],
        "comment": "Dimension table: anonymized ICU caregiver identifiers referenced throughout the ICU module fact tables.",
        "source_file": "icu/caregiver.csv.gz",
    },
    "dim_chart_item": {
        "columns": _cols(
            ("item_id", "BIGINT"),
            ("label", "TEXT"),
            ("abbreviation", "TEXT"),
            ("links_to", "TEXT"),
            ("category", "TEXT"),
            ("unit_name", "TEXT"),
            ("param_type", "TEXT"),
            ("low_normal_value", "DOUBLE PRECISION"),
            ("high_normal_value", "DOUBLE PRECISION"),
        ),
        "primary_key": ("item_id",),
        "foreign_keys": [],
        "comment": "Dimension table: ICU itemid definitions (label, category, unit of measure, normal ranges) referenced by fact_chart_observation, fact_datetime_event, fact_input_event, fact_output_event, fact_procedure_event, and fact_ingredient_event.",
        "source_file": "icu/d_items.csv.gz",
    },
    "dim_date": {
        "columns": _cols(
            ("date_key", "INTEGER"),
            ("full_date", "DATE"),
            ("day_of_week", "INTEGER"),
            ("day_name", "TEXT"),
            ("day_name_short", "TEXT"),
            ("day_of_month", "INTEGER"),
            ("day_of_year", "INTEGER"),
            ("week_of_year", "INTEGER"),
            ("week_start_date", "DATE"),
            ("week_end_date", "DATE"),
            ("month_number", "INTEGER"),
            ("month_name", "TEXT"),
            ("month_name_short", "TEXT"),
            ("first_day_of_month", "DATE"),
            ("last_day_of_month", "DATE"),
            ("quarter_number", "INTEGER"),
            ("quarter_name", "TEXT"),
            ("year", "INTEGER"),
            ("is_weekday", "BOOLEAN"),
            ("is_weekend", "BOOLEAN"),
            ("is_holiday", "BOOLEAN"),
            ("holiday_name", "TEXT"),
            ("fiscal_month", "INTEGER"),
            ("fiscal_quarter", "INTEGER"),
            ("fiscal_year", "INTEGER"),
        ),
        "primary_key": ("date_key",),
        "foreign_keys": [],
        "comment": "Standard calendar date dimension: one row per calendar day. Role-playing -- joined once per date role by every fact table below. Not sourced from a MIMIC CSV; populate separately.",
        "source_file": None,
    },
    # --------------------------------------------------------------- HOSP FACTS -----
    "fact_admission": {
        "columns": _cols(
            ("subject_id", "BIGINT"),
            ("hadm_id", "BIGINT"),
            ("admit_date_key", "INTEGER"),
            ("disch_date_key", "INTEGER"),
            ("death_date_key", "INTEGER"),
            ("ed_reg_date_key", "INTEGER"),
            ("ed_out_date_key", "INTEGER"),
            ("admit_time", "TIMESTAMP"),
            ("disch_time", "TIMESTAMP"),
            ("death_time", "TIMESTAMP"),
            ("admission_type", "TEXT"),
            ("admit_provider_id", "TEXT"),
            ("admission_location", "TEXT"),
            ("discharge_location", "TEXT"),
            ("insurance", "TEXT"),
            ("language", "TEXT"),
            ("marital_status", "TEXT"),
            ("race", "TEXT"),
            ("ed_reg_time", "TIMESTAMP"),
            ("ed_out_time", "TIMESTAMP"),
            ("hospital_expire_flag", "INTEGER"),
            ("hospital_los_hours", "DOUBLE PRECISION"),
            ("ed_los_minutes", "DOUBLE PRECISION"),
            ("time_to_death_hours", "DOUBLE PRECISION"),
            ("age_at_admission", "INTEGER"),
            ("is_readmission_flag", "BOOLEAN"),
            ("days_since_prior_discharge", "INTEGER"),
        ),
        "primary_key": ("hadm_id",),
        "foreign_keys": [
            (("subject_id",), "dim_patient", ("subject_id",)),
            (("admit_provider_id",), "dim_provider", ("provider_id",)),
            (("admit_date_key",), "dim_date", ("date_key",)),
            (("disch_date_key",), "dim_date", ("date_key",)),
            (("death_date_key",), "dim_date", ("date_key",)),
            (("ed_reg_date_key",), "dim_date", ("date_key",)),
            (("ed_out_date_key",), "dim_date", ("date_key",)),
        ],
        "comment": "Hospital admissions: one row per patient hospitalization (hadm_id).",
        "source_file": "hosp/admissions.csv.gz",
    },
    "fact_diagnosis": {
        "columns": _cols(
            ("subject_id", "BIGINT"),
            ("hadm_id", "BIGINT"),
            ("seq_num", "INTEGER"),
            ("icd_code", "TEXT"),
            ("icd_version", "INTEGER"),
        ),
        "primary_key": ("hadm_id", "seq_num"),
        "foreign_keys": [
            (("subject_id",), "dim_patient", ("subject_id",)),
            (("hadm_id",), "fact_admission", ("hadm_id",)),
            (("icd_code", "icd_version"), "dim_diagnosis", ("icd_code", "icd_version")),
        ],
        "comment": "Billed ICD diagnosis codes assigned to each hospital admission, ranked by seq_num. Factless fact.",
        "source_file": "hosp/diagnoses_icd.csv.gz",
    },
    "fact_drg_assignment": {
        "columns": _cols(
            ("subject_id", "BIGINT"),
            ("hadm_id", "BIGINT"),
            ("drg_type", "TEXT"),
            ("drg_code", "BIGINT"),
            ("description", "TEXT"),
            ("drg_severity", "INTEGER"),
            ("drg_mortality", "INTEGER"),
        ),
        "primary_key": ("hadm_id", "drg_type", "drg_code"),
        "foreign_keys": [
            (("subject_id",), "dim_patient", ("subject_id",)),
            (("hadm_id",), "fact_admission", ("hadm_id",)),
        ],
        "comment": "Diagnosis Related Group (DRG) codes assigned to each hospital admission.",
        "source_file": "hosp/drgcodes.csv.gz",
    },
    "fact_provider_order": {
        "columns": _cols(
            ("poe_id", "TEXT"),
            ("poe_seq", "INTEGER"),
            ("subject_id", "BIGINT"),
            ("hadm_id", "BIGINT"),
            ("order_date_key", "INTEGER"),
            ("order_time", "TIMESTAMP"),
            ("order_type", "TEXT"),
            ("order_subtype", "TEXT"),
            ("transaction_type", "TEXT"),
            ("discontinue_of_poe_id", "TEXT"),
            ("discontinued_by_poe_id", "TEXT"),
            ("order_provider_id", "TEXT"),
            ("order_status", "TEXT"),
        ),
        "primary_key": ("poe_id",),
        "foreign_keys": [
            (("subject_id",), "dim_patient", ("subject_id",)),
            (("hadm_id",), "fact_admission", ("hadm_id",)),
            (("order_provider_id",), "dim_provider", ("provider_id",)),
            (("order_date_key",), "dim_date", ("date_key",)),
        ],
        "comment": "Provider Order Entry: header record of every order placed for a patient.",
        "source_file": "hosp/poe.csv.gz",
    },
    "fact_medication_administration": {
        "columns": _cols(
            ("subject_id", "BIGINT"),
            ("hadm_id", "BIGINT"),
            ("emar_id", "TEXT"),
            ("emar_seq", "INTEGER"),
            ("poe_id", "TEXT"),
            ("pharmacy_id", "BIGINT"),
            ("enter_provider_id", "TEXT"),
            ("chart_date_key", "INTEGER"),
            ("schedule_date_key", "INTEGER"),
            ("store_date_key", "INTEGER"),
            ("chart_time", "TIMESTAMP"),
            ("medication", "TEXT"),
            ("event_txt", "TEXT"),
            ("schedule_time", "TIMESTAMP"),
            ("store_time", "TIMESTAMP"),
            ("admin_delay_minutes", "DOUBLE PRECISION"),
        ),
        "primary_key": ("emar_id",),
        "foreign_keys": [
            (("subject_id",), "dim_patient", ("subject_id",)),
            (("hadm_id",), "fact_admission", ("hadm_id",)),
            (("poe_id",), "fact_provider_order", ("poe_id",)),
            (("pharmacy_id",), "fact_pharmacy_order", ("pharmacy_id",)),
            (("enter_provider_id",), "dim_provider", ("provider_id",)),
            (("chart_date_key",), "dim_date", ("date_key",)),
            (("schedule_date_key",), "dim_date", ("date_key",)),
            (("store_date_key",), "dim_date", ("date_key",)),
        ],
        "comment": "Electronic Medication Administration Record: header-level record of each medication administration event.",
        "source_file": "hosp/emar.csv.gz",
    },
    "fact_medication_administration_mini_detail": {
        "columns": _cols(
            ("med_admin_detail_id", "BIGINT GENERATED ALWAYS AS IDENTITY"),
            ("subject_id", "BIGINT"),
            ("emar_id", "TEXT"),
            ("emar_seq", "INTEGER"),
            ("parent_field_ordinal", "DOUBLE PRECISION"),
            ("administration_type", "TEXT"),
            ("pharmacy_id", "BIGINT"),
            ("barcode_type", "TEXT"),
            ("reason_for_no_barcode", "TEXT"),
            ("complete_dose_not_given", "TEXT"),
            # TEXT, not DOUBLE PRECISION: real values include dose ranges for
            # variable/PRN dosing (e.g. "1-2", "0-50"), same as prescriptions.form_val_disp.
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
            ("infusion_rate_adjustment_amount", "DOUBLE PRECISION"),
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
        ),
        "primary_key": ("med_admin_detail_id",),
        "unique_key_exprs": [(
            "ux_med_admin_mini_detail_grain",
            ('COALESCE("emar_id", \'\')', 'COALESCE("emar_seq", -1)', 'COALESCE("parent_field_ordinal", -1)'),
        )],
        "foreign_keys": [
            (("subject_id",), "dim_patient", ("subject_id",)),
            (("emar_id",), "fact_medication_administration", ("emar_id",)),
            (("pharmacy_id",), "fact_pharmacy_order", ("pharmacy_id",)),
        ],
        "comment": "Electronic Medication Administration Record detail: granular dose/administration attributes for each emar_id. Surrogate PK -- (emar_id, parent_field_ordinal) can't be one, parent_field_ordinal is NULL in half the real rows. UNIQUE INDEX ux_med_admin_mini_detail_grain on COALESCE(emar_id,''), COALESCE(emar_seq,-1), COALESCE(parent_field_ordinal,-1) is the dedup grain etl_warehouse.py's ON CONFLICT loader uses -- an expression index (not a plain UNIQUE) so two NULL-parent_field_ordinal rows still collide, since a plain UNIQUE treats NULL as never-equal-to-NULL.",
        "source_file": "hosp/emar_detail.csv.gz",
    },
    "fact_hcpcs_event": {
        "columns": _cols(
            ("subject_id", "BIGINT"),
            ("hadm_id", "BIGINT"),
            ("chart_date_key", "INTEGER"),
            ("chart_date", "DATE"),
            ("hcpcs_cd", "TEXT"),
            ("seq_num", "INTEGER"),
            ("short_description", "TEXT"),
        ),
        "primary_key": ("hadm_id", "seq_num"),
        "foreign_keys": [
            (("subject_id",), "dim_patient", ("subject_id",)),
            (("hadm_id",), "fact_admission", ("hadm_id",)),
            (("hcpcs_cd",), "dim_hcpcs", ("code",)),
            (("chart_date_key",), "dim_date", ("date_key",)),
        ],
        "comment": "Billed HCPCS procedure/service codes recorded against a hospital admission.",
        "source_file": "hosp/hcpcsevents.csv.gz",
    },
    "fact_lab_result": {
        "columns": _cols(
            ("labevent_id", "BIGINT"),
            ("subject_id", "BIGINT"),
            ("hadm_id", "BIGINT"),
            ("specimen_id", "BIGINT"),
            ("item_id", "BIGINT"),
            ("order_provider_id", "TEXT"),
            ("chart_date_key", "INTEGER"),
            ("store_date_key", "INTEGER"),
            ("chart_time", "TIMESTAMP"),
            ("store_time", "TIMESTAMP"),
            ("value", "TEXT"),
            ("value_num", "DOUBLE PRECISION"),
            ("value_uom", "TEXT"),
            ("ref_range_lower", "DOUBLE PRECISION"),
            ("ref_range_upper", "DOUBLE PRECISION"),
            ("flag", "TEXT"),
            ("priority", "TEXT"),
            ("comments", "TEXT"),
            ("result_turnaround_minutes", "DOUBLE PRECISION"),
            ("is_abnormal_flag", "BOOLEAN"),
        ),
        "primary_key": ("labevent_id",),
        "foreign_keys": [
            (("subject_id",), "dim_patient", ("subject_id",)),
            (("hadm_id",), "fact_admission", ("hadm_id",)),
            (("item_id",), "dim_lab_item", ("item_id",)),
            (("order_provider_id",), "dim_provider", ("provider_id",)),
            (("chart_date_key",), "dim_date", ("date_key",)),
            (("store_date_key",), "dim_date", ("date_key",)),
        ],
        "comment": "Laboratory test results collected for a patient, optionally tied to a hospital admission.",
        "source_file": "hosp/labevents.csv.gz",
    },
    "fact_microbiology_result": {
        "columns": _cols(
            ("microevent_id", "BIGINT"),
            ("subject_id", "BIGINT"),
            ("hadm_id", "BIGINT"),
            ("micro_specimen_id", "BIGINT"),
            ("order_provider_id", "TEXT"),
            ("chart_date_key", "INTEGER"),
            ("store_date_key", "INTEGER"),
            ("chart_date", "TIMESTAMP"),
            ("chart_time", "TIMESTAMP"),
            ("spec_itemid", "BIGINT"),
            ("spec_type_desc", "TEXT"),
            ("test_seq", "INTEGER"),
            ("store_date", "TIMESTAMP"),
            ("store_time", "TIMESTAMP"),
            ("test_itemid", "BIGINT"),
            ("test_name", "TEXT"),
            ("org_itemid", "BIGINT"),
            ("org_name", "TEXT"),
            ("isolate_num", "INTEGER"),
            ("quantity", "DOUBLE PRECISION"),
            ("ab_itemid", "BIGINT"),
            ("ab_name", "TEXT"),
            ("dilution_text", "TEXT"),
            ("dilution_comparison", "TEXT"),
            ("dilution_value", "DOUBLE PRECISION"),
            ("interpretation", "TEXT"),
            ("comments", "TEXT"),
            ("result_turnaround_minutes", "DOUBLE PRECISION"),
            ("is_positive_culture_flag", "BOOLEAN"),
        ),
        "primary_key": ("microevent_id",),
        "foreign_keys": [
            (("subject_id",), "dim_patient", ("subject_id",)),
            (("hadm_id",), "fact_admission", ("hadm_id",)),
            (("order_provider_id",), "dim_provider", ("provider_id",)),
            (("chart_date_key",), "dim_date", ("date_key",)),
            (("store_date_key",), "dim_date", ("date_key",)),
        ],
        "comment": "Microbiology culture orders, organisms isolated, and antibiotic sensitivity results. spec/test/org/ab itemid columns have no FK -- no dimension table exists for those code spaces in the source.",
        "source_file": "hosp/microbiologyevents.csv.gz",
    },
    "fact_outpatient_measurement": {
        "columns": _cols(
            ("subject_id", "BIGINT"),
            ("chart_date_key", "INTEGER"),
            ("chart_date", "DATE"),
            ("seq_num", "INTEGER"),
            ("result_name", "TEXT"),
            ("result_value", "TEXT"),
        ),
        "primary_key": ("subject_id", "chart_date", "seq_num", "result_name"),
        "foreign_keys": [
            (("subject_id",), "dim_patient", ("subject_id",)),
            (("chart_date_key",), "dim_date", ("date_key",)),
        ],
        "comment": "Online Medical Record: outpatient-recorded observations such as height, weight, and blood pressure.",
        "source_file": "hosp/omr.csv.gz",
    },
    "fact_pharmacy_order": {
        "columns": _cols(
            ("subject_id", "BIGINT"),
            ("hadm_id", "BIGINT"),
            ("pharmacy_id", "BIGINT"),
            ("poe_id", "TEXT"),
            ("start_date_key", "INTEGER"),
            ("stop_date_key", "INTEGER"),
            ("enter_date_key", "INTEGER"),
            ("verified_date_key", "INTEGER"),
            ("start_time", "TIMESTAMP"),
            ("stop_time", "TIMESTAMP"),
            ("medication", "TEXT"),
            ("proc_type", "TEXT"),
            ("status", "TEXT"),
            ("enter_time", "TIMESTAMP"),
            ("verified_time", "TIMESTAMP"),
            ("route", "TEXT"),
            ("frequency", "TEXT"),
            ("disp_sched", "TEXT"),
            ("infusion_type", "TEXT"),
            ("sliding_scale", "TEXT"),
            ("lockout_interval", "DOUBLE PRECISION"),
            ("basal_rate", "DOUBLE PRECISION"),
            ("one_hr_max", "DOUBLE PRECISION"),
            ("doses_per_24_hrs", "DOUBLE PRECISION"),
            ("duration", "DOUBLE PRECISION"),
            ("duration_interval", "TEXT"),
            ("expiration_value", "DOUBLE PRECISION"),
            ("expiration_unit", "TEXT"),
            ("expiration_date", "DATE"),
            ("dispensation", "TEXT"),
            ("fill_quantity", "DOUBLE PRECISION"),
        ),
        "primary_key": ("pharmacy_id",),
        "foreign_keys": [
            (("subject_id",), "dim_patient", ("subject_id",)),
            (("hadm_id",), "fact_admission", ("hadm_id",)),
            (("poe_id",), "fact_provider_order", ("poe_id",)),
            (("start_date_key",), "dim_date", ("date_key",)),
            (("stop_date_key",), "dim_date", ("date_key",)),
            (("enter_date_key",), "dim_date", ("date_key",)),
            (("verified_date_key",), "dim_date", ("date_key",)),
        ],
        "comment": "Pharmacy dispensing records: medication orders with dosing schedule, route, frequency, and dispensation details.",
        "source_file": "hosp/pharmacy.csv.gz",
    },
    "fact_provider_order_mini_detail": {
        "columns": _cols(
            ("poe_id", "TEXT"),
            ("poe_seq", "INTEGER"),
            ("subject_id", "BIGINT"),
            ("field_name", "TEXT"),
            ("field_value", "TEXT"),
        ),
        "primary_key": ("poe_id", "field_name"),
        "foreign_keys": [
            (("subject_id",), "dim_patient", ("subject_id",)),
            (("poe_id",), "fact_provider_order", ("poe_id",)),
        ],
        "comment": "Provider Order Entry detail: free-text field name/value pairs providing additional context for a poe_id.",
        "source_file": "hosp/poe_detail.csv.gz",
    },
    "fact_prescription": {
        "columns": _cols(
            ("prescription_id", "BIGINT GENERATED ALWAYS AS IDENTITY"),
            ("subject_id", "BIGINT"),
            ("hadm_id", "BIGINT"),
            ("pharmacy_id", "BIGINT"),
            ("poe_id", "TEXT"),
            ("poe_seq", "INTEGER"),
            ("order_provider_id", "TEXT"),
            ("start_date_key", "INTEGER"),
            ("stop_date_key", "INTEGER"),
            ("start_time", "TIMESTAMP"),
            ("stop_time", "TIMESTAMP"),
            ("drug_type", "TEXT"),
            ("drug", "TEXT"),
            ("formulary_drug_cd", "TEXT"),
            ("gsn", "TEXT"),
            ("ndc", "BIGINT"),
            ("prod_strength", "TEXT"),
            ("form_rx", "TEXT"),
            ("dose_val_rx", "TEXT"),
            ("dose_unit_rx", "TEXT"),
            # TEXT, not DOUBLE PRECISION: real values include dose ranges for
            # variable/PRN dosing (e.g. "1-2"), which no numeric type can hold.
            ("form_val_disp", "TEXT"),
            ("form_unit_disp", "TEXT"),
            ("doses_per_24_hrs", "DOUBLE PRECISION"),
            ("route", "TEXT"),
        ),
        "primary_key": ("prescription_id",),
        "unique_key_exprs": [(
            "ux_fact_prescription_grain",
            (
                'COALESCE("subject_id", -1)', 'COALESCE("hadm_id", -1)', 'COALESCE("pharmacy_id", -1)',
                'COALESCE("poe_id", \'\')', 'COALESCE("poe_seq", -1)',
                'COALESCE("start_time", TIMESTAMP \'epoch\')', 'COALESCE("drug", \'\')',
            ),
        )],
        "foreign_keys": [
            (("subject_id",), "dim_patient", ("subject_id",)),
            (("hadm_id",), "fact_admission", ("hadm_id",)),
            (("pharmacy_id",), "fact_pharmacy_order", ("pharmacy_id",)),
            (("poe_id",), "fact_provider_order", ("poe_id",)),
            (("order_provider_id",), "dim_provider", ("provider_id",)),
            (("start_date_key",), "dim_date", ("date_key",)),
            (("stop_date_key",), "dim_date", ("date_key",)),
        ],
        "comment": "Prescribed medication orders including drug identifiers (NDC, GSN), dose, formulary code, and route. Surrogate PK -- no natural unique column in the source. UNIQUE INDEX ux_fact_prescription_grain on COALESCE(subject_id,-1), COALESCE(hadm_id,-1), COALESCE(pharmacy_id,-1), COALESCE(poe_id,''), COALESCE(poe_seq,-1), COALESCE(start_time,epoch), COALESCE(drug,'') is the dedup grain etl_warehouse.py's ON CONFLICT loader uses -- an expression index (not a plain UNIQUE) so NULL-containing grains (pharmacy_id/poe_id are unresolved for a real fraction of rows) still collide correctly.",
        "source_file": "hosp/prescriptions.csv.gz",
    },
    "fact_procedure": {
        "columns": _cols(
            ("subject_id", "BIGINT"),
            ("hadm_id", "BIGINT"),
            ("seq_num", "INTEGER"),
            ("chart_date_key", "INTEGER"),
            ("chart_date", "DATE"),
            ("icd_code", "TEXT"),
            ("icd_version", "INTEGER"),
        ),
        "primary_key": ("hadm_id", "seq_num"),
        "foreign_keys": [
            (("subject_id",), "dim_patient", ("subject_id",)),
            (("hadm_id",), "fact_admission", ("hadm_id",)),
            (("chart_date_key",), "dim_date", ("date_key",)),
            (("icd_code", "icd_version"), "dim_procedure", ("icd_code", "icd_version")),
        ],
        "comment": "Billed ICD procedure codes performed during a hospital admission, ranked by seq_num. Factless fact.",
        "source_file": "hosp/procedures_icd.csv.gz",
    },
    "fact_service_assignment": {
        "columns": _cols(
            ("subject_id", "BIGINT"),
            ("hadm_id", "BIGINT"),
            ("transfer_date_key", "INTEGER"),
            ("transfer_time", "TIMESTAMP"),
            ("prev_service", "TEXT"),
            ("curr_service", "TEXT"),
        ),
        "primary_key": ("hadm_id", "transfer_time"),
        "foreign_keys": [
            (("subject_id",), "dim_patient", ("subject_id",)),
            (("hadm_id",), "fact_admission", ("hadm_id",)),
            (("transfer_date_key",), "dim_date", ("date_key",)),
        ],
        "comment": "Hospital service transfers: record of the clinical service a patient was under during an admission.",
        "source_file": "hosp/services.csv.gz",
    },
    "fact_transfer": {
        "columns": _cols(
            ("subject_id", "BIGINT"),
            ("hadm_id", "BIGINT"),
            ("transfer_id", "BIGINT"),
            ("event_type", "TEXT"),
            ("care_unit", "TEXT"),
            ("in_date_key", "INTEGER"),
            ("out_date_key", "INTEGER"),
            ("in_time", "TIMESTAMP"),
            ("out_time", "TIMESTAMP"),
            ("transfer_duration_hours", "DOUBLE PRECISION"),
        ),
        "primary_key": ("transfer_id",),
        "foreign_keys": [
            (("subject_id",), "dim_patient", ("subject_id",)),
            (("hadm_id",), "fact_admission", ("hadm_id",)),
            (("in_date_key",), "dim_date", ("date_key",)),
            (("out_date_key",), "dim_date", ("date_key",)),
        ],
        "comment": "Patient movement between hospital units/wards (ADT events) during an admission, including ICU stay boundaries.",
        "source_file": "hosp/transfers.csv.gz",
    },
    # ---------------------------------------------------------------- ICU FACTS -----
    "fact_icu_stay_accumulating": {
        "columns": _cols(
            ("subject_id", "BIGINT"),
            ("hadm_id", "BIGINT"),
            ("stay_id", "BIGINT"),
            ("first_careunit", "TEXT"),
            ("last_careunit", "TEXT"),
            ("in_date_key", "INTEGER"),
            ("out_date_key", "INTEGER"),
            ("in_time", "TIMESTAMP"),
            ("out_time", "TIMESTAMP"),
            ("los", "DOUBLE PRECISION"),
            ("time_to_icu_hours", "DOUBLE PRECISION"),
            ("is_icu_readmission_flag", "BOOLEAN"),
        ),
        "primary_key": ("stay_id",),
        "foreign_keys": [
            (("subject_id",), "dim_patient", ("subject_id",)),
            (("hadm_id",), "fact_admission", ("hadm_id",)),
            (("in_date_key",), "dim_date", ("date_key",)),
            (("out_date_key",), "dim_date", ("date_key",)),
        ],
        "comment": "One row per ICU stay (stay_id). Accumulating-snapshot fact: child grain of fact_admission.",
        "source_file": "icu/icustays.csv.gz",
    },
    "fact_chart_observation": {
        "columns": _cols(
            ("chart_observation_id", "BIGINT GENERATED ALWAYS AS IDENTITY"),
            ("subject_id", "BIGINT"),
            ("hadm_id", "BIGINT"),
            ("stay_id", "BIGINT"),
            ("caregiver_id", "BIGINT"),
            ("chart_date_key", "INTEGER"),
            ("store_date_key", "INTEGER"),
            ("chart_time", "TIMESTAMP"),
            ("store_time", "TIMESTAMP"),
            ("item_id", "BIGINT"),
            ("value", "TEXT"),
            ("value_num", "DOUBLE PRECISION"),
            ("value_uom", "TEXT"),
            ("warning", "INTEGER"),
            ("charting_delay_minutes", "DOUBLE PRECISION"),
            ("is_abnormal_flag", "BOOLEAN"),
        ),
        "primary_key": ("chart_observation_id",),
        # No unique_keys entry: the equivalent candidate grain, (stay_id, item_id,
        # chart_time, store_time), is genuinely NOT unique here -- 79,782 of 668,862
        # chartevents.csv.gz rows (~12%) collide with another row on exactly those four
        # columns (verified directly against the raw data, not assumed). A UNIQUE
        # constraint on it would reject real rows on every load. Keeps only the plain
        # (non-unique) ix_fchartobs_dedup index from etl_warehouse.py.
        "foreign_keys": [
            (("subject_id",), "dim_patient", ("subject_id",)),
            (("hadm_id",), "fact_admission", ("hadm_id",)),
            (("stay_id",), "fact_icu_stay_accumulating", ("stay_id",)),
            (("caregiver_id",), "dim_caregiver", ("caregiver_id",)),
            (("item_id",), "dim_chart_item", ("item_id",)),
            (("chart_date_key",), "dim_date", ("date_key",)),
            (("store_date_key",), "dim_date", ("date_key",)),
        ],
        "comment": "Charted vital signs and observations recorded at the bedside during an ICU stay. Highest-volume ICU fact table; surrogate PK (no natural ID column in source). No UNIQUE constraint on (stay_id, item_id, chart_time, store_time): ~12% of raw chartevents.csv.gz rows genuinely collide on it.",
        "source_file": "icu/chartevents.csv.gz",
    },
    "fact_datetime_event": {
        "columns": _cols(
            ("datetime_event_id", "BIGINT GENERATED ALWAYS AS IDENTITY"),
            ("subject_id", "BIGINT"),
            ("hadm_id", "BIGINT"),
            ("stay_id", "BIGINT"),
            ("caregiver_id", "BIGINT"),
            ("chart_date_key", "INTEGER"),
            ("store_date_key", "INTEGER"),
            ("chart_time", "TIMESTAMP"),
            ("store_time", "TIMESTAMP"),
            ("item_id", "BIGINT"),
            ("value", "TEXT"),
            ("value_uom", "TEXT"),
            ("warning", "INTEGER"),
            ("charting_delay_minutes", "DOUBLE PRECISION"),
        ),
        "primary_key": ("datetime_event_id",),
        "unique_key_exprs": [(
            "ux_fact_datetime_event_grain",
            (
                'COALESCE("stay_id", -1)', 'COALESCE("item_id", -1)',
                'COALESCE("chart_time", TIMESTAMP \'epoch\')', 'COALESCE("store_time", TIMESTAMP \'epoch\')',
            ),
        )],
        "foreign_keys": [
            (("subject_id",), "dim_patient", ("subject_id",)),
            (("hadm_id",), "fact_admission", ("hadm_id",)),
            (("stay_id",), "fact_icu_stay_accumulating", ("stay_id",)),
            (("caregiver_id",), "dim_caregiver", ("caregiver_id",)),
            (("item_id",), "dim_chart_item", ("item_id",)),
            (("chart_date_key",), "dim_date", ("date_key",)),
            (("store_date_key",), "dim_date", ("date_key",)),
        ],
        "comment": "ICU-charted events whose value is itself a date/time, recorded during an ICU stay. Surrogate PK (no natural ID column in source). UNIQUE INDEX ux_fact_datetime_event_grain on COALESCE(stay_id,-1), COALESCE(item_id,-1), COALESCE(chart_time,epoch), COALESCE(store_time,epoch) is the dedup grain etl_warehouse.py's ON CONFLICT loader uses -- an expression index (not a plain UNIQUE) so a NULL store_time still collides correctly.",
        "source_file": "icu/datetimeevents.csv.gz",
    },
    "fact_input_event": {
        "columns": _cols(
            ("input_event_id", "BIGINT GENERATED ALWAYS AS IDENTITY"),
            ("subject_id", "BIGINT"),
            ("hadm_id", "BIGINT"),
            ("stay_id", "BIGINT"),
            ("caregiver_id", "BIGINT"),
            ("start_date_key", "INTEGER"),
            ("end_date_key", "INTEGER"),
            ("store_date_key", "INTEGER"),
            ("start_time", "TIMESTAMP"),
            ("end_time", "TIMESTAMP"),
            ("store_time", "TIMESTAMP"),
            ("duration_minutes", "DOUBLE PRECISION"),
            ("item_id", "BIGINT"),
            ("amount", "DOUBLE PRECISION"),
            ("amount_uom", "TEXT"),
            ("rate", "DOUBLE PRECISION"),
            ("rate_uom", "TEXT"),
            ("order_id", "BIGINT"),
            ("link_order_id", "BIGINT"),
            ("order_category_name", "TEXT"),
            ("secondary_order_category_name", "TEXT"),
            ("order_component_type_description", "TEXT"),
            ("order_category_description", "TEXT"),
            ("patient_weight", "DOUBLE PRECISION"),
            ("total_amount", "DOUBLE PRECISION"),
            ("total_amount_uom", "TEXT"),
            ("is_open_bag", "INTEGER"),
            ("continue_in_next_dept", "INTEGER"),
            ("status_description", "TEXT"),
            ("original_amount", "DOUBLE PRECISION"),
            ("original_rate", "DOUBLE PRECISION"),
        ),
        "primary_key": ("input_event_id",),
        "unique_key_exprs": [(
            "ux_fact_input_event_grain",
            (
                'COALESCE("stay_id", -1)', 'COALESCE("item_id", -1)',
                'COALESCE("start_time", TIMESTAMP \'epoch\')', 'COALESCE("end_time", TIMESTAMP \'epoch\')',
                'COALESCE("order_id", -1)',
            ),
        )],
        "foreign_keys": [
            (("subject_id",), "dim_patient", ("subject_id",)),
            (("hadm_id",), "fact_admission", ("hadm_id",)),
            (("stay_id",), "fact_icu_stay_accumulating", ("stay_id",)),
            (("caregiver_id",), "dim_caregiver", ("caregiver_id",)),
            (("item_id",), "dim_chart_item", ("item_id",)),
            (("start_date_key",), "dim_date", ("date_key",)),
            (("end_date_key",), "dim_date", ("date_key",)),
            (("store_date_key",), "dim_date", ("date_key",)),
        ],
        "comment": "Intake events: fluids, medications, and nutrition administered during an ICU stay. Surrogate PK (no natural ID column in source). UNIQUE INDEX ux_fact_input_event_grain on COALESCE(stay_id,-1), COALESCE(item_id,-1), COALESCE(start_time,epoch), COALESCE(end_time,epoch), COALESCE(order_id,-1) is the dedup grain etl_warehouse.py's ON CONFLICT loader uses -- an expression index (not a plain UNIQUE) so a NULL order_id/end_time still collides correctly.",
        "source_file": "icu/inputevents.csv.gz",
    },
    "fact_ingredient_event": {
        "columns": _cols(
            ("ingredient_event_id", "BIGINT GENERATED ALWAYS AS IDENTITY"),
            ("subject_id", "BIGINT"),
            ("hadm_id", "BIGINT"),
            ("stay_id", "BIGINT"),
            ("caregiver_id", "BIGINT"),
            ("start_date_key", "INTEGER"),
            ("end_date_key", "INTEGER"),
            ("store_date_key", "INTEGER"),
            ("start_time", "TIMESTAMP"),
            ("end_time", "TIMESTAMP"),
            ("store_time", "TIMESTAMP"),
            ("duration_minutes", "DOUBLE PRECISION"),
            ("item_id", "BIGINT"),
            ("amount", "DOUBLE PRECISION"),
            ("amount_uom", "TEXT"),
            ("rate", "DOUBLE PRECISION"),
            ("rate_uom", "TEXT"),
            ("order_id", "BIGINT"),
            ("link_order_id", "BIGINT"),
            ("status_description", "TEXT"),
            ("original_amount", "BIGINT"),
            ("original_rate", "DOUBLE PRECISION"),
        ),
        "primary_key": ("ingredient_event_id",),
        "unique_key_exprs": [(
            "ux_fact_ingredient_event_grain",
            (
                'COALESCE("stay_id", -1)', 'COALESCE("item_id", -1)',
                'COALESCE("start_time", TIMESTAMP \'epoch\')', 'COALESCE("end_time", TIMESTAMP \'epoch\')',
                'COALESCE("order_id", -1)',
            ),
        )],
        "foreign_keys": [
            (("subject_id",), "dim_patient", ("subject_id",)),
            (("hadm_id",), "fact_admission", ("hadm_id",)),
            (("stay_id",), "fact_icu_stay_accumulating", ("stay_id",)),
            (("caregiver_id",), "dim_caregiver", ("caregiver_id",)),
            (("item_id",), "dim_chart_item", ("item_id",)),
            (("start_date_key",), "dim_date", ("date_key",)),
            (("end_date_key",), "dim_date", ("date_key",)),
            (("store_date_key",), "dim_date", ("date_key",)),
        ],
        "comment": "Individual ingredient-level detail underlying fact_input_event records. Child grain of fact_input_event; surrogate PK. UNIQUE INDEX ux_fact_ingredient_event_grain on COALESCE(stay_id,-1), COALESCE(item_id,-1), COALESCE(start_time,epoch), COALESCE(end_time,epoch), COALESCE(order_id,-1) is the dedup grain etl_warehouse.py's ON CONFLICT loader uses -- an expression index (not a plain UNIQUE) so a NULL order_id/end_time still collides correctly.",
        "source_file": "icu/ingredientevents.csv.gz",
    },
    "fact_output_event": {
        "columns": _cols(
            ("output_event_id", "BIGINT GENERATED ALWAYS AS IDENTITY"),
            ("subject_id", "BIGINT"),
            ("hadm_id", "BIGINT"),
            ("stay_id", "BIGINT"),
            ("caregiver_id", "BIGINT"),
            ("chart_date_key", "INTEGER"),
            ("store_date_key", "INTEGER"),
            ("chart_time", "TIMESTAMP"),
            ("store_time", "TIMESTAMP"),
            ("item_id", "BIGINT"),
            ("value", "BIGINT"),
            ("value_uom", "TEXT"),
            ("charting_delay_minutes", "DOUBLE PRECISION"),
        ),
        "primary_key": ("output_event_id",),
        "unique_key_exprs": [(
            "ux_fact_output_event_grain",
            (
                'COALESCE("stay_id", -1)', 'COALESCE("item_id", -1)',
                'COALESCE("chart_time", TIMESTAMP \'epoch\')', 'COALESCE("store_time", TIMESTAMP \'epoch\')',
            ),
        )],
        "foreign_keys": [
            (("subject_id",), "dim_patient", ("subject_id",)),
            (("hadm_id",), "fact_admission", ("hadm_id",)),
            (("stay_id",), "fact_icu_stay_accumulating", ("stay_id",)),
            (("caregiver_id",), "dim_caregiver", ("caregiver_id",)),
            (("item_id",), "dim_chart_item", ("item_id",)),
            (("chart_date_key",), "dim_date", ("date_key",)),
            (("store_date_key",), "dim_date", ("date_key",)),
        ],
        "comment": "Output events: fluid output recorded for a patient during an ICU stay. Surrogate PK (no natural ID column in source). UNIQUE INDEX ux_fact_output_event_grain on COALESCE(stay_id,-1), COALESCE(item_id,-1), COALESCE(chart_time,epoch), COALESCE(store_time,epoch) is the dedup grain etl_warehouse.py's ON CONFLICT loader uses -- an expression index (not a plain UNIQUE) so a NULL store_time still collides correctly.",
        "source_file": "icu/outputevents.csv.gz",
    },
    "fact_procedure_event": {
        "columns": _cols(
            ("procedure_event_id", "BIGINT GENERATED ALWAYS AS IDENTITY"),
            ("subject_id", "BIGINT"),
            ("hadm_id", "BIGINT"),
            ("stay_id", "BIGINT"),
            ("caregiver_id", "BIGINT"),
            ("start_date_key", "INTEGER"),
            ("end_date_key", "INTEGER"),
            ("store_date_key", "INTEGER"),
            ("start_time", "TIMESTAMP"),
            ("end_time", "TIMESTAMP"),
            ("store_time", "TIMESTAMP"),
            ("duration_minutes", "DOUBLE PRECISION"),
            ("item_id", "BIGINT"),
            ("value", "DOUBLE PRECISION"),
            ("value_uom", "TEXT"),
            ("location", "TEXT"),
            ("location_category", "TEXT"),
            ("order_id", "BIGINT"),
            ("link_order_id", "BIGINT"),
            ("order_category_name", "TEXT"),
            ("order_category_description", "TEXT"),
            ("patient_weight", "DOUBLE PRECISION"),
            ("is_open_bag", "INTEGER"),
            ("continue_in_next_dept", "INTEGER"),
            ("status_description", "TEXT"),
            ("original_amount", "BIGINT"),
            ("original_rate", "INTEGER"),
        ),
        "primary_key": ("procedure_event_id",),
        "unique_key_exprs": [(
            "ux_fact_procedure_event_grain",
            (
                'COALESCE("stay_id", -1)', 'COALESCE("item_id", -1)',
                'COALESCE("start_time", TIMESTAMP \'epoch\')', 'COALESCE("end_time", TIMESTAMP \'epoch\')',
                'COALESCE("order_id", -1)',
            ),
        )],
        "foreign_keys": [
            (("subject_id",), "dim_patient", ("subject_id",)),
            (("hadm_id",), "fact_admission", ("hadm_id",)),
            (("stay_id",), "fact_icu_stay_accumulating", ("stay_id",)),
            (("caregiver_id",), "dim_caregiver", ("caregiver_id",)),
            (("item_id",), "dim_chart_item", ("item_id",)),
            (("start_date_key",), "dim_date", ("date_key",)),
            (("end_date_key",), "dim_date", ("date_key",)),
            (("store_date_key",), "dim_date", ("date_key",)),
        ],
        "comment": "Procedures with a duration (e.g. ventilation, dialysis) performed during an ICU stay. Distinct from fact_procedure (billed ICD codes). Surrogate PK. UNIQUE INDEX ux_fact_procedure_event_grain on COALESCE(stay_id,-1), COALESCE(item_id,-1), COALESCE(start_time,epoch), COALESCE(end_time,epoch), COALESCE(order_id,-1) is the dedup grain etl_warehouse.py's ON CONFLICT loader uses -- an expression index (not a plain UNIQUE) so a NULL order_id/end_time still collides correctly.",
        "source_file": "icu/procedureevents.csv.gz",
    },
    # ------------------------------------------------------------- AGGREGATES -------
    # Incrementally-refreshed rollups, not full-reload tables like the dims/facts
    # above -- see etl_warehouse.py's AGGREGATE_LOAD_SQL. No created_by/updated_by
    # (see _agg_cols): the source DDL gives these tables only created_ts/updated_ts.
    "agg_admission_daily": {
        "columns": _agg_cols(
            ("date_key", "INTEGER"),
            ("admission_type", "TEXT"),
            ("admit_count", "INTEGER"),
            ("discharge_count", "INTEGER"),
            ("death_count", "INTEGER"),
        ),
        "primary_key": ("date_key", "admission_type"),
        "foreign_keys": [(("date_key",), "dim_date", ("date_key",))],
        "comment": "Daily admission/discharge/mortality counts per admission_type, rolled up from fact_admission.",
        "source_file": None,
    },
    "agg_icu_fluid_balance_daily": {
        "columns": _agg_cols(
            ("date_key", "INTEGER"),
            ("stay_id", "BIGINT"),
            ("total_intake_ml", "DOUBLE PRECISION"),
            ("total_output_ml", "DOUBLE PRECISION"),
            ("net_balance_ml", "DOUBLE PRECISION"),
        ),
        "primary_key": ("date_key", "stay_id"),
        "foreign_keys": [
            (("date_key",), "dim_date", ("date_key",)),
            (("stay_id",), "fact_icu_stay_accumulating", ("stay_id",)),
        ],
        "comment": "Daily net ICU fluid balance per stay_id: total_intake_ml (fact_input_event, amount_uom='ml' only) minus total_output_ml (fact_output_event), rolled up.",
        "source_file": None,
    },
    "agg_admission_monthly": {
        "columns": _agg_cols(
            ("year_month", "INTEGER"),
            ("admission_type", "TEXT"),
            ("admit_count", "INTEGER"),
            ("discharge_count", "INTEGER"),
            ("death_count", "INTEGER"),
        ),
        "primary_key": ("year_month", "admission_type"),
        # No FK to dim_date: year_month (YYYYMM) is coarser than dim_date's day grain.
        "foreign_keys": [],
        "comment": "Monthly admission/discharge/mortality counts per admission_type, rolled up from agg_admission_daily (not directly from fact_admission).",
        "source_file": None,
    },
    # ------------------------------------------------------------- PROCESS LOG ------
    "etl_process_log": {
        # No _cols()/_agg_cols() audit trailer -- this table IS the audit trail, and
        # start_ts/end_ts/logged_ts already cover what created_ts/updated_ts would.
        # Column set deliberately mirrors the lakehouse's mimic4_db_business.etl_process_log
        # exactly (same names/order) -- log_id is the one addition, a surrogate PK Postgres
        # requires that Athena/Iceberg has no equivalent of.
        "columns": [
            ("log_id", "BIGINT GENERATED ALWAYS AS IDENTITY"),
            ("full_run_id", "BIGINT"),
            ("run_id", "TEXT"),
            ("job_name", "TEXT"),
            ("process_name", "TEXT"),
            ("process_type", "TEXT"),
            ("phase", "TEXT"),
            ("dataset", "TEXT"),
            ("start_ts", "TIMESTAMP"),
            ("end_ts", "TIMESTAMP"),
            ("execution_seconds", "DOUBLE PRECISION"),
            ("dpu_seconds", "DOUBLE PRECISION"),
            ("raw_row_count", "BIGINT"),
            ("gold_row_count", "BIGINT"),
            ("status", "TEXT"),
            ("error_message", "TEXT"),
            ("logged_ts", "TIMESTAMP"),
        ],
        "primary_key": ("log_id",),
        "foreign_keys": [],
        "comment": (
            "Append-only per-table timing/row-count log written by glue_jobs/glue_common.py's "
            "log_timed_call() from inside each of the 4 Glue jobs' (staging/dimensions/facts/"
            "aggregates) per-table loops -- the Postgres/coarser-job-grain counterpart to the "
            "lakehouse's mimic4_db_business.etl_process_log, with the same columns. full_run_id "
            "is one integer shared by every row from a single `mimic-load glue run-all` "
            "invocation (computed once in cli.py via MAX(full_run_id)+1 and passed to each of "
            "the 4 independently-launched Glue jobs as --FULL_RUN_ID; a standalone single-step "
            "command such as `glue run-facts` gets its own fresh full_run_id). NOT safe against "
            "concurrent invocations, same caveat as the lakehouse. phase is the glue_run_all "
            "step name, letter-prefixed to align with the lakehouse's A-J scheme: "
            "A:staging, B:dimensions, C:facts, D:aggregates -- coarser than process_type "
            "(dim/fact/agg/staging) the same way the lakehouse's phase letters group multiple "
            "process_types together (the lakehouse splits facts/aggregates further into "
            "several letters since it runs each table as its own parallel Glue job; this "
            "pipeline runs facts/aggregates as one sequential per-table loop inside a single "
            "Glue job each, so one letter per stage is enough here). dpu_seconds is NULL at "
            "insert time (a table's row is logged mid-job-run, before the job's total "
            "DPUSeconds is known) and "
            "is backfilled by cli.py's _run_glue_step for every row of a run_id once that Glue "
            "job run reaches a terminal state, via glue.get_job_run_dpu_seconds -- same "
            "after-the-fact-from-the-orchestrator pattern the lakehouse uses, just delayed by "
            "one step since a lakehouse job IS one table. raw_row_count is COUNT(*) of the "
            "table read (stg_<table> for staging; the warehouse table itself, pre-load, for "
            "dim/fact/agg upserts -- NULL where not meaningful, e.g. dim_date's seed step). "
            "gold_row_count is COUNT(*) of the warehouse target table immediately after the load."
        ),
        "source_file": None,
    },
}

# Dependency order: every dimension before any fact, and every fact before any other
# fact that references it (e.g. fact_admission before fact_diagnosis; fact_provider_order
# before fact_pharmacy_order/fact_medication_administration; fact_pharmacy_order before
# fact_prescription/fact_medication_administration_mini_detail; fact_icu_stay_accumulating
# before the ICU event facts).
WAREHOUSE_TABLE_ORDER: list[str] = [
    # dimensions
    "dim_hcpcs",
    "dim_diagnosis",
    "dim_procedure",
    "dim_lab_item",
    "dim_patient",
    "dim_provider",
    "dim_caregiver",
    "dim_chart_item",
    "dim_date",
    # hosp facts
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
    # icu facts
    "fact_icu_stay_accumulating",
    "fact_chart_observation",
    "fact_datetime_event",
    "fact_input_event",
    "fact_ingredient_event",
    "fact_output_event",
    "fact_procedure_event",
    # aggregates -- depend on facts above being loaded (agg_admission_monthly further
    # depends on agg_admission_daily being refreshed first, see etl_warehouse.py)
    "agg_admission_daily",
    "agg_icu_fluid_balance_daily",
    "agg_admission_monthly",
    # process log -- no FK dependents, order doesn't matter, listed last
    "etl_process_log",
]

assert set(WAREHOUSE_TABLE_ORDER) == set(WAREHOUSE_TABLES), "WAREHOUSE_TABLE_ORDER must list every table exactly once"


def _quote_list(names: tuple[str, ...]) -> str:
    return ", ".join(f'"{n}"' for n in names)


def _create_table_sql(name: str, table_def: dict) -> str:
    pk_cols = set(table_def["primary_key"])
    lines = []
    for col_name, col_type in table_def["columns"]:
        not_null = " NOT NULL" if col_name in pk_cols and "GENERATED" not in col_type else ""
        lines.append(f'    "{col_name}" {col_type}{not_null}')
    lines.append(f'    PRIMARY KEY ({_quote_list(table_def["primary_key"])})')
    for unique_cols in table_def.get("unique_keys", []):
        lines.append(f"    UNIQUE ({_quote_list(unique_cols)})")
    for local_cols, ref_table, ref_cols in table_def["foreign_keys"]:
        lines.append(
            f"    FOREIGN KEY ({_quote_list(local_cols)}) "
            f'REFERENCES {WAREHOUSE_SCHEMA}."{ref_table}" ({_quote_list(ref_cols)})'
        )
    body = ",\n".join(lines)
    return f'CREATE TABLE IF NOT EXISTS {WAREHOUSE_SCHEMA}."{name}" (\n{body}\n);'


def _comment_sql(name: str, table_def: dict) -> str | None:
    comment = table_def.get("comment")
    if not comment:
        return None
    escaped = comment.replace("'", "''")
    return f'COMMENT ON TABLE {WAREHOUSE_SCHEMA}."{name}" IS \'{escaped}\';'


def _unique_expr_index_sql_list(name: str, table_def: dict) -> list[str]:
    """CREATE UNIQUE INDEX statements for `unique_key_exprs` entries -- see the
    WAREHOUSE_TABLES key comment for why these exist alongside plain `unique_keys`."""
    out = []
    for index_name, exprs in table_def.get("unique_key_exprs", []):
        expr_list = ", ".join(exprs)
        out.append(
            f'CREATE UNIQUE INDEX IF NOT EXISTS "{index_name}" '
            f'ON {WAREHOUSE_SCHEMA}."{name}" ({expr_list});'
        )
    return out


def render_ddl() -> str:
    """Render the full warehouse DDL (schema + all dim/fact tables, in dependency
    order, with table comments) as plain SQL text. Single source of truth for
    sql/warehouse_tables.sql -- regenerate with scripts/export_warehouse_ddl.py."""
    statements = [
        "-- Auto-generated from mimic_datawarehouse.warehouse.WAREHOUSE_TABLES.",
        "-- Regenerate with: python scripts/export_warehouse_ddl.py",
        "-- Do not edit by hand -- edit warehouse.py and regenerate instead.",
        "-- Translated from the lakehouse Gold DDL (mimic_iv_ddl_gold_combined_v4.sql).",
        "",
        f'CREATE SCHEMA IF NOT EXISTS "{WAREHOUSE_SCHEMA}";',
        "",
    ]
    for name in WAREHOUSE_TABLE_ORDER:
        table_def = WAREHOUSE_TABLES[name]
        source_file = table_def.get("source_file")
        if source_file:
            statements.append(f"-- Source file: {source_file}")
        statements.append(_create_table_sql(name, table_def))
        comment_sql = _comment_sql(name, table_def)
        if comment_sql:
            statements.append(comment_sql)
        statements.extend(_unique_expr_index_sql_list(name, table_def))
        statements.append("")
    return "\n".join(statements)


def create_warehouse_tables(engine: Engine) -> list[str]:
    """Create the `warehouse` schema and every dim_/fact_ table, in dependency order.
    Returns the list of table names created."""
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{WAREHOUSE_SCHEMA}"'))
        for name in WAREHOUSE_TABLE_ORDER:
            table_def = WAREHOUSE_TABLES[name]
            conn.execute(text(_create_table_sql(name, table_def)))
            comment_sql = _comment_sql(name, table_def)
            if comment_sql:
                conn.execute(text(comment_sql))
            for stmt in _unique_expr_index_sql_list(name, table_def):
                conn.execute(text(stmt))
    return list(WAREHOUSE_TABLE_ORDER)
