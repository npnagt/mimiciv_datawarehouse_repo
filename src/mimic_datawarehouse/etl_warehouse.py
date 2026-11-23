"""ETL: load `warehouse` dims/facts from the `staging` tables.

Column mappings, renames, role-playing date_key derivations, and calculated-column
formulas below are all taken from the source-to-target mapping workbook
(mimic_iv_raw_to_gold_mapping_v2.xlsx, "Column Mapping" sheet) rather than re-derived --
that workbook is the single source of truth for *how* each warehouse column is populated.

Two different incremental strategies, per the requested behavior:
  - Dimensions: upserted (INSERT ... ON CONFLICT (natural_key) DO UPDATE) -- if the
    business key already exists, its attributes are refreshed in place (SCD Type 1).
  - Facts: insert-only (INSERT ... ON CONFLICT ... DO NOTHING / a NOT EXISTS anti-join
    for the handful with no natural key) -- an existing fact row is never modified or
    re-inserted; only rows not yet present are added. Calculated columns that need
    cross-row context (is_readmission_flag, days_since_prior_discharge,
    is_icu_readmission_flag) are computed as SQL window functions over the *entire*
    staging table -- correct because staging is always a full truncate-and-reload
    snapshot (see etl.py), not an incremental extract -- and only the resulting
    not-yet-in-the-fact-table rows are then inserted.

date_key columns are clamped to the seeded dim_date range (see seed_date_dim.py): a
timestamp outside [SEED_START_DATE, SEED_END_DATE] gets a NULL date_key rather than
failing the whole row's FK -- a handful of real rows have far-out-of-range timestamps
(e.g. 3 emar.scheduletime values before 2100), and losing one role-playing date on an
otherwise-valid record is preferable to losing the record entirely.
"""

from __future__ import annotations

from sqlalchemy import Engine, text

from .seed_date_dim import SEED_END_DATE, SEED_START_DATE
from .warehouse import WAREHOUSE_SCHEMA, WAREHOUSE_TABLES

ETL_JOB_NAME = "etl_load_warehouse"
_START = SEED_START_DATE.isoformat()
_END = SEED_END_DATE.isoformat()


def _date_key(col: str) -> str:
    """SQL expression: YYYYMMDD integer for `col` (a timestamp/date expression),
    NULL if `col` is NULL or falls outside the seeded dim_date range."""
    return (
        f"(CASE WHEN {col} IS NULL OR {col}::date < DATE '{_START}' OR {col}::date > DATE '{_END}' "
        f"THEN NULL ELSE (to_char({col}, 'YYYYMMDD'))::integer END)"
    )


def _minutes_between(start_col: str, end_col: str) -> str:
    return f"(EXTRACT(EPOCH FROM ({end_col} - {start_col})) / 60.0)"


def _hours_between(start_col: str, end_col: str) -> str:
    return f"(EXTRACT(EPOCH FROM ({end_col} - {start_col})) / 3600.0)"


# ======================================================================================
# DIMENSIONS -- generic upsert. column_map: ordered [(warehouse_col, staging_expr), ...],
# staging_expr referencing the staging row as `s`. natural_key comes straight from
# warehouse.WAREHOUSE_TABLES (every dimension's PK *is* its business key).
# ======================================================================================

DIMENSION_COLUMN_MAP: dict[str, tuple[str, list[tuple[str, str]]]] = {
    # warehouse_table -> (staging_table, [(warehouse_col, staging_expr), ...])
    "dim_hcpcs": (
        "stg_d_hcpcs",
        [
            ("code", "s.code"),
            ("category", "s.category::bigint"),
            ("long_description", "s.long_description"),
            ("short_description", "s.short_description"),
        ],
    ),
    "dim_diagnosis": (
        "stg_d_icd_diagnoses",
        [
            ("icd_code", "s.icd_code"),
            ("icd_version", "s.icd_version"),
            ("long_title", "s.long_title"),
        ],
    ),
    "dim_procedure": (
        "stg_d_icd_procedures",
        [
            ("icd_code", "s.icd_code"),
            ("icd_version", "s.icd_version"),
            ("long_title", "s.long_title"),
        ],
    ),
    "dim_lab_item": (
        "stg_d_labitems",
        [
            ("item_id", "s.itemid"),
            ("label", "s.label"),
            ("fluid", "s.fluid"),
            ("category", "s.category"),
        ],
    ),
    "dim_patient": (
        "stg_patients",
        [
            ("subject_id", "s.subject_id"),
            ("gender", "s.gender"),
            ("anchor_age", "s.anchor_age"),
            ("anchor_year", "s.anchor_year"),
            ("anchor_year_group", "s.anchor_year_group"),
            ("dod", "s.dod::date"),
        ],
    ),
    "dim_provider": ("stg_provider", [("provider_id", "s.provider_id")]),
    "dim_caregiver": ("stg_caregiver", [("caregiver_id", "s.caregiver_id")]),
    "dim_chart_item": (
        "stg_d_items",
        [
            ("item_id", "s.itemid"),
            ("label", "s.label"),
            ("abbreviation", "s.abbreviation"),
            ("links_to", "s.linksto"),
            ("category", "s.category"),
            ("unit_name", "s.unitname"),
            ("param_type", "s.param_type"),
            ("low_normal_value", "s.lownormalvalue::double precision"),
            ("high_normal_value", "s.highnormalvalue::double precision"),
        ],
    ),
}


def _dimension_upsert_sql(warehouse_table: str) -> str:
    staging_table, column_map = DIMENSION_COLUMN_MAP[warehouse_table]
    natural_key = WAREHOUSE_TABLES[warehouse_table]["primary_key"]

    col_list = ", ".join(f'"{c}"' for c, _ in column_map)
    select_list = ", ".join(f'{expr} AS "{c}"' for c, expr in column_map)
    non_key_cols = [c for c, _ in column_map if c not in natural_key]

    conflict_action = (
        "DO NOTHING"
        if not non_key_cols
        else "DO UPDATE SET "
        + ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in non_key_cols)
        + f", updated_ts = now(), updated_by = '{ETL_JOB_NAME}'"
    )
    conflict_key = ", ".join(f'"{c}"' for c in natural_key)

    return (
        f'INSERT INTO {WAREHOUSE_SCHEMA}."{warehouse_table}" ({col_list}, created_ts, updated_ts, created_by, updated_by)\n'
        f"SELECT {select_list}, now(), now(), '{ETL_JOB_NAME}', '{ETL_JOB_NAME}'\n"
        f'FROM staging."{staging_table}" s\n'
        f"ON CONFLICT ({conflict_key}) {conflict_action};"
    )


def load_dimension(engine: Engine, warehouse_table: str) -> int:
    """Upsert `warehouse_table` from its staging source. Returns rows inserted+updated."""
    with engine.begin() as conn:
        result = conn.execute(text(_dimension_upsert_sql(warehouse_table)))
        return result.rowcount


def load_all_dimensions(engine: Engine) -> dict[str, int]:
    return {name: load_dimension(engine, name) for name in DIMENSION_COLUMN_MAP}


# ======================================================================================
# FACTS -- bespoke SQL per table (calculated columns/joins/window functions are too
# varied to generalize cleanly). Each entry is a list of SQL statements executed in
# order, normally just the INSERT itself. fact_chart_observation is the one exception:
# it's the only anti-join-deduped (surrogate-key) fact whose natural grain, (stay_id,
# item_id, chart_time, store_time), isn't actually unique in the raw data (~12% of
# chartevents.csv.gz rows collide on it) -- see warehouse.py's WAREHOUSE_TABLES comment
# for that table -- so it's the only one that still needs its own supporting
# (non-unique) index here rather than getting one for free from a UNIQUE constraint.
# ======================================================================================

_now4 = f"now(), now(), '{ETL_JOB_NAME}', '{ETL_JOB_NAME}'"

FACT_LOAD_SQL: dict[str, list[str]] = {
    "fact_admission": [
        f"""
WITH src AS (
    SELECT
        s.subject_id, s.hadm_id,
        s.admittime::timestamp AS admit_time,
        s.dischtime::timestamp AS disch_time,
        s.deathtime::timestamp AS death_time,
        s.edregtime::timestamp AS ed_reg_time,
        s.edouttime::timestamp AS ed_out_time,
        s.admission_type, s.admit_provider_id, s.admission_location, s.discharge_location,
        s.insurance, s.language, s.marital_status, s.race, s.hospital_expire_flag,
        dp.anchor_age, dp.anchor_year,
        LAG(s.admittime::timestamp) OVER (PARTITION BY s.subject_id ORDER BY s.admittime::timestamp) AS prior_admit_time,
        LAG(s.dischtime::timestamp) OVER (PARTITION BY s.subject_id ORDER BY s.admittime::timestamp) AS prior_disch_time
    FROM staging.stg_admissions s
    LEFT JOIN warehouse.dim_patient dp ON dp.subject_id = s.subject_id
)
INSERT INTO warehouse.fact_admission (
    subject_id, hadm_id, admit_date_key, disch_date_key, death_date_key, ed_reg_date_key, ed_out_date_key,
    admit_time, disch_time, death_time, admission_type, admit_provider_id, admission_location,
    discharge_location, insurance, language, marital_status, race, ed_reg_time, ed_out_time,
    hospital_expire_flag, hospital_los_hours, ed_los_minutes, time_to_death_hours, age_at_admission,
    is_readmission_flag, days_since_prior_discharge, created_ts, updated_ts, created_by, updated_by
)
SELECT
    subject_id, hadm_id,
    {_date_key("admit_time")}, {_date_key("disch_time")}, {_date_key("death_time")},
    {_date_key("ed_reg_time")}, {_date_key("ed_out_time")},
    admit_time, disch_time, death_time, admission_type, admit_provider_id, admission_location,
    discharge_location, insurance, language, marital_status, race, ed_reg_time, ed_out_time,
    hospital_expire_flag,
    {_hours_between("admit_time", "disch_time")},
    {_minutes_between("ed_reg_time", "ed_out_time")},
    {_hours_between("admit_time", "death_time")},
    (anchor_age + (EXTRACT(YEAR FROM admit_time)::int - anchor_year)),
    (prior_admit_time IS NOT NULL),
    (CASE WHEN prior_disch_time IS NULL THEN NULL ELSE FLOOR(EXTRACT(EPOCH FROM (admit_time - prior_disch_time)) / 86400.0)::int END),
    {_now4}
FROM src
ON CONFLICT (hadm_id) DO NOTHING;
"""
    ],
    "fact_diagnosis": [
        f"""
INSERT INTO warehouse.fact_diagnosis (subject_id, hadm_id, seq_num, icd_code, icd_version, created_ts, updated_ts, created_by, updated_by)
SELECT s.subject_id, s.hadm_id, s.seq_num, s.icd_code, s.icd_version, {_now4}
FROM staging.stg_diagnoses_icd s
ON CONFLICT (hadm_id, seq_num) DO NOTHING;
"""
    ],
    "fact_drg_assignment": [
        f"""
INSERT INTO warehouse.fact_drg_assignment (subject_id, hadm_id, drg_type, drg_code, description, drg_severity, drg_mortality, created_ts, updated_ts, created_by, updated_by)
SELECT s.subject_id, s.hadm_id, s.drg_type, s.drg_code, s.description, s.drg_severity, s.drg_mortality, {_now4}
FROM staging.stg_drgcodes s
ON CONFLICT (hadm_id, drg_type, drg_code) DO NOTHING;
"""
    ],
    "fact_provider_order": [
        f"""
WITH src AS (
    SELECT s.*, s.ordertime::timestamp AS order_time_ts
    FROM staging.stg_poe s
)
INSERT INTO warehouse.fact_provider_order (
    poe_id, poe_seq, subject_id, hadm_id, order_date_key, order_time, order_type, order_subtype,
    transaction_type, discontinue_of_poe_id, discontinued_by_poe_id, order_provider_id, order_status,
    created_ts, updated_ts, created_by, updated_by
)
SELECT
    poe_id, poe_seq, subject_id, hadm_id, {_date_key("order_time_ts")}, order_time_ts, order_type, order_subtype,
    transaction_type, discontinue_of_poe_id, discontinued_by_poe_id, order_provider_id, order_status,
    {_now4}
FROM src
ON CONFLICT (poe_id) DO NOTHING;
"""
    ],
    "fact_pharmacy_order": [
        f"""
WITH src AS (
    SELECT
        s.*,
        s.starttime::timestamp AS start_time_ts, s.stoptime::timestamp AS stop_time_ts,
        s.entertime::timestamp AS enter_time_ts, s.verifiedtime::timestamp AS verified_time_ts,
        fpo.poe_id AS resolved_poe_id
    FROM staging.stg_pharmacy s
    -- LEFT JOIN, not a plain column reference: ~0.1% of pharmacy rows reference a poe_id
    -- that doesn't exist in poe.csv (a real MIMIC-IV data-completeness gap, not an ETL
    -- bug -- verified against the raw data). Resolving through the join means an
    -- unmatched poe_id becomes NULL here instead of the whole row failing its FK.
    LEFT JOIN warehouse.fact_provider_order fpo ON fpo.poe_id = s.poe_id
)
INSERT INTO warehouse.fact_pharmacy_order (
    subject_id, hadm_id, pharmacy_id, poe_id, start_date_key, stop_date_key, enter_date_key, verified_date_key,
    start_time, stop_time, medication, proc_type, status, enter_time, verified_time, route, frequency,
    disp_sched, infusion_type, sliding_scale, lockout_interval, basal_rate, one_hr_max, doses_per_24_hrs,
    duration, duration_interval, expiration_value, expiration_unit, expiration_date, dispensation, fill_quantity,
    created_ts, updated_ts, created_by, updated_by
)
SELECT
    subject_id, hadm_id, pharmacy_id, resolved_poe_id,
    {_date_key("start_time_ts")}, {_date_key("stop_time_ts")}, {_date_key("enter_time_ts")}, {_date_key("verified_time_ts")},
    start_time_ts, stop_time_ts, medication, proc_type, status, enter_time_ts, verified_time_ts, route, frequency,
    disp_sched, infusion_type, sliding_scale,
    lockout_interval::double precision, basal_rate::double precision, one_hr_max::double precision,
    doses_per_24_hrs, duration, duration_interval, expiration_value, expiration_unit, expirationdate::date,
    dispensation, fill_quantity::double precision,
    {_now4}
FROM src
ON CONFLICT (pharmacy_id) DO NOTHING;
"""
    ],
    "fact_medication_administration": [
        f"""
WITH src AS (
    SELECT
        s.*, s.charttime::timestamp AS chart_time_ts, s.scheduletime::timestamp AS schedule_time_ts,
        s.storetime::timestamp AS store_time_ts,
        fpo.poe_id AS resolved_poe_id, fpho.pharmacy_id AS resolved_pharmacy_id
    FROM staging.stg_emar s
    -- LEFT JOINs, not plain column references: ~3% of emar rows reference a poe_id and
    -- ~2% a pharmacy_id that doesn't exist in poe.csv/pharmacy.csv (a real MIMIC-IV
    -- data-completeness gap, verified against the raw data, not an ETL bug). Resolving
    -- through the join means an unmatched reference becomes NULL here instead of the
    -- whole row failing its FK.
    LEFT JOIN warehouse.fact_provider_order fpo ON fpo.poe_id = s.poe_id
    LEFT JOIN warehouse.fact_pharmacy_order fpho ON fpho.pharmacy_id = s.pharmacy_id
)
INSERT INTO warehouse.fact_medication_administration (
    subject_id, hadm_id, emar_id, emar_seq, poe_id, pharmacy_id, enter_provider_id,
    chart_date_key, schedule_date_key, store_date_key, chart_time, medication, event_txt,
    schedule_time, store_time, admin_delay_minutes, created_ts, updated_ts, created_by, updated_by
)
SELECT
    subject_id, hadm_id, emar_id, emar_seq, resolved_poe_id, resolved_pharmacy_id, enter_provider_id,
    {_date_key("chart_time_ts")}, {_date_key("schedule_time_ts")}, {_date_key("store_time_ts")},
    chart_time_ts, medication, event_txt, schedule_time_ts, store_time_ts,
    {_minutes_between("schedule_time_ts", "chart_time_ts")},
    {_now4}
FROM src
ON CONFLICT (emar_id) DO NOTHING;
"""
    ],
    "fact_medication_administration_mini_detail": [
        # ON CONFLICT against warehouse.py's ux_med_admin_mini_detail_grain expression
        # index (COALESCE(emar_id,''), COALESCE(emar_seq,-1), COALESCE(parent_field_
        # ordinal,-1)) -- a NULL-safe unique index, not a plain UNIQUE constraint, so a
        # NULL parent_field_ordinal (true for ~half of real rows) still collides
        # correctly. This replaces an earlier WHERE NOT EXISTS anti-join + DISTINCT ON
        # dedup: the anti-join only guarded against rows already committed to the
        # target, so two source rows sharing a grain (a real occurrence -- verified 64
        # exact-duplicate pairs in emar_detail.csv.gz at full-dataset scale) collided
        # with each other during the INSERT itself. ON CONFLICT checks the index
        # row-by-row as rows are inserted, so same-batch duplicates are handled for
        # free -- no separate dedup step, no sort, no extra work_mem needed.
        f"""
INSERT INTO warehouse.fact_medication_administration_mini_detail (
    subject_id, emar_id, emar_seq, parent_field_ordinal, administration_type, pharmacy_id, barcode_type,
    reason_for_no_barcode, complete_dose_not_given, dose_due, dose_due_unit, dose_given, dose_given_unit,
    will_remainder_of_dose_be_given, product_amount_given, product_unit, product_code, product_description,
    product_description_other, prior_infusion_rate, infusion_rate, infusion_rate_adjustment,
    infusion_rate_adjustment_amount, infusion_rate_unit, route, infusion_complete, completion_interval,
    new_iv_bag_hung, continued_infusion_in_other_location, restart_interval, side, site,
    non_formulary_visual_verification, created_ts, updated_ts, created_by, updated_by
)
SELECT
    s.subject_id, s.emar_id, s.emar_seq, s.parent_field_ordinal, s.administration_type, fpho.pharmacy_id, s.barcode_type,
    s.reason_for_no_barcode, s.complete_dose_not_given, s.dose_due, s.dose_due_unit, s.dose_given, s.dose_given_unit,
    s.will_remainder_of_dose_be_given, s.product_amount_given, s.product_unit, s.product_code, s.product_description,
    s.product_description_other, s.prior_infusion_rate, s.infusion_rate, s.infusion_rate_adjustment,
    s.infusion_rate_adjustment_amount::double precision, s.infusion_rate_unit, s.route, s.infusion_complete,
    s.completion_interval, s.new_iv_bag_hung, s.continued_infusion_in_other_location, s.restart_interval, s.side,
    s.site, s.non_formulary_visual_verification, {_now4}
FROM staging.stg_emar_detail s
-- LEFT JOIN, not a plain column reference: ~2% of emar_detail rows reference a
-- pharmacy_id that doesn't exist in pharmacy.csv (a real MIMIC-IV data-completeness
-- gap, verified against the raw data, not an ETL bug).
LEFT JOIN warehouse.fact_pharmacy_order fpho ON fpho.pharmacy_id = s.pharmacy_id
ON CONFLICT (
    (COALESCE(emar_id, '')), (COALESCE(emar_seq, -1)), (COALESCE(parent_field_ordinal, -1))
) DO NOTHING;
""",
    ],
    "fact_hcpcs_event": [
        f"""
WITH src AS (
    SELECT s.*, s.chartdate::date AS chart_date_d
    FROM staging.stg_hcpcsevents s
)
INSERT INTO warehouse.fact_hcpcs_event (
    subject_id, hadm_id, chart_date_key, chart_date, hcpcs_cd, seq_num, short_description,
    created_ts, updated_ts, created_by, updated_by
)
SELECT subject_id, hadm_id, {_date_key("chart_date_d")}, chart_date_d, hcpcs_cd, seq_num, short_description, {_now4}
FROM src
ON CONFLICT (hadm_id, seq_num) DO NOTHING;
"""
    ],
    "fact_lab_result": [
        f"""
WITH src AS (
    SELECT s.*, s.charttime::timestamp AS chart_time_ts, s.storetime::timestamp AS store_time_ts
    FROM staging.stg_labevents s
)
INSERT INTO warehouse.fact_lab_result (
    labevent_id, subject_id, hadm_id, specimen_id, item_id, order_provider_id,
    chart_date_key, store_date_key, chart_time, store_time, value, value_num, value_uom,
    ref_range_lower, ref_range_upper, flag, priority, comments,
    result_turnaround_minutes, is_abnormal_flag, created_ts, updated_ts, created_by, updated_by
)
SELECT
    labevent_id, subject_id, hadm_id, specimen_id, itemid, order_provider_id,
    {_date_key("chart_time_ts")}, {_date_key("store_time_ts")}, chart_time_ts, store_time_ts,
    value, valuenum, valueuom, ref_range_lower, ref_range_upper, flag, priority, comments,
    {_minutes_between("chart_time_ts", "store_time_ts")},
    (CASE WHEN valuenum IS NULL OR ref_range_lower IS NULL OR ref_range_upper IS NULL THEN NULL
          WHEN valuenum < ref_range_lower OR valuenum > ref_range_upper THEN TRUE ELSE FALSE END),
    {_now4}
FROM src
ON CONFLICT (labevent_id) DO NOTHING;
"""
    ],
    "fact_microbiology_result": [
        f"""
WITH src AS (
    SELECT s.*, s.chartdate::timestamp AS chart_date_ts, s.charttime::timestamp AS chart_time_ts,
           s.storedate::timestamp AS store_date_ts, s.storetime::timestamp AS store_time_ts
    FROM staging.stg_microbiologyevents s
)
INSERT INTO warehouse.fact_microbiology_result (
    microevent_id, subject_id, hadm_id, micro_specimen_id, order_provider_id,
    chart_date_key, store_date_key, chart_date, chart_time, spec_itemid, spec_type_desc, test_seq,
    store_date, store_time, test_itemid, test_name, org_itemid, org_name, isolate_num, quantity,
    ab_itemid, ab_name, dilution_text, dilution_comparison, dilution_value, interpretation, comments,
    result_turnaround_minutes, is_positive_culture_flag, created_ts, updated_ts, created_by, updated_by
)
SELECT
    microevent_id, subject_id, hadm_id, micro_specimen_id, order_provider_id,
    {_date_key("chart_time_ts")}, {_date_key("store_time_ts")}, chart_date_ts, chart_time_ts,
    spec_itemid, spec_type_desc, test_seq, store_date_ts, store_time_ts, test_itemid, test_name,
    org_itemid, org_name, isolate_num, quantity::double precision, ab_itemid, ab_name, dilution_text,
    dilution_comparison, dilution_value, interpretation, comments,
    {_minutes_between("chart_time_ts", "store_time_ts")},
    (CASE WHEN org_name IS NOT NULL AND org_name <> '' THEN TRUE ELSE FALSE END),
    {_now4}
FROM src
ON CONFLICT (microevent_id) DO NOTHING;
"""
    ],
    "fact_outpatient_measurement": [
        f"""
WITH src AS (
    SELECT s.*, s.chartdate::date AS chart_date_d
    FROM staging.stg_omr s
)
INSERT INTO warehouse.fact_outpatient_measurement (
    subject_id, chart_date_key, chart_date, seq_num, result_name, result_value,
    created_ts, updated_ts, created_by, updated_by
)
SELECT subject_id, {_date_key("chart_date_d")}, chart_date_d, seq_num, result_name, result_value, {_now4}
FROM src
ON CONFLICT (subject_id, chart_date, seq_num, result_name) DO NOTHING;
"""
    ],
    "fact_provider_order_mini_detail": [
        # INNER JOIN, not a plain column reference: poe_id is NOT NULL + FK on this
        # table, so a poe_id absent from fact_provider_order can't be nulled out the way
        # pharmacy_id/poe_id are elsewhere (those columns are nullable) -- it has to be
        # dropped instead. At the current --row-cap this isn't a small edge case:
        # stg_poe and stg_poe_detail are capped independently (1,000,000 rows each by
        # raw file order), so their poe_id sets only partially overlap -- verified
        # 840,769 of 1,000,000 (84%) stg_poe_detail rows unresolved against
        # fact_provider_order, far above the ~2-3% real MIMIC-IV completeness gaps
        # documented for the nullable-FK tables.
        f"""
INSERT INTO warehouse.fact_provider_order_mini_detail (poe_id, poe_seq, subject_id, field_name, field_value, created_ts, updated_ts, created_by, updated_by)
SELECT s.poe_id, s.poe_seq, s.subject_id, s.field_name, s.field_value, {_now4}
FROM staging.stg_poe_detail s
JOIN warehouse.fact_provider_order fpo ON fpo.poe_id = s.poe_id
ON CONFLICT (poe_id, field_name) DO NOTHING;
"""
    ],
    "fact_prescription": [
        # ON CONFLICT against warehouse.py's ux_fact_prescription_grain expression index
        # (COALESCE'd subject_id/hadm_id/pharmacy_id/poe_id/poe_seq/start_time/drug) --
        # a NULL-safe unique index, not a plain UNIQUE constraint, so a NULL pharmacy_id/
        # poe_id (a real fraction of rows are unresolved against pharmacy.csv/poe.csv)
        # still collides correctly. Replaces an earlier WHERE NOT EXISTS anti-join +
        # DISTINCT ON dedup: the anti-join only guarded against rows already committed
        # to the target, so two source rows sharing a grain (verified 66 duplicate
        # groups in prescriptions.csv.gz at full-dataset scale) collided with each other
        # during the INSERT itself. ON CONFLICT checks the index row-by-row as rows are
        # inserted, so same-batch duplicates are handled for free.
        f"""
WITH src AS (
    SELECT
        s.*, s.starttime::timestamp AS start_time_ts, s.stoptime::timestamp AS stop_time_ts,
        fpho.pharmacy_id AS resolved_pharmacy_id, fpo.poe_id AS resolved_poe_id
    FROM staging.stg_prescriptions s
    -- LEFT JOINs, not plain column references: a small fraction of prescription rows
    -- reference a poe_id that doesn't exist in poe.csv (a real MIMIC-IV
    -- data-completeness gap, verified against the raw data, not an ETL bug).
    LEFT JOIN warehouse.fact_pharmacy_order fpho ON fpho.pharmacy_id = s.pharmacy_id
    LEFT JOIN warehouse.fact_provider_order fpo ON fpo.poe_id = s.poe_id
)
INSERT INTO warehouse.fact_prescription (
    subject_id, hadm_id, pharmacy_id, poe_id, poe_seq, order_provider_id, start_date_key, stop_date_key,
    start_time, stop_time, drug_type, drug, formulary_drug_cd, gsn, ndc, prod_strength, form_rx,
    dose_val_rx, dose_unit_rx, form_val_disp, form_unit_disp, doses_per_24_hrs, route,
    created_ts, updated_ts, created_by, updated_by
)
SELECT
    subject_id, hadm_id, resolved_pharmacy_id, resolved_poe_id, poe_seq, order_provider_id,
    {_date_key("start_time_ts")}, {_date_key("stop_time_ts")}, start_time_ts, stop_time_ts,
    drug_type, drug, formulary_drug_cd, gsn, ndc, prod_strength, form_rx, dose_val_rx, dose_unit_rx,
    form_val_disp, form_unit_disp, doses_per_24_hrs, route, {_now4}
FROM src s2
ON CONFLICT (
    (COALESCE(subject_id, -1)), (COALESCE(hadm_id, -1)), (COALESCE(pharmacy_id, -1)),
    (COALESCE(poe_id, '')), (COALESCE(poe_seq, -1)), (COALESCE(start_time, TIMESTAMP 'epoch')),
    (COALESCE(drug, ''))
) DO NOTHING;
""",
    ],
    "fact_procedure": [
        f"""
WITH src AS (
    SELECT s.*, s.chartdate::date AS chart_date_d
    FROM staging.stg_procedures_icd s
)
INSERT INTO warehouse.fact_procedure (
    subject_id, hadm_id, seq_num, chart_date_key, chart_date, icd_code, icd_version,
    created_ts, updated_ts, created_by, updated_by
)
SELECT subject_id, hadm_id, seq_num, {_date_key("chart_date_d")}, chart_date_d, icd_code, icd_version, {_now4}
FROM src
ON CONFLICT (hadm_id, seq_num) DO NOTHING;
"""
    ],
    "fact_service_assignment": [
        f"""
WITH src AS (
    SELECT s.*, s.transfertime::timestamp AS transfer_time_ts
    FROM staging.stg_services s
)
INSERT INTO warehouse.fact_service_assignment (
    subject_id, hadm_id, transfer_date_key, transfer_time, prev_service, curr_service,
    created_ts, updated_ts, created_by, updated_by
)
SELECT subject_id, hadm_id, {_date_key("transfer_time_ts")}, transfer_time_ts, prev_service, curr_service, {_now4}
FROM src
ON CONFLICT (hadm_id, transfer_time) DO NOTHING;
"""
    ],
    "fact_transfer": [
        f"""
WITH src AS (
    SELECT s.*, s.intime::timestamp AS in_time_ts, s.outtime::timestamp AS out_time_ts
    FROM staging.stg_transfers s
)
INSERT INTO warehouse.fact_transfer (
    subject_id, hadm_id, transfer_id, event_type, care_unit, in_date_key, out_date_key,
    in_time, out_time, transfer_duration_hours, created_ts, updated_ts, created_by, updated_by
)
SELECT
    subject_id, hadm_id, transfer_id, eventtype, careunit,
    {_date_key("in_time_ts")}, {_date_key("out_time_ts")}, in_time_ts, out_time_ts,
    {_hours_between("in_time_ts", "out_time_ts")},
    {_now4}
FROM src
ON CONFLICT (transfer_id) DO NOTHING;
"""
    ],
    "fact_icu_stay_accumulating": [
        f"""
WITH src AS (
    SELECT
        s.*, s.intime::timestamp AS in_time_ts, s.outtime::timestamp AS out_time_ts,
        fa.admit_time AS admit_time,
        LAG(s.intime::timestamp) OVER (PARTITION BY s.subject_id ORDER BY s.intime::timestamp) AS prior_in_time
    FROM staging.stg_icustays s
    LEFT JOIN warehouse.fact_admission fa ON fa.hadm_id = s.hadm_id
)
INSERT INTO warehouse.fact_icu_stay_accumulating (
    subject_id, hadm_id, stay_id, first_careunit, last_careunit, in_date_key, out_date_key,
    in_time, out_time, los, time_to_icu_hours, is_icu_readmission_flag,
    created_ts, updated_ts, created_by, updated_by
)
SELECT
    subject_id, hadm_id, stay_id, first_careunit, last_careunit,
    {_date_key("in_time_ts")}, {_date_key("out_time_ts")}, in_time_ts, out_time_ts, los,
    {_hours_between("admit_time", "in_time_ts")},
    (prior_in_time IS NOT NULL),
    {_now4}
FROM src
ON CONFLICT (stay_id) DO NOTHING;
"""
    ],
    "fact_chart_observation": [
        """
CREATE INDEX IF NOT EXISTS ix_fchartobs_dedup ON warehouse.fact_chart_observation
    (stay_id, item_id, chart_time, store_time);
""",
        f"""
WITH src AS (
    SELECT s.*, s.charttime::timestamp AS chart_time_ts, s.storetime::timestamp AS store_time_ts
    FROM staging.stg_chartevents s
)
INSERT INTO warehouse.fact_chart_observation (
    subject_id, hadm_id, stay_id, caregiver_id, chart_date_key, store_date_key, chart_time, store_time,
    item_id, value, value_num, value_uom, warning, charting_delay_minutes, is_abnormal_flag,
    created_ts, updated_ts, created_by, updated_by
)
SELECT
    s.subject_id, s.hadm_id, s.stay_id, s.caregiver_id,
    {_date_key("s.chart_time_ts")}, {_date_key("s.store_time_ts")}, s.chart_time_ts, s.store_time_ts,
    s.itemid, s.value, s.valuenum, s.valueuom, s.warning,
    {_minutes_between("s.chart_time_ts", "s.store_time_ts")},
    (CASE WHEN s.valuenum IS NULL OR dci.low_normal_value IS NULL OR dci.high_normal_value IS NULL THEN NULL
          WHEN s.valuenum < dci.low_normal_value OR s.valuenum > dci.high_normal_value THEN TRUE ELSE FALSE END),
    {_now4}
FROM src s
LEFT JOIN warehouse.dim_chart_item dci ON dci.item_id = s.itemid
WHERE NOT EXISTS (
    SELECT 1 FROM warehouse.fact_chart_observation w
    WHERE w.stay_id IS NOT DISTINCT FROM s.stay_id
      AND w.item_id IS NOT DISTINCT FROM s.itemid
      AND w.chart_time IS NOT DISTINCT FROM s.chart_time_ts
      AND w.store_time IS NOT DISTINCT FROM s.store_time_ts
);
""",
    ],
    "fact_datetime_event": [
        # ON CONFLICT against warehouse.py's ux_fact_datetime_event_grain expression
        # index (COALESCE'd stay_id/item_id/chart_time/store_time) -- a NULL-safe
        # unique index, not a plain UNIQUE constraint, so a NULL store_time still
        # collides correctly. Replaces an earlier WHERE NOT EXISTS anti-join + DISTINCT
        # ON dedup (see fact_medication_administration_mini_detail for why that
        # combination was both slower -- the DISTINCT ON forced a disk-spilled sort at
        # this instance's 4MB work_mem -- and incomplete: the anti-join only guarded
        # against rows already committed to the target, not same-batch duplicates).
        f"""
WITH src AS (
    SELECT s.*, s.charttime::timestamp AS chart_time_ts, s.storetime::timestamp AS store_time_ts
    FROM staging.stg_datetimeevents s
)
INSERT INTO warehouse.fact_datetime_event (
    subject_id, hadm_id, stay_id, caregiver_id, chart_date_key, store_date_key, chart_time, store_time,
    item_id, value, value_uom, warning, charting_delay_minutes, created_ts, updated_ts, created_by, updated_by
)
SELECT
    s.subject_id, s.hadm_id, s.stay_id, s.caregiver_id,
    {_date_key("s.chart_time_ts")}, {_date_key("s.store_time_ts")}, s.chart_time_ts, s.store_time_ts,
    s.itemid, s.value, s.valueuom, s.warning,
    {_minutes_between("s.chart_time_ts", "s.store_time_ts")},
    {_now4}
FROM src s
ON CONFLICT (
    (COALESCE(stay_id, -1)), (COALESCE(item_id, -1)),
    (COALESCE(chart_time, TIMESTAMP 'epoch')), (COALESCE(store_time, TIMESTAMP 'epoch'))
) DO NOTHING;
""",
    ],
    "fact_input_event": [
        # ON CONFLICT against warehouse.py's ux_fact_input_event_grain expression index
        # -- see fact_datetime_event for why this replaces an anti-join + DISTINCT ON.
        f"""
WITH src AS (
    SELECT s.*, s.starttime::timestamp AS start_time_ts, s.endtime::timestamp AS end_time_ts,
           s.storetime::timestamp AS store_time_ts
    FROM staging.stg_inputevents s
)
INSERT INTO warehouse.fact_input_event (
    subject_id, hadm_id, stay_id, caregiver_id, start_date_key, end_date_key, store_date_key,
    start_time, end_time, store_time, duration_minutes, item_id, amount, amount_uom, rate, rate_uom,
    order_id, link_order_id, order_category_name, secondary_order_category_name,
    order_component_type_description, order_category_description, patient_weight, total_amount,
    total_amount_uom, is_open_bag, continue_in_next_dept, status_description, original_amount, original_rate,
    created_ts, updated_ts, created_by, updated_by
)
SELECT
    s.subject_id, s.hadm_id, s.stay_id, s.caregiver_id,
    {_date_key("s.start_time_ts")}, {_date_key("s.end_time_ts")}, {_date_key("s.store_time_ts")},
    s.start_time_ts, s.end_time_ts, s.store_time_ts,
    {_minutes_between("s.start_time_ts", "s.end_time_ts")},
    s.itemid, s.amount, s.amountuom, s.rate, s.rateuom, s.orderid, s.linkorderid,
    s.ordercategoryname, s.secondaryordercategoryname, s.ordercomponenttypedescription,
    s.ordercategorydescription, s.patientweight, s.totalamount, s.totalamountuom,
    s.isopenbag, s.continueinnextdept, s.statusdescription, s.originalamount, s.originalrate,
    {_now4}
FROM src s
ON CONFLICT (
    (COALESCE(stay_id, -1)), (COALESCE(item_id, -1)),
    (COALESCE(start_time, TIMESTAMP 'epoch')), (COALESCE(end_time, TIMESTAMP 'epoch')),
    (COALESCE(order_id, -1))
) DO NOTHING;
""",
    ],
    "fact_ingredient_event": [
        # ON CONFLICT against warehouse.py's ux_fact_ingredient_event_grain expression
        # index -- see fact_datetime_event for why this replaces an anti-join + DISTINCT ON.
        f"""
WITH src AS (
    SELECT s.*, s.starttime::timestamp AS start_time_ts, s.endtime::timestamp AS end_time_ts,
           s.storetime::timestamp AS store_time_ts
    FROM staging.stg_ingredientevents s
)
INSERT INTO warehouse.fact_ingredient_event (
    subject_id, hadm_id, stay_id, caregiver_id, start_date_key, end_date_key, store_date_key,
    start_time, end_time, store_time, duration_minutes, item_id, amount, amount_uom, rate, rate_uom,
    order_id, link_order_id, status_description, original_amount, original_rate,
    created_ts, updated_ts, created_by, updated_by
)
SELECT
    s.subject_id, s.hadm_id, s.stay_id, s.caregiver_id,
    {_date_key("s.start_time_ts")}, {_date_key("s.end_time_ts")}, {_date_key("s.store_time_ts")},
    s.start_time_ts, s.end_time_ts, s.store_time_ts,
    {_minutes_between("s.start_time_ts", "s.end_time_ts")},
    s.itemid, s.amount, s.amountuom, s.rate, s.rateuom, s.orderid, s.linkorderid,
    s.statusdescription, s.originalamount, s.originalrate,
    {_now4}
FROM src s
ON CONFLICT (
    (COALESCE(stay_id, -1)), (COALESCE(item_id, -1)),
    (COALESCE(start_time, TIMESTAMP 'epoch')), (COALESCE(end_time, TIMESTAMP 'epoch')),
    (COALESCE(order_id, -1))
) DO NOTHING;
""",
    ],
    "fact_output_event": [
        # ON CONFLICT against warehouse.py's ux_fact_output_event_grain expression
        # index -- see fact_datetime_event for why this replaces an anti-join + DISTINCT ON.
        f"""
WITH src AS (
    SELECT s.*, s.charttime::timestamp AS chart_time_ts, s.storetime::timestamp AS store_time_ts
    FROM staging.stg_outputevents s
)
INSERT INTO warehouse.fact_output_event (
    subject_id, hadm_id, stay_id, caregiver_id, chart_date_key, store_date_key, chart_time, store_time,
    item_id, value, value_uom, charting_delay_minutes, created_ts, updated_ts, created_by, updated_by
)
SELECT
    s.subject_id, s.hadm_id, s.stay_id, s.caregiver_id,
    {_date_key("s.chart_time_ts")}, {_date_key("s.store_time_ts")}, s.chart_time_ts, s.store_time_ts,
    s.itemid, s.value, s.valueuom,
    {_minutes_between("s.chart_time_ts", "s.store_time_ts")},
    {_now4}
FROM src s
ON CONFLICT (
    (COALESCE(stay_id, -1)), (COALESCE(item_id, -1)),
    (COALESCE(chart_time, TIMESTAMP 'epoch')), (COALESCE(store_time, TIMESTAMP 'epoch'))
) DO NOTHING;
""",
    ],
    "fact_procedure_event": [
        # ON CONFLICT against warehouse.py's ux_fact_procedure_event_grain expression
        # index -- see fact_datetime_event for why this replaces an anti-join + DISTINCT ON.
        f"""
WITH src AS (
    SELECT s.*, s.starttime::timestamp AS start_time_ts, s.endtime::timestamp AS end_time_ts,
           s.storetime::timestamp AS store_time_ts
    FROM staging.stg_procedureevents s
)
INSERT INTO warehouse.fact_procedure_event (
    subject_id, hadm_id, stay_id, caregiver_id, start_date_key, end_date_key, store_date_key,
    start_time, end_time, store_time, duration_minutes, item_id, value, value_uom, location,
    location_category, order_id, link_order_id, order_category_name, order_category_description,
    patient_weight, is_open_bag, continue_in_next_dept, status_description, original_amount, original_rate,
    created_ts, updated_ts, created_by, updated_by
)
SELECT
    s.subject_id, s.hadm_id, s.stay_id, s.caregiver_id,
    {_date_key("s.start_time_ts")}, {_date_key("s.end_time_ts")}, {_date_key("s.store_time_ts")},
    s.start_time_ts, s.end_time_ts, s.store_time_ts,
    {_minutes_between("s.start_time_ts", "s.end_time_ts")},
    s.itemid, s.value, s.valueuom, s.location, s.locationcategory, s.orderid, s.linkorderid,
    s.ordercategoryname, s.ordercategorydescription, s.patientweight, s.isopenbag, s.continueinnextdept,
    s.statusdescription, s."ORIGINALAMOUNT", s."ORIGINALRATE",
    {_now4}
FROM src s
ON CONFLICT (
    (COALESCE(stay_id, -1)), (COALESCE(item_id, -1)),
    (COALESCE(start_time, TIMESTAMP 'epoch')), (COALESCE(end_time, TIMESTAMP 'epoch')),
    (COALESCE(order_id, -1))
) DO NOTHING;
""",
    ],
}


def load_fact(engine: Engine, table_name: str) -> int:
    """Run `table_name`'s load: any supporting index statements, then the insert-only
    load itself. Returns the number of new rows actually inserted."""
    statements = FACT_LOAD_SQL[table_name]
    with engine.begin() as conn:
        rowcount = 0
        for stmt in statements:
            result = conn.execute(text(stmt))
            if result.returns_rows or result.rowcount not in (None, -1):
                rowcount = result.rowcount
        return rowcount


def load_all_facts(engine: Engine, table_order: list[str]) -> dict[str, int]:
    """Load every fact table in `table_order` (must respect FK dependency order --
    pass warehouse.WAREHOUSE_TABLE_ORDER filtered to fact tables)."""
    return {name: load_fact(engine, name) for name in table_order if name in FACT_LOAD_SQL}


# ======================================================================================
# AGGREGATES -- incrementally-refreshed rollups (per the source DDL's own description),
# recomputed here as a full GROUP BY over the already-loaded warehouse fact tables (not
# staging) and upserted on the aggregate's natural key. At this dataset's volume a full
# recompute is cheap; a true incremental refresh would need a watermark (etl_control in
# the source design) to only touch calendar days affected by new/changed source rows --
# out of scope here, same tradeoff already made for Postgres partitioning (see
# warehouse.py's docstring).
#
# agg_admission_monthly rolls up from agg_admission_daily, not directly from
# fact_admission -- "coarser rolls up from finer", per the source's own design and
# comments -- so it must run after agg_admission_daily (see AGGREGATE_LOAD_ORDER).
# ======================================================================================

AGGREGATE_LOAD_SQL: dict[str, str] = {
    "agg_admission_daily": f"""
WITH admits AS (
    SELECT admit_date_key AS date_key, admission_type, count(*) AS admit_count
    FROM warehouse.fact_admission WHERE admit_date_key IS NOT NULL GROUP BY 1, 2
),
discharges AS (
    SELECT disch_date_key AS date_key, admission_type, count(*) AS discharge_count
    FROM warehouse.fact_admission WHERE disch_date_key IS NOT NULL GROUP BY 1, 2
),
deaths AS (
    SELECT death_date_key AS date_key, admission_type, count(*) AS death_count
    FROM warehouse.fact_admission WHERE death_date_key IS NOT NULL GROUP BY 1, 2
),
keys AS (
    SELECT date_key, admission_type FROM admits
    UNION
    SELECT date_key, admission_type FROM discharges
    UNION
    SELECT date_key, admission_type FROM deaths
)
INSERT INTO warehouse.agg_admission_daily (date_key, admission_type, admit_count, discharge_count, death_count, created_ts, updated_ts)
SELECT
    k.date_key, k.admission_type,
    COALESCE(a.admit_count, 0), COALESCE(d.discharge_count, 0), COALESCE(x.death_count, 0),
    now(), now()
FROM keys k
LEFT JOIN admits a ON a.date_key = k.date_key AND a.admission_type = k.admission_type
LEFT JOIN discharges d ON d.date_key = k.date_key AND d.admission_type = k.admission_type
LEFT JOIN deaths x ON x.date_key = k.date_key AND x.admission_type = k.admission_type
ON CONFLICT (date_key, admission_type) DO UPDATE SET
    admit_count = EXCLUDED.admit_count,
    discharge_count = EXCLUDED.discharge_count,
    death_count = EXCLUDED.death_count,
    updated_ts = now();
""",
    "agg_icu_fluid_balance_daily": """
WITH intake AS (
    -- amount_uom filter: fact_input_event mixes volumes (ml) with medication/electrolyte
    -- doses (mg, mcg, units, mEq, mmol, grams) in the same column -- only ml is a fluid
    -- volume. fact_ingredient_event is deliberately excluded: it's a child-grain
    -- decomposition of fact_input_event's own amount, so including it would double-count.
    SELECT start_date_key AS date_key, stay_id, SUM(amount) AS total_intake_ml
    FROM warehouse.fact_input_event
    WHERE start_date_key IS NOT NULL AND stay_id IS NOT NULL AND amount_uom ILIKE 'ml'
    GROUP BY 1, 2
),
output AS (
    SELECT chart_date_key AS date_key, stay_id, SUM(value) AS total_output_ml
    FROM warehouse.fact_output_event
    WHERE chart_date_key IS NOT NULL AND stay_id IS NOT NULL
    GROUP BY 1, 2
),
keys AS (
    SELECT date_key, stay_id FROM intake
    UNION
    SELECT date_key, stay_id FROM output
)
INSERT INTO warehouse.agg_icu_fluid_balance_daily (date_key, stay_id, total_intake_ml, total_output_ml, net_balance_ml, created_ts, updated_ts)
SELECT
    k.date_key, k.stay_id,
    COALESCE(i.total_intake_ml, 0), COALESCE(o.total_output_ml, 0),
    COALESCE(i.total_intake_ml, 0) - COALESCE(o.total_output_ml, 0),
    now(), now()
FROM keys k
LEFT JOIN intake i ON i.date_key = k.date_key AND i.stay_id = k.stay_id
LEFT JOIN output o ON o.date_key = k.date_key AND o.stay_id = k.stay_id
ON CONFLICT (date_key, stay_id) DO UPDATE SET
    total_intake_ml = EXCLUDED.total_intake_ml,
    total_output_ml = EXCLUDED.total_output_ml,
    net_balance_ml = EXCLUDED.net_balance_ml,
    updated_ts = now();
""",
    "agg_admission_monthly": """
INSERT INTO warehouse.agg_admission_monthly (year_month, admission_type, admit_count, discharge_count, death_count, created_ts, updated_ts)
SELECT
    (date_key / 100) AS year_month, admission_type,
    SUM(admit_count), SUM(discharge_count), SUM(death_count),
    now(), now()
FROM warehouse.agg_admission_daily
GROUP BY 1, 2
ON CONFLICT (year_month, admission_type) DO UPDATE SET
    admit_count = EXCLUDED.admit_count,
    discharge_count = EXCLUDED.discharge_count,
    death_count = EXCLUDED.death_count,
    updated_ts = now();
""",
}

# agg_admission_monthly must run after agg_admission_daily -- it rolls up from it.
AGGREGATE_LOAD_ORDER: list[str] = [
    "agg_admission_daily",
    "agg_icu_fluid_balance_daily",
    "agg_admission_monthly",
]


def refresh_aggregate(engine: Engine, table_name: str) -> int:
    """Recompute and upsert `table_name` from its source fact table(s). Returns the
    number of rows inserted+updated."""
    with engine.begin() as conn:
        result = conn.execute(text(AGGREGATE_LOAD_SQL[table_name]))
        return result.rowcount


def refresh_all_aggregates(engine: Engine) -> dict[str, int]:
    return {name: refresh_aggregate(engine, name) for name in AGGREGATE_LOAD_ORDER}
