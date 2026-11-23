-- Auto-generated from mimic_datawarehouse.warehouse.WAREHOUSE_TABLES.
-- Regenerate with: python scripts/export_warehouse_ddl.py
-- Do not edit by hand -- edit warehouse.py and regenerate instead.
-- Translated from the lakehouse Gold DDL (mimic_iv_ddl_gold_combined_v4.sql).

CREATE SCHEMA IF NOT EXISTS "warehouse";

-- Source file: hosp/d_hcpcs.csv.gz
CREATE TABLE IF NOT EXISTS warehouse."dim_hcpcs" (
    "code" TEXT NOT NULL,
    "category" BIGINT,
    "long_description" TEXT,
    "short_description" TEXT,
    "created_ts" TIMESTAMP,
    "updated_ts" TIMESTAMP,
    "created_by" TEXT,
    "updated_by" TEXT,
    PRIMARY KEY ("code")
);
COMMENT ON TABLE warehouse."dim_hcpcs" IS 'Dimension table: HCPCS billing code definitions (long/short descriptions).';

-- Source file: hosp/d_icd_diagnoses.csv.gz
CREATE TABLE IF NOT EXISTS warehouse."dim_diagnosis" (
    "icd_code" TEXT NOT NULL,
    "icd_version" INTEGER NOT NULL,
    "long_title" TEXT,
    "created_ts" TIMESTAMP,
    "updated_ts" TIMESTAMP,
    "created_by" TEXT,
    "updated_by" TEXT,
    PRIMARY KEY ("icd_code", "icd_version")
);
COMMENT ON TABLE warehouse."dim_diagnosis" IS 'Dimension table: ICD-9/ICD-10 diagnosis code definitions (title text) referenced by fact_diagnosis.';

-- Source file: hosp/d_icd_procedures.csv.gz
CREATE TABLE IF NOT EXISTS warehouse."dim_procedure" (
    "icd_code" TEXT NOT NULL,
    "icd_version" INTEGER NOT NULL,
    "long_title" TEXT,
    "created_ts" TIMESTAMP,
    "updated_ts" TIMESTAMP,
    "created_by" TEXT,
    "updated_by" TEXT,
    PRIMARY KEY ("icd_code", "icd_version")
);
COMMENT ON TABLE warehouse."dim_procedure" IS 'Dimension table: ICD-9/ICD-10 procedure code definitions (title text) referenced by fact_procedure.';

-- Source file: hosp/d_labitems.csv.gz
CREATE TABLE IF NOT EXISTS warehouse."dim_lab_item" (
    "item_id" BIGINT NOT NULL,
    "label" TEXT,
    "fluid" TEXT,
    "category" TEXT,
    "created_ts" TIMESTAMP,
    "updated_ts" TIMESTAMP,
    "created_by" TEXT,
    "updated_by" TEXT,
    PRIMARY KEY ("item_id")
);
COMMENT ON TABLE warehouse."dim_lab_item" IS 'Dimension table: laboratory test (itemid) definitions -- label, fluid type, and category -- referenced by fact_lab_result.';

-- Source file: hosp/patients.csv.gz
CREATE TABLE IF NOT EXISTS warehouse."dim_patient" (
    "subject_id" BIGINT NOT NULL,
    "gender" TEXT,
    "anchor_age" INTEGER,
    "anchor_year" INTEGER,
    "anchor_year_group" TEXT,
    "dod" DATE,
    "created_ts" TIMESTAMP,
    "updated_ts" TIMESTAMP,
    "created_by" TEXT,
    "updated_by" TEXT,
    PRIMARY KEY ("subject_id")
);
COMMENT ON TABLE warehouse."dim_patient" IS 'Patient-level demographics: gender, anonymized age/year anchors, and date of death (if applicable). One row per subject_id.';

-- Source file: hosp/provider.csv.gz
CREATE TABLE IF NOT EXISTS warehouse."dim_provider" (
    "provider_id" TEXT NOT NULL,
    "created_ts" TIMESTAMP,
    "updated_ts" TIMESTAMP,
    "created_by" TEXT,
    "updated_by" TEXT,
    PRIMARY KEY ("provider_id")
);
COMMENT ON TABLE warehouse."dim_provider" IS 'Dimension table: anonymized caregiver/provider identifiers referenced throughout the hosp module.';

-- Source file: icu/caregiver.csv.gz
CREATE TABLE IF NOT EXISTS warehouse."dim_caregiver" (
    "caregiver_id" BIGINT NOT NULL,
    "created_ts" TIMESTAMP,
    "updated_ts" TIMESTAMP,
    "created_by" TEXT,
    "updated_by" TEXT,
    PRIMARY KEY ("caregiver_id")
);
COMMENT ON TABLE warehouse."dim_caregiver" IS 'Dimension table: anonymized ICU caregiver identifiers referenced throughout the ICU module fact tables.';

-- Source file: icu/d_items.csv.gz
CREATE TABLE IF NOT EXISTS warehouse."dim_chart_item" (
    "item_id" BIGINT NOT NULL,
    "label" TEXT,
    "abbreviation" TEXT,
    "links_to" TEXT,
    "category" TEXT,
    "unit_name" TEXT,
    "param_type" TEXT,
    "low_normal_value" DOUBLE PRECISION,
    "high_normal_value" DOUBLE PRECISION,
    "created_ts" TIMESTAMP,
    "updated_ts" TIMESTAMP,
    "created_by" TEXT,
    "updated_by" TEXT,
    PRIMARY KEY ("item_id")
);
COMMENT ON TABLE warehouse."dim_chart_item" IS 'Dimension table: ICU itemid definitions (label, category, unit of measure, normal ranges) referenced by fact_chart_observation, fact_datetime_event, fact_input_event, fact_output_event, fact_procedure_event, and fact_ingredient_event.';

CREATE TABLE IF NOT EXISTS warehouse."dim_date" (
    "date_key" INTEGER NOT NULL,
    "full_date" DATE,
    "day_of_week" INTEGER,
    "day_name" TEXT,
    "day_name_short" TEXT,
    "day_of_month" INTEGER,
    "day_of_year" INTEGER,
    "week_of_year" INTEGER,
    "week_start_date" DATE,
    "week_end_date" DATE,
    "month_number" INTEGER,
    "month_name" TEXT,
    "month_name_short" TEXT,
    "first_day_of_month" DATE,
    "last_day_of_month" DATE,
    "quarter_number" INTEGER,
    "quarter_name" TEXT,
    "year" INTEGER,
    "is_weekday" BOOLEAN,
    "is_weekend" BOOLEAN,
    "is_holiday" BOOLEAN,
    "holiday_name" TEXT,
    "fiscal_month" INTEGER,
    "fiscal_quarter" INTEGER,
    "fiscal_year" INTEGER,
    "created_ts" TIMESTAMP,
    "updated_ts" TIMESTAMP,
    "created_by" TEXT,
    "updated_by" TEXT,
    PRIMARY KEY ("date_key")
);
COMMENT ON TABLE warehouse."dim_date" IS 'Standard calendar date dimension: one row per calendar day. Role-playing -- joined once per date role by every fact table below. Not sourced from a MIMIC CSV; populate separately.';

-- Source file: hosp/admissions.csv.gz
CREATE TABLE IF NOT EXISTS warehouse."fact_admission" (
    "subject_id" BIGINT,
    "hadm_id" BIGINT NOT NULL,
    "admit_date_key" INTEGER,
    "disch_date_key" INTEGER,
    "death_date_key" INTEGER,
    "ed_reg_date_key" INTEGER,
    "ed_out_date_key" INTEGER,
    "admit_time" TIMESTAMP,
    "disch_time" TIMESTAMP,
    "death_time" TIMESTAMP,
    "admission_type" TEXT,
    "admit_provider_id" TEXT,
    "admission_location" TEXT,
    "discharge_location" TEXT,
    "insurance" TEXT,
    "language" TEXT,
    "marital_status" TEXT,
    "race" TEXT,
    "ed_reg_time" TIMESTAMP,
    "ed_out_time" TIMESTAMP,
    "hospital_expire_flag" INTEGER,
    "hospital_los_hours" DOUBLE PRECISION,
    "ed_los_minutes" DOUBLE PRECISION,
    "time_to_death_hours" DOUBLE PRECISION,
    "age_at_admission" INTEGER,
    "is_readmission_flag" BOOLEAN,
    "days_since_prior_discharge" INTEGER,
    "created_ts" TIMESTAMP,
    "updated_ts" TIMESTAMP,
    "created_by" TEXT,
    "updated_by" TEXT,
    PRIMARY KEY ("hadm_id"),
    FOREIGN KEY ("subject_id") REFERENCES warehouse."dim_patient" ("subject_id"),
    FOREIGN KEY ("admit_provider_id") REFERENCES warehouse."dim_provider" ("provider_id"),
    FOREIGN KEY ("admit_date_key") REFERENCES warehouse."dim_date" ("date_key"),
    FOREIGN KEY ("disch_date_key") REFERENCES warehouse."dim_date" ("date_key"),
    FOREIGN KEY ("death_date_key") REFERENCES warehouse."dim_date" ("date_key"),
    FOREIGN KEY ("ed_reg_date_key") REFERENCES warehouse."dim_date" ("date_key"),
    FOREIGN KEY ("ed_out_date_key") REFERENCES warehouse."dim_date" ("date_key")
);
COMMENT ON TABLE warehouse."fact_admission" IS 'Hospital admissions: one row per patient hospitalization (hadm_id).';

-- Source file: hosp/diagnoses_icd.csv.gz
CREATE TABLE IF NOT EXISTS warehouse."fact_diagnosis" (
    "subject_id" BIGINT,
    "hadm_id" BIGINT NOT NULL,
    "seq_num" INTEGER NOT NULL,
    "icd_code" TEXT,
    "icd_version" INTEGER,
    "created_ts" TIMESTAMP,
    "updated_ts" TIMESTAMP,
    "created_by" TEXT,
    "updated_by" TEXT,
    PRIMARY KEY ("hadm_id", "seq_num"),
    FOREIGN KEY ("subject_id") REFERENCES warehouse."dim_patient" ("subject_id"),
    FOREIGN KEY ("hadm_id") REFERENCES warehouse."fact_admission" ("hadm_id"),
    FOREIGN KEY ("icd_code", "icd_version") REFERENCES warehouse."dim_diagnosis" ("icd_code", "icd_version")
);
COMMENT ON TABLE warehouse."fact_diagnosis" IS 'Billed ICD diagnosis codes assigned to each hospital admission, ranked by seq_num. Factless fact.';

-- Source file: hosp/drgcodes.csv.gz
CREATE TABLE IF NOT EXISTS warehouse."fact_drg_assignment" (
    "subject_id" BIGINT,
    "hadm_id" BIGINT NOT NULL,
    "drg_type" TEXT NOT NULL,
    "drg_code" BIGINT NOT NULL,
    "description" TEXT,
    "drg_severity" INTEGER,
    "drg_mortality" INTEGER,
    "created_ts" TIMESTAMP,
    "updated_ts" TIMESTAMP,
    "created_by" TEXT,
    "updated_by" TEXT,
    PRIMARY KEY ("hadm_id", "drg_type", "drg_code"),
    FOREIGN KEY ("subject_id") REFERENCES warehouse."dim_patient" ("subject_id"),
    FOREIGN KEY ("hadm_id") REFERENCES warehouse."fact_admission" ("hadm_id")
);
COMMENT ON TABLE warehouse."fact_drg_assignment" IS 'Diagnosis Related Group (DRG) codes assigned to each hospital admission.';

-- Source file: hosp/poe.csv.gz
CREATE TABLE IF NOT EXISTS warehouse."fact_provider_order" (
    "poe_id" TEXT NOT NULL,
    "poe_seq" INTEGER,
    "subject_id" BIGINT,
    "hadm_id" BIGINT,
    "order_date_key" INTEGER,
    "order_time" TIMESTAMP,
    "order_type" TEXT,
    "order_subtype" TEXT,
    "transaction_type" TEXT,
    "discontinue_of_poe_id" TEXT,
    "discontinued_by_poe_id" TEXT,
    "order_provider_id" TEXT,
    "order_status" TEXT,
    "created_ts" TIMESTAMP,
    "updated_ts" TIMESTAMP,
    "created_by" TEXT,
    "updated_by" TEXT,
    PRIMARY KEY ("poe_id"),
    FOREIGN KEY ("subject_id") REFERENCES warehouse."dim_patient" ("subject_id"),
    FOREIGN KEY ("hadm_id") REFERENCES warehouse."fact_admission" ("hadm_id"),
    FOREIGN KEY ("order_provider_id") REFERENCES warehouse."dim_provider" ("provider_id"),
    FOREIGN KEY ("order_date_key") REFERENCES warehouse."dim_date" ("date_key")
);
COMMENT ON TABLE warehouse."fact_provider_order" IS 'Provider Order Entry: header record of every order placed for a patient.';

-- Source file: hosp/pharmacy.csv.gz
CREATE TABLE IF NOT EXISTS warehouse."fact_pharmacy_order" (
    "subject_id" BIGINT,
    "hadm_id" BIGINT,
    "pharmacy_id" BIGINT NOT NULL,
    "poe_id" TEXT,
    "start_date_key" INTEGER,
    "stop_date_key" INTEGER,
    "enter_date_key" INTEGER,
    "verified_date_key" INTEGER,
    "start_time" TIMESTAMP,
    "stop_time" TIMESTAMP,
    "medication" TEXT,
    "proc_type" TEXT,
    "status" TEXT,
    "enter_time" TIMESTAMP,
    "verified_time" TIMESTAMP,
    "route" TEXT,
    "frequency" TEXT,
    "disp_sched" TEXT,
    "infusion_type" TEXT,
    "sliding_scale" TEXT,
    "lockout_interval" DOUBLE PRECISION,
    "basal_rate" DOUBLE PRECISION,
    "one_hr_max" DOUBLE PRECISION,
    "doses_per_24_hrs" DOUBLE PRECISION,
    "duration" DOUBLE PRECISION,
    "duration_interval" TEXT,
    "expiration_value" DOUBLE PRECISION,
    "expiration_unit" TEXT,
    "expiration_date" DATE,
    "dispensation" TEXT,
    "fill_quantity" DOUBLE PRECISION,
    "created_ts" TIMESTAMP,
    "updated_ts" TIMESTAMP,
    "created_by" TEXT,
    "updated_by" TEXT,
    PRIMARY KEY ("pharmacy_id"),
    FOREIGN KEY ("subject_id") REFERENCES warehouse."dim_patient" ("subject_id"),
    FOREIGN KEY ("hadm_id") REFERENCES warehouse."fact_admission" ("hadm_id"),
    FOREIGN KEY ("poe_id") REFERENCES warehouse."fact_provider_order" ("poe_id"),
    FOREIGN KEY ("start_date_key") REFERENCES warehouse."dim_date" ("date_key"),
    FOREIGN KEY ("stop_date_key") REFERENCES warehouse."dim_date" ("date_key"),
    FOREIGN KEY ("enter_date_key") REFERENCES warehouse."dim_date" ("date_key"),
    FOREIGN KEY ("verified_date_key") REFERENCES warehouse."dim_date" ("date_key")
);
COMMENT ON TABLE warehouse."fact_pharmacy_order" IS 'Pharmacy dispensing records: medication orders with dosing schedule, route, frequency, and dispensation details.';

-- Source file: hosp/emar.csv.gz
CREATE TABLE IF NOT EXISTS warehouse."fact_medication_administration" (
    "subject_id" BIGINT,
    "hadm_id" BIGINT,
    "emar_id" TEXT NOT NULL,
    "emar_seq" INTEGER,
    "poe_id" TEXT,
    "pharmacy_id" BIGINT,
    "enter_provider_id" TEXT,
    "chart_date_key" INTEGER,
    "schedule_date_key" INTEGER,
    "store_date_key" INTEGER,
    "chart_time" TIMESTAMP,
    "medication" TEXT,
    "event_txt" TEXT,
    "schedule_time" TIMESTAMP,
    "store_time" TIMESTAMP,
    "admin_delay_minutes" DOUBLE PRECISION,
    "created_ts" TIMESTAMP,
    "updated_ts" TIMESTAMP,
    "created_by" TEXT,
    "updated_by" TEXT,
    PRIMARY KEY ("emar_id"),
    FOREIGN KEY ("subject_id") REFERENCES warehouse."dim_patient" ("subject_id"),
    FOREIGN KEY ("hadm_id") REFERENCES warehouse."fact_admission" ("hadm_id"),
    FOREIGN KEY ("poe_id") REFERENCES warehouse."fact_provider_order" ("poe_id"),
    FOREIGN KEY ("pharmacy_id") REFERENCES warehouse."fact_pharmacy_order" ("pharmacy_id"),
    FOREIGN KEY ("enter_provider_id") REFERENCES warehouse."dim_provider" ("provider_id"),
    FOREIGN KEY ("chart_date_key") REFERENCES warehouse."dim_date" ("date_key"),
    FOREIGN KEY ("schedule_date_key") REFERENCES warehouse."dim_date" ("date_key"),
    FOREIGN KEY ("store_date_key") REFERENCES warehouse."dim_date" ("date_key")
);
COMMENT ON TABLE warehouse."fact_medication_administration" IS 'Electronic Medication Administration Record: header-level record of each medication administration event.';

-- Source file: hosp/emar_detail.csv.gz
CREATE TABLE IF NOT EXISTS warehouse."fact_medication_administration_mini_detail" (
    "med_admin_detail_id" BIGINT GENERATED ALWAYS AS IDENTITY,
    "subject_id" BIGINT,
    "emar_id" TEXT,
    "emar_seq" INTEGER,
    "parent_field_ordinal" DOUBLE PRECISION,
    "administration_type" TEXT,
    "pharmacy_id" BIGINT,
    "barcode_type" TEXT,
    "reason_for_no_barcode" TEXT,
    "complete_dose_not_given" TEXT,
    "dose_due" TEXT,
    "dose_due_unit" TEXT,
    "dose_given" TEXT,
    "dose_given_unit" TEXT,
    "will_remainder_of_dose_be_given" TEXT,
    "product_amount_given" DOUBLE PRECISION,
    "product_unit" TEXT,
    "product_code" TEXT,
    "product_description" TEXT,
    "product_description_other" TEXT,
    "prior_infusion_rate" DOUBLE PRECISION,
    "infusion_rate" DOUBLE PRECISION,
    "infusion_rate_adjustment" TEXT,
    "infusion_rate_adjustment_amount" DOUBLE PRECISION,
    "infusion_rate_unit" TEXT,
    "route" TEXT,
    "infusion_complete" TEXT,
    "completion_interval" TEXT,
    "new_iv_bag_hung" TEXT,
    "continued_infusion_in_other_location" TEXT,
    "restart_interval" TEXT,
    "side" TEXT,
    "site" TEXT,
    "non_formulary_visual_verification" TEXT,
    "created_ts" TIMESTAMP,
    "updated_ts" TIMESTAMP,
    "created_by" TEXT,
    "updated_by" TEXT,
    PRIMARY KEY ("med_admin_detail_id"),
    FOREIGN KEY ("subject_id") REFERENCES warehouse."dim_patient" ("subject_id"),
    FOREIGN KEY ("emar_id") REFERENCES warehouse."fact_medication_administration" ("emar_id"),
    FOREIGN KEY ("pharmacy_id") REFERENCES warehouse."fact_pharmacy_order" ("pharmacy_id")
);
COMMENT ON TABLE warehouse."fact_medication_administration_mini_detail" IS 'Electronic Medication Administration Record detail: granular dose/administration attributes for each emar_id. Surrogate PK -- (emar_id, parent_field_ordinal) can''t be one, parent_field_ordinal is NULL in half the real rows. UNIQUE INDEX ux_med_admin_mini_detail_grain on COALESCE(emar_id,''''), COALESCE(emar_seq,-1), COALESCE(parent_field_ordinal,-1) is the dedup grain etl_warehouse.py''s ON CONFLICT loader uses -- an expression index (not a plain UNIQUE) so two NULL-parent_field_ordinal rows still collide, since a plain UNIQUE treats NULL as never-equal-to-NULL.';
CREATE UNIQUE INDEX IF NOT EXISTS "ux_med_admin_mini_detail_grain" ON warehouse."fact_medication_administration_mini_detail" (COALESCE("emar_id", ''), COALESCE("emar_seq", -1), COALESCE("parent_field_ordinal", -1));

-- Source file: hosp/poe_detail.csv.gz
CREATE TABLE IF NOT EXISTS warehouse."fact_provider_order_mini_detail" (
    "poe_id" TEXT NOT NULL,
    "poe_seq" INTEGER,
    "subject_id" BIGINT,
    "field_name" TEXT NOT NULL,
    "field_value" TEXT,
    "created_ts" TIMESTAMP,
    "updated_ts" TIMESTAMP,
    "created_by" TEXT,
    "updated_by" TEXT,
    PRIMARY KEY ("poe_id", "field_name"),
    FOREIGN KEY ("subject_id") REFERENCES warehouse."dim_patient" ("subject_id"),
    FOREIGN KEY ("poe_id") REFERENCES warehouse."fact_provider_order" ("poe_id")
);
COMMENT ON TABLE warehouse."fact_provider_order_mini_detail" IS 'Provider Order Entry detail: free-text field name/value pairs providing additional context for a poe_id.';

-- Source file: hosp/prescriptions.csv.gz
CREATE TABLE IF NOT EXISTS warehouse."fact_prescription" (
    "prescription_id" BIGINT GENERATED ALWAYS AS IDENTITY,
    "subject_id" BIGINT,
    "hadm_id" BIGINT,
    "pharmacy_id" BIGINT,
    "poe_id" TEXT,
    "poe_seq" INTEGER,
    "order_provider_id" TEXT,
    "start_date_key" INTEGER,
    "stop_date_key" INTEGER,
    "start_time" TIMESTAMP,
    "stop_time" TIMESTAMP,
    "drug_type" TEXT,
    "drug" TEXT,
    "formulary_drug_cd" TEXT,
    "gsn" TEXT,
    "ndc" BIGINT,
    "prod_strength" TEXT,
    "form_rx" TEXT,
    "dose_val_rx" TEXT,
    "dose_unit_rx" TEXT,
    "form_val_disp" TEXT,
    "form_unit_disp" TEXT,
    "doses_per_24_hrs" DOUBLE PRECISION,
    "route" TEXT,
    "created_ts" TIMESTAMP,
    "updated_ts" TIMESTAMP,
    "created_by" TEXT,
    "updated_by" TEXT,
    PRIMARY KEY ("prescription_id"),
    FOREIGN KEY ("subject_id") REFERENCES warehouse."dim_patient" ("subject_id"),
    FOREIGN KEY ("hadm_id") REFERENCES warehouse."fact_admission" ("hadm_id"),
    FOREIGN KEY ("pharmacy_id") REFERENCES warehouse."fact_pharmacy_order" ("pharmacy_id"),
    FOREIGN KEY ("poe_id") REFERENCES warehouse."fact_provider_order" ("poe_id"),
    FOREIGN KEY ("order_provider_id") REFERENCES warehouse."dim_provider" ("provider_id"),
    FOREIGN KEY ("start_date_key") REFERENCES warehouse."dim_date" ("date_key"),
    FOREIGN KEY ("stop_date_key") REFERENCES warehouse."dim_date" ("date_key")
);
COMMENT ON TABLE warehouse."fact_prescription" IS 'Prescribed medication orders including drug identifiers (NDC, GSN), dose, formulary code, and route. Surrogate PK -- no natural unique column in the source. UNIQUE INDEX ux_fact_prescription_grain on COALESCE(subject_id,-1), COALESCE(hadm_id,-1), COALESCE(pharmacy_id,-1), COALESCE(poe_id,''''), COALESCE(poe_seq,-1), COALESCE(start_time,epoch), COALESCE(drug,'''') is the dedup grain etl_warehouse.py''s ON CONFLICT loader uses -- an expression index (not a plain UNIQUE) so NULL-containing grains (pharmacy_id/poe_id are unresolved for a real fraction of rows) still collide correctly.';
CREATE UNIQUE INDEX IF NOT EXISTS "ux_fact_prescription_grain" ON warehouse."fact_prescription" (COALESCE("subject_id", -1), COALESCE("hadm_id", -1), COALESCE("pharmacy_id", -1), COALESCE("poe_id", ''), COALESCE("poe_seq", -1), COALESCE("start_time", TIMESTAMP 'epoch'), COALESCE("drug", ''));

-- Source file: hosp/hcpcsevents.csv.gz
CREATE TABLE IF NOT EXISTS warehouse."fact_hcpcs_event" (
    "subject_id" BIGINT,
    "hadm_id" BIGINT NOT NULL,
    "chart_date_key" INTEGER,
    "chart_date" DATE,
    "hcpcs_cd" TEXT,
    "seq_num" INTEGER NOT NULL,
    "short_description" TEXT,
    "created_ts" TIMESTAMP,
    "updated_ts" TIMESTAMP,
    "created_by" TEXT,
    "updated_by" TEXT,
    PRIMARY KEY ("hadm_id", "seq_num"),
    FOREIGN KEY ("subject_id") REFERENCES warehouse."dim_patient" ("subject_id"),
    FOREIGN KEY ("hadm_id") REFERENCES warehouse."fact_admission" ("hadm_id"),
    FOREIGN KEY ("hcpcs_cd") REFERENCES warehouse."dim_hcpcs" ("code"),
    FOREIGN KEY ("chart_date_key") REFERENCES warehouse."dim_date" ("date_key")
);
COMMENT ON TABLE warehouse."fact_hcpcs_event" IS 'Billed HCPCS procedure/service codes recorded against a hospital admission.';

-- Source file: hosp/labevents.csv.gz
CREATE TABLE IF NOT EXISTS warehouse."fact_lab_result" (
    "labevent_id" BIGINT NOT NULL,
    "subject_id" BIGINT,
    "hadm_id" BIGINT,
    "specimen_id" BIGINT,
    "item_id" BIGINT,
    "order_provider_id" TEXT,
    "chart_date_key" INTEGER,
    "store_date_key" INTEGER,
    "chart_time" TIMESTAMP,
    "store_time" TIMESTAMP,
    "value" TEXT,
    "value_num" DOUBLE PRECISION,
    "value_uom" TEXT,
    "ref_range_lower" DOUBLE PRECISION,
    "ref_range_upper" DOUBLE PRECISION,
    "flag" TEXT,
    "priority" TEXT,
    "comments" TEXT,
    "result_turnaround_minutes" DOUBLE PRECISION,
    "is_abnormal_flag" BOOLEAN,
    "created_ts" TIMESTAMP,
    "updated_ts" TIMESTAMP,
    "created_by" TEXT,
    "updated_by" TEXT,
    PRIMARY KEY ("labevent_id"),
    FOREIGN KEY ("subject_id") REFERENCES warehouse."dim_patient" ("subject_id"),
    FOREIGN KEY ("hadm_id") REFERENCES warehouse."fact_admission" ("hadm_id"),
    FOREIGN KEY ("item_id") REFERENCES warehouse."dim_lab_item" ("item_id"),
    FOREIGN KEY ("order_provider_id") REFERENCES warehouse."dim_provider" ("provider_id"),
    FOREIGN KEY ("chart_date_key") REFERENCES warehouse."dim_date" ("date_key"),
    FOREIGN KEY ("store_date_key") REFERENCES warehouse."dim_date" ("date_key")
);
COMMENT ON TABLE warehouse."fact_lab_result" IS 'Laboratory test results collected for a patient, optionally tied to a hospital admission.';

-- Source file: hosp/microbiologyevents.csv.gz
CREATE TABLE IF NOT EXISTS warehouse."fact_microbiology_result" (
    "microevent_id" BIGINT NOT NULL,
    "subject_id" BIGINT,
    "hadm_id" BIGINT,
    "micro_specimen_id" BIGINT,
    "order_provider_id" TEXT,
    "chart_date_key" INTEGER,
    "store_date_key" INTEGER,
    "chart_date" TIMESTAMP,
    "chart_time" TIMESTAMP,
    "spec_itemid" BIGINT,
    "spec_type_desc" TEXT,
    "test_seq" INTEGER,
    "store_date" TIMESTAMP,
    "store_time" TIMESTAMP,
    "test_itemid" BIGINT,
    "test_name" TEXT,
    "org_itemid" BIGINT,
    "org_name" TEXT,
    "isolate_num" INTEGER,
    "quantity" DOUBLE PRECISION,
    "ab_itemid" BIGINT,
    "ab_name" TEXT,
    "dilution_text" TEXT,
    "dilution_comparison" TEXT,
    "dilution_value" DOUBLE PRECISION,
    "interpretation" TEXT,
    "comments" TEXT,
    "result_turnaround_minutes" DOUBLE PRECISION,
    "is_positive_culture_flag" BOOLEAN,
    "created_ts" TIMESTAMP,
    "updated_ts" TIMESTAMP,
    "created_by" TEXT,
    "updated_by" TEXT,
    PRIMARY KEY ("microevent_id"),
    FOREIGN KEY ("subject_id") REFERENCES warehouse."dim_patient" ("subject_id"),
    FOREIGN KEY ("hadm_id") REFERENCES warehouse."fact_admission" ("hadm_id"),
    FOREIGN KEY ("order_provider_id") REFERENCES warehouse."dim_provider" ("provider_id"),
    FOREIGN KEY ("chart_date_key") REFERENCES warehouse."dim_date" ("date_key"),
    FOREIGN KEY ("store_date_key") REFERENCES warehouse."dim_date" ("date_key")
);
COMMENT ON TABLE warehouse."fact_microbiology_result" IS 'Microbiology culture orders, organisms isolated, and antibiotic sensitivity results. spec/test/org/ab itemid columns have no FK -- no dimension table exists for those code spaces in the source.';

-- Source file: hosp/omr.csv.gz
CREATE TABLE IF NOT EXISTS warehouse."fact_outpatient_measurement" (
    "subject_id" BIGINT NOT NULL,
    "chart_date_key" INTEGER,
    "chart_date" DATE NOT NULL,
    "seq_num" INTEGER NOT NULL,
    "result_name" TEXT NOT NULL,
    "result_value" TEXT,
    "created_ts" TIMESTAMP,
    "updated_ts" TIMESTAMP,
    "created_by" TEXT,
    "updated_by" TEXT,
    PRIMARY KEY ("subject_id", "chart_date", "seq_num", "result_name"),
    FOREIGN KEY ("subject_id") REFERENCES warehouse."dim_patient" ("subject_id"),
    FOREIGN KEY ("chart_date_key") REFERENCES warehouse."dim_date" ("date_key")
);
COMMENT ON TABLE warehouse."fact_outpatient_measurement" IS 'Online Medical Record: outpatient-recorded observations such as height, weight, and blood pressure.';

-- Source file: hosp/procedures_icd.csv.gz
CREATE TABLE IF NOT EXISTS warehouse."fact_procedure" (
    "subject_id" BIGINT,
    "hadm_id" BIGINT NOT NULL,
    "seq_num" INTEGER NOT NULL,
    "chart_date_key" INTEGER,
    "chart_date" DATE,
    "icd_code" TEXT,
    "icd_version" INTEGER,
    "created_ts" TIMESTAMP,
    "updated_ts" TIMESTAMP,
    "created_by" TEXT,
    "updated_by" TEXT,
    PRIMARY KEY ("hadm_id", "seq_num"),
    FOREIGN KEY ("subject_id") REFERENCES warehouse."dim_patient" ("subject_id"),
    FOREIGN KEY ("hadm_id") REFERENCES warehouse."fact_admission" ("hadm_id"),
    FOREIGN KEY ("chart_date_key") REFERENCES warehouse."dim_date" ("date_key"),
    FOREIGN KEY ("icd_code", "icd_version") REFERENCES warehouse."dim_procedure" ("icd_code", "icd_version")
);
COMMENT ON TABLE warehouse."fact_procedure" IS 'Billed ICD procedure codes performed during a hospital admission, ranked by seq_num. Factless fact.';

-- Source file: hosp/services.csv.gz
CREATE TABLE IF NOT EXISTS warehouse."fact_service_assignment" (
    "subject_id" BIGINT,
    "hadm_id" BIGINT NOT NULL,
    "transfer_date_key" INTEGER,
    "transfer_time" TIMESTAMP NOT NULL,
    "prev_service" TEXT,
    "curr_service" TEXT,
    "created_ts" TIMESTAMP,
    "updated_ts" TIMESTAMP,
    "created_by" TEXT,
    "updated_by" TEXT,
    PRIMARY KEY ("hadm_id", "transfer_time"),
    FOREIGN KEY ("subject_id") REFERENCES warehouse."dim_patient" ("subject_id"),
    FOREIGN KEY ("hadm_id") REFERENCES warehouse."fact_admission" ("hadm_id"),
    FOREIGN KEY ("transfer_date_key") REFERENCES warehouse."dim_date" ("date_key")
);
COMMENT ON TABLE warehouse."fact_service_assignment" IS 'Hospital service transfers: record of the clinical service a patient was under during an admission.';

-- Source file: hosp/transfers.csv.gz
CREATE TABLE IF NOT EXISTS warehouse."fact_transfer" (
    "subject_id" BIGINT,
    "hadm_id" BIGINT,
    "transfer_id" BIGINT NOT NULL,
    "event_type" TEXT,
    "care_unit" TEXT,
    "in_date_key" INTEGER,
    "out_date_key" INTEGER,
    "in_time" TIMESTAMP,
    "out_time" TIMESTAMP,
    "transfer_duration_hours" DOUBLE PRECISION,
    "created_ts" TIMESTAMP,
    "updated_ts" TIMESTAMP,
    "created_by" TEXT,
    "updated_by" TEXT,
    PRIMARY KEY ("transfer_id"),
    FOREIGN KEY ("subject_id") REFERENCES warehouse."dim_patient" ("subject_id"),
    FOREIGN KEY ("hadm_id") REFERENCES warehouse."fact_admission" ("hadm_id"),
    FOREIGN KEY ("in_date_key") REFERENCES warehouse."dim_date" ("date_key"),
    FOREIGN KEY ("out_date_key") REFERENCES warehouse."dim_date" ("date_key")
);
COMMENT ON TABLE warehouse."fact_transfer" IS 'Patient movement between hospital units/wards (ADT events) during an admission, including ICU stay boundaries.';

-- Source file: icu/icustays.csv.gz
CREATE TABLE IF NOT EXISTS warehouse."fact_icu_stay_accumulating" (
    "subject_id" BIGINT,
    "hadm_id" BIGINT,
    "stay_id" BIGINT NOT NULL,
    "first_careunit" TEXT,
    "last_careunit" TEXT,
    "in_date_key" INTEGER,
    "out_date_key" INTEGER,
    "in_time" TIMESTAMP,
    "out_time" TIMESTAMP,
    "los" DOUBLE PRECISION,
    "time_to_icu_hours" DOUBLE PRECISION,
    "is_icu_readmission_flag" BOOLEAN,
    "created_ts" TIMESTAMP,
    "updated_ts" TIMESTAMP,
    "created_by" TEXT,
    "updated_by" TEXT,
    PRIMARY KEY ("stay_id"),
    FOREIGN KEY ("subject_id") REFERENCES warehouse."dim_patient" ("subject_id"),
    FOREIGN KEY ("hadm_id") REFERENCES warehouse."fact_admission" ("hadm_id"),
    FOREIGN KEY ("in_date_key") REFERENCES warehouse."dim_date" ("date_key"),
    FOREIGN KEY ("out_date_key") REFERENCES warehouse."dim_date" ("date_key")
);
COMMENT ON TABLE warehouse."fact_icu_stay_accumulating" IS 'One row per ICU stay (stay_id). Accumulating-snapshot fact: child grain of fact_admission.';

-- Source file: icu/chartevents.csv.gz
CREATE TABLE IF NOT EXISTS warehouse."fact_chart_observation" (
    "chart_observation_id" BIGINT GENERATED ALWAYS AS IDENTITY,
    "subject_id" BIGINT,
    "hadm_id" BIGINT,
    "stay_id" BIGINT,
    "caregiver_id" BIGINT,
    "chart_date_key" INTEGER,
    "store_date_key" INTEGER,
    "chart_time" TIMESTAMP,
    "store_time" TIMESTAMP,
    "item_id" BIGINT,
    "value" TEXT,
    "value_num" DOUBLE PRECISION,
    "value_uom" TEXT,
    "warning" INTEGER,
    "charting_delay_minutes" DOUBLE PRECISION,
    "is_abnormal_flag" BOOLEAN,
    "created_ts" TIMESTAMP,
    "updated_ts" TIMESTAMP,
    "created_by" TEXT,
    "updated_by" TEXT,
    PRIMARY KEY ("chart_observation_id"),
    FOREIGN KEY ("subject_id") REFERENCES warehouse."dim_patient" ("subject_id"),
    FOREIGN KEY ("hadm_id") REFERENCES warehouse."fact_admission" ("hadm_id"),
    FOREIGN KEY ("stay_id") REFERENCES warehouse."fact_icu_stay_accumulating" ("stay_id"),
    FOREIGN KEY ("caregiver_id") REFERENCES warehouse."dim_caregiver" ("caregiver_id"),
    FOREIGN KEY ("item_id") REFERENCES warehouse."dim_chart_item" ("item_id"),
    FOREIGN KEY ("chart_date_key") REFERENCES warehouse."dim_date" ("date_key"),
    FOREIGN KEY ("store_date_key") REFERENCES warehouse."dim_date" ("date_key")
);
COMMENT ON TABLE warehouse."fact_chart_observation" IS 'Charted vital signs and observations recorded at the bedside during an ICU stay. Highest-volume ICU fact table; surrogate PK (no natural ID column in source). No UNIQUE constraint on (stay_id, item_id, chart_time, store_time): ~12% of raw chartevents.csv.gz rows genuinely collide on it.';

-- Source file: icu/datetimeevents.csv.gz
CREATE TABLE IF NOT EXISTS warehouse."fact_datetime_event" (
    "datetime_event_id" BIGINT GENERATED ALWAYS AS IDENTITY,
    "subject_id" BIGINT,
    "hadm_id" BIGINT,
    "stay_id" BIGINT,
    "caregiver_id" BIGINT,
    "chart_date_key" INTEGER,
    "store_date_key" INTEGER,
    "chart_time" TIMESTAMP,
    "store_time" TIMESTAMP,
    "item_id" BIGINT,
    "value" TEXT,
    "value_uom" TEXT,
    "warning" INTEGER,
    "charting_delay_minutes" DOUBLE PRECISION,
    "created_ts" TIMESTAMP,
    "updated_ts" TIMESTAMP,
    "created_by" TEXT,
    "updated_by" TEXT,
    PRIMARY KEY ("datetime_event_id"),
    FOREIGN KEY ("subject_id") REFERENCES warehouse."dim_patient" ("subject_id"),
    FOREIGN KEY ("hadm_id") REFERENCES warehouse."fact_admission" ("hadm_id"),
    FOREIGN KEY ("stay_id") REFERENCES warehouse."fact_icu_stay_accumulating" ("stay_id"),
    FOREIGN KEY ("caregiver_id") REFERENCES warehouse."dim_caregiver" ("caregiver_id"),
    FOREIGN KEY ("item_id") REFERENCES warehouse."dim_chart_item" ("item_id"),
    FOREIGN KEY ("chart_date_key") REFERENCES warehouse."dim_date" ("date_key"),
    FOREIGN KEY ("store_date_key") REFERENCES warehouse."dim_date" ("date_key")
);
COMMENT ON TABLE warehouse."fact_datetime_event" IS 'ICU-charted events whose value is itself a date/time, recorded during an ICU stay. Surrogate PK (no natural ID column in source). UNIQUE INDEX ux_fact_datetime_event_grain on COALESCE(stay_id,-1), COALESCE(item_id,-1), COALESCE(chart_time,epoch), COALESCE(store_time,epoch) is the dedup grain etl_warehouse.py''s ON CONFLICT loader uses -- an expression index (not a plain UNIQUE) so a NULL store_time still collides correctly.';
CREATE UNIQUE INDEX IF NOT EXISTS "ux_fact_datetime_event_grain" ON warehouse."fact_datetime_event" (COALESCE("stay_id", -1), COALESCE("item_id", -1), COALESCE("chart_time", TIMESTAMP 'epoch'), COALESCE("store_time", TIMESTAMP 'epoch'));

-- Source file: icu/inputevents.csv.gz
CREATE TABLE IF NOT EXISTS warehouse."fact_input_event" (
    "input_event_id" BIGINT GENERATED ALWAYS AS IDENTITY,
    "subject_id" BIGINT,
    "hadm_id" BIGINT,
    "stay_id" BIGINT,
    "caregiver_id" BIGINT,
    "start_date_key" INTEGER,
    "end_date_key" INTEGER,
    "store_date_key" INTEGER,
    "start_time" TIMESTAMP,
    "end_time" TIMESTAMP,
    "store_time" TIMESTAMP,
    "duration_minutes" DOUBLE PRECISION,
    "item_id" BIGINT,
    "amount" DOUBLE PRECISION,
    "amount_uom" TEXT,
    "rate" DOUBLE PRECISION,
    "rate_uom" TEXT,
    "order_id" BIGINT,
    "link_order_id" BIGINT,
    "order_category_name" TEXT,
    "secondary_order_category_name" TEXT,
    "order_component_type_description" TEXT,
    "order_category_description" TEXT,
    "patient_weight" DOUBLE PRECISION,
    "total_amount" DOUBLE PRECISION,
    "total_amount_uom" TEXT,
    "is_open_bag" INTEGER,
    "continue_in_next_dept" INTEGER,
    "status_description" TEXT,
    "original_amount" DOUBLE PRECISION,
    "original_rate" DOUBLE PRECISION,
    "created_ts" TIMESTAMP,
    "updated_ts" TIMESTAMP,
    "created_by" TEXT,
    "updated_by" TEXT,
    PRIMARY KEY ("input_event_id"),
    FOREIGN KEY ("subject_id") REFERENCES warehouse."dim_patient" ("subject_id"),
    FOREIGN KEY ("hadm_id") REFERENCES warehouse."fact_admission" ("hadm_id"),
    FOREIGN KEY ("stay_id") REFERENCES warehouse."fact_icu_stay_accumulating" ("stay_id"),
    FOREIGN KEY ("caregiver_id") REFERENCES warehouse."dim_caregiver" ("caregiver_id"),
    FOREIGN KEY ("item_id") REFERENCES warehouse."dim_chart_item" ("item_id"),
    FOREIGN KEY ("start_date_key") REFERENCES warehouse."dim_date" ("date_key"),
    FOREIGN KEY ("end_date_key") REFERENCES warehouse."dim_date" ("date_key"),
    FOREIGN KEY ("store_date_key") REFERENCES warehouse."dim_date" ("date_key")
);
COMMENT ON TABLE warehouse."fact_input_event" IS 'Intake events: fluids, medications, and nutrition administered during an ICU stay. Surrogate PK (no natural ID column in source). UNIQUE INDEX ux_fact_input_event_grain on COALESCE(stay_id,-1), COALESCE(item_id,-1), COALESCE(start_time,epoch), COALESCE(end_time,epoch), COALESCE(order_id,-1) is the dedup grain etl_warehouse.py''s ON CONFLICT loader uses -- an expression index (not a plain UNIQUE) so a NULL order_id/end_time still collides correctly.';
CREATE UNIQUE INDEX IF NOT EXISTS "ux_fact_input_event_grain" ON warehouse."fact_input_event" (COALESCE("stay_id", -1), COALESCE("item_id", -1), COALESCE("start_time", TIMESTAMP 'epoch'), COALESCE("end_time", TIMESTAMP 'epoch'), COALESCE("order_id", -1));

-- Source file: icu/ingredientevents.csv.gz
CREATE TABLE IF NOT EXISTS warehouse."fact_ingredient_event" (
    "ingredient_event_id" BIGINT GENERATED ALWAYS AS IDENTITY,
    "subject_id" BIGINT,
    "hadm_id" BIGINT,
    "stay_id" BIGINT,
    "caregiver_id" BIGINT,
    "start_date_key" INTEGER,
    "end_date_key" INTEGER,
    "store_date_key" INTEGER,
    "start_time" TIMESTAMP,
    "end_time" TIMESTAMP,
    "store_time" TIMESTAMP,
    "duration_minutes" DOUBLE PRECISION,
    "item_id" BIGINT,
    "amount" DOUBLE PRECISION,
    "amount_uom" TEXT,
    "rate" DOUBLE PRECISION,
    "rate_uom" TEXT,
    "order_id" BIGINT,
    "link_order_id" BIGINT,
    "status_description" TEXT,
    "original_amount" BIGINT,
    "original_rate" DOUBLE PRECISION,
    "created_ts" TIMESTAMP,
    "updated_ts" TIMESTAMP,
    "created_by" TEXT,
    "updated_by" TEXT,
    PRIMARY KEY ("ingredient_event_id"),
    FOREIGN KEY ("subject_id") REFERENCES warehouse."dim_patient" ("subject_id"),
    FOREIGN KEY ("hadm_id") REFERENCES warehouse."fact_admission" ("hadm_id"),
    FOREIGN KEY ("stay_id") REFERENCES warehouse."fact_icu_stay_accumulating" ("stay_id"),
    FOREIGN KEY ("caregiver_id") REFERENCES warehouse."dim_caregiver" ("caregiver_id"),
    FOREIGN KEY ("item_id") REFERENCES warehouse."dim_chart_item" ("item_id"),
    FOREIGN KEY ("start_date_key") REFERENCES warehouse."dim_date" ("date_key"),
    FOREIGN KEY ("end_date_key") REFERENCES warehouse."dim_date" ("date_key"),
    FOREIGN KEY ("store_date_key") REFERENCES warehouse."dim_date" ("date_key")
);
COMMENT ON TABLE warehouse."fact_ingredient_event" IS 'Individual ingredient-level detail underlying fact_input_event records. Child grain of fact_input_event; surrogate PK. UNIQUE INDEX ux_fact_ingredient_event_grain on COALESCE(stay_id,-1), COALESCE(item_id,-1), COALESCE(start_time,epoch), COALESCE(end_time,epoch), COALESCE(order_id,-1) is the dedup grain etl_warehouse.py''s ON CONFLICT loader uses -- an expression index (not a plain UNIQUE) so a NULL order_id/end_time still collides correctly.';
CREATE UNIQUE INDEX IF NOT EXISTS "ux_fact_ingredient_event_grain" ON warehouse."fact_ingredient_event" (COALESCE("stay_id", -1), COALESCE("item_id", -1), COALESCE("start_time", TIMESTAMP 'epoch'), COALESCE("end_time", TIMESTAMP 'epoch'), COALESCE("order_id", -1));

-- Source file: icu/outputevents.csv.gz
CREATE TABLE IF NOT EXISTS warehouse."fact_output_event" (
    "output_event_id" BIGINT GENERATED ALWAYS AS IDENTITY,
    "subject_id" BIGINT,
    "hadm_id" BIGINT,
    "stay_id" BIGINT,
    "caregiver_id" BIGINT,
    "chart_date_key" INTEGER,
    "store_date_key" INTEGER,
    "chart_time" TIMESTAMP,
    "store_time" TIMESTAMP,
    "item_id" BIGINT,
    "value" BIGINT,
    "value_uom" TEXT,
    "charting_delay_minutes" DOUBLE PRECISION,
    "created_ts" TIMESTAMP,
    "updated_ts" TIMESTAMP,
    "created_by" TEXT,
    "updated_by" TEXT,
    PRIMARY KEY ("output_event_id"),
    FOREIGN KEY ("subject_id") REFERENCES warehouse."dim_patient" ("subject_id"),
    FOREIGN KEY ("hadm_id") REFERENCES warehouse."fact_admission" ("hadm_id"),
    FOREIGN KEY ("stay_id") REFERENCES warehouse."fact_icu_stay_accumulating" ("stay_id"),
    FOREIGN KEY ("caregiver_id") REFERENCES warehouse."dim_caregiver" ("caregiver_id"),
    FOREIGN KEY ("item_id") REFERENCES warehouse."dim_chart_item" ("item_id"),
    FOREIGN KEY ("chart_date_key") REFERENCES warehouse."dim_date" ("date_key"),
    FOREIGN KEY ("store_date_key") REFERENCES warehouse."dim_date" ("date_key")
);
COMMENT ON TABLE warehouse."fact_output_event" IS 'Output events: fluid output recorded for a patient during an ICU stay. Surrogate PK (no natural ID column in source). UNIQUE INDEX ux_fact_output_event_grain on COALESCE(stay_id,-1), COALESCE(item_id,-1), COALESCE(chart_time,epoch), COALESCE(store_time,epoch) is the dedup grain etl_warehouse.py''s ON CONFLICT loader uses -- an expression index (not a plain UNIQUE) so a NULL store_time still collides correctly.';
CREATE UNIQUE INDEX IF NOT EXISTS "ux_fact_output_event_grain" ON warehouse."fact_output_event" (COALESCE("stay_id", -1), COALESCE("item_id", -1), COALESCE("chart_time", TIMESTAMP 'epoch'), COALESCE("store_time", TIMESTAMP 'epoch'));

-- Source file: icu/procedureevents.csv.gz
CREATE TABLE IF NOT EXISTS warehouse."fact_procedure_event" (
    "procedure_event_id" BIGINT GENERATED ALWAYS AS IDENTITY,
    "subject_id" BIGINT,
    "hadm_id" BIGINT,
    "stay_id" BIGINT,
    "caregiver_id" BIGINT,
    "start_date_key" INTEGER,
    "end_date_key" INTEGER,
    "store_date_key" INTEGER,
    "start_time" TIMESTAMP,
    "end_time" TIMESTAMP,
    "store_time" TIMESTAMP,
    "duration_minutes" DOUBLE PRECISION,
    "item_id" BIGINT,
    "value" DOUBLE PRECISION,
    "value_uom" TEXT,
    "location" TEXT,
    "location_category" TEXT,
    "order_id" BIGINT,
    "link_order_id" BIGINT,
    "order_category_name" TEXT,
    "order_category_description" TEXT,
    "patient_weight" DOUBLE PRECISION,
    "is_open_bag" INTEGER,
    "continue_in_next_dept" INTEGER,
    "status_description" TEXT,
    "original_amount" BIGINT,
    "original_rate" INTEGER,
    "created_ts" TIMESTAMP,
    "updated_ts" TIMESTAMP,
    "created_by" TEXT,
    "updated_by" TEXT,
    PRIMARY KEY ("procedure_event_id"),
    FOREIGN KEY ("subject_id") REFERENCES warehouse."dim_patient" ("subject_id"),
    FOREIGN KEY ("hadm_id") REFERENCES warehouse."fact_admission" ("hadm_id"),
    FOREIGN KEY ("stay_id") REFERENCES warehouse."fact_icu_stay_accumulating" ("stay_id"),
    FOREIGN KEY ("caregiver_id") REFERENCES warehouse."dim_caregiver" ("caregiver_id"),
    FOREIGN KEY ("item_id") REFERENCES warehouse."dim_chart_item" ("item_id"),
    FOREIGN KEY ("start_date_key") REFERENCES warehouse."dim_date" ("date_key"),
    FOREIGN KEY ("end_date_key") REFERENCES warehouse."dim_date" ("date_key"),
    FOREIGN KEY ("store_date_key") REFERENCES warehouse."dim_date" ("date_key")
);
COMMENT ON TABLE warehouse."fact_procedure_event" IS 'Procedures with a duration (e.g. ventilation, dialysis) performed during an ICU stay. Distinct from fact_procedure (billed ICD codes). Surrogate PK. UNIQUE INDEX ux_fact_procedure_event_grain on COALESCE(stay_id,-1), COALESCE(item_id,-1), COALESCE(start_time,epoch), COALESCE(end_time,epoch), COALESCE(order_id,-1) is the dedup grain etl_warehouse.py''s ON CONFLICT loader uses -- an expression index (not a plain UNIQUE) so a NULL order_id/end_time still collides correctly.';
CREATE UNIQUE INDEX IF NOT EXISTS "ux_fact_procedure_event_grain" ON warehouse."fact_procedure_event" (COALESCE("stay_id", -1), COALESCE("item_id", -1), COALESCE("start_time", TIMESTAMP 'epoch'), COALESCE("end_time", TIMESTAMP 'epoch'), COALESCE("order_id", -1));

CREATE TABLE IF NOT EXISTS warehouse."agg_admission_daily" (
    "date_key" INTEGER NOT NULL,
    "admission_type" TEXT NOT NULL,
    "admit_count" INTEGER,
    "discharge_count" INTEGER,
    "death_count" INTEGER,
    "created_ts" TIMESTAMP,
    "updated_ts" TIMESTAMP,
    PRIMARY KEY ("date_key", "admission_type"),
    FOREIGN KEY ("date_key") REFERENCES warehouse."dim_date" ("date_key")
);
COMMENT ON TABLE warehouse."agg_admission_daily" IS 'Daily admission/discharge/mortality counts per admission_type, rolled up from fact_admission.';

CREATE TABLE IF NOT EXISTS warehouse."agg_icu_fluid_balance_daily" (
    "date_key" INTEGER NOT NULL,
    "stay_id" BIGINT NOT NULL,
    "total_intake_ml" DOUBLE PRECISION,
    "total_output_ml" DOUBLE PRECISION,
    "net_balance_ml" DOUBLE PRECISION,
    "created_ts" TIMESTAMP,
    "updated_ts" TIMESTAMP,
    PRIMARY KEY ("date_key", "stay_id"),
    FOREIGN KEY ("date_key") REFERENCES warehouse."dim_date" ("date_key"),
    FOREIGN KEY ("stay_id") REFERENCES warehouse."fact_icu_stay_accumulating" ("stay_id")
);
COMMENT ON TABLE warehouse."agg_icu_fluid_balance_daily" IS 'Daily net ICU fluid balance per stay_id: total_intake_ml (fact_input_event, amount_uom=''ml'' only) minus total_output_ml (fact_output_event), rolled up.';

CREATE TABLE IF NOT EXISTS warehouse."agg_admission_monthly" (
    "year_month" INTEGER NOT NULL,
    "admission_type" TEXT NOT NULL,
    "admit_count" INTEGER,
    "discharge_count" INTEGER,
    "death_count" INTEGER,
    "created_ts" TIMESTAMP,
    "updated_ts" TIMESTAMP,
    PRIMARY KEY ("year_month", "admission_type")
);
COMMENT ON TABLE warehouse."agg_admission_monthly" IS 'Monthly admission/discharge/mortality counts per admission_type, rolled up from agg_admission_daily (not directly from fact_admission).';

CREATE TABLE IF NOT EXISTS warehouse."etl_process_log" (
    "log_id" BIGINT GENERATED ALWAYS AS IDENTITY,
    "full_run_id" BIGINT,
    "run_id" TEXT,
    "job_name" TEXT,
    "process_name" TEXT,
    "process_type" TEXT,
    "phase" TEXT,
    "dataset" TEXT,
    "start_ts" TIMESTAMP,
    "end_ts" TIMESTAMP,
    "execution_seconds" DOUBLE PRECISION,
    "dpu_seconds" DOUBLE PRECISION,
    "raw_row_count" BIGINT,
    "gold_row_count" BIGINT,
    "status" TEXT,
    "error_message" TEXT,
    "logged_ts" TIMESTAMP,
    PRIMARY KEY ("log_id")
);
COMMENT ON TABLE warehouse."etl_process_log" IS 'Append-only per-table timing/row-count log written by glue_jobs/glue_common.py''s log_timed_call() from inside each of the 4 Glue jobs'' (staging/dimensions/facts/aggregates) per-table loops -- the Postgres/coarser-job-grain counterpart to the lakehouse''s mimic4_db_business.etl_process_log, with the same columns. full_run_id is one integer shared by every row from a single `mimic-load glue run-all` invocation (computed once in cli.py via MAX(full_run_id)+1 and passed to each of the 4 independently-launched Glue jobs as --FULL_RUN_ID; a standalone single-step command such as `glue run-facts` gets its own fresh full_run_id). NOT safe against concurrent invocations, same caveat as the lakehouse. phase is the glue_run_all step name, letter-prefixed to align with the lakehouse''s A-J scheme: A:staging, B:dimensions, C:facts, D:aggregates -- coarser than process_type (dim/fact/agg/staging) the same way the lakehouse''s phase letters group multiple process_types together (the lakehouse splits facts/aggregates further into several letters since it runs each table as its own parallel Glue job; this pipeline runs facts/aggregates as one sequential per-table loop inside a single Glue job each, so one letter per stage is enough here). dpu_seconds is NULL at insert time (a table''s row is logged mid-job-run, before the job''s total DPUSeconds is known) and is backfilled by cli.py''s _run_glue_step for every row of a run_id once that Glue job run reaches a terminal state, via glue.get_job_run_dpu_seconds -- same after-the-fact-from-the-orchestrator pattern the lakehouse uses, just delayed by one step since a lakehouse job IS one table. raw_row_count is COUNT(*) of the table read (stg_<table> for staging; the warehouse table itself, pre-load, for dim/fact/agg upserts -- NULL where not meaningful, e.g. dim_date''s seed step). gold_row_count is COUNT(*) of the warehouse target table immediately after the load.';
