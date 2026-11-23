-- Auto-generated from mimic_datawarehouse.staging.STAGING_TABLE_COLUMNS.
-- Regenerate with: python scripts/export_staging_ddl.py
-- Do not edit by hand -- edit staging.py and regenerate instead.

CREATE SCHEMA IF NOT EXISTS "staging";

CREATE TABLE IF NOT EXISTS staging.stg_admissions (
    "subject_id" BIGINT,
    "hadm_id" BIGINT,
    "admittime" TEXT,
    "dischtime" TEXT,
    "deathtime" TEXT,
    "admission_type" TEXT,
    "admit_provider_id" TEXT,
    "admission_location" TEXT,
    "discharge_location" TEXT,
    "insurance" TEXT,
    "language" TEXT,
    "marital_status" TEXT,
    "race" TEXT,
    "edregtime" TEXT,
    "edouttime" TEXT,
    "hospital_expire_flag" BIGINT
);

CREATE TABLE IF NOT EXISTS staging.stg_caregiver (
    "caregiver_id" BIGINT
);

CREATE TABLE IF NOT EXISTS staging.stg_chartevents (
    "subject_id" BIGINT,
    "hadm_id" BIGINT,
    "stay_id" BIGINT,
    "caregiver_id" BIGINT,
    "charttime" TEXT,
    "storetime" TEXT,
    "itemid" BIGINT,
    "value" TEXT,
    "valuenum" DOUBLE PRECISION,
    "valueuom" TEXT,
    "warning" BIGINT
);

CREATE TABLE IF NOT EXISTS staging.stg_d_hcpcs (
    "code" TEXT,
    "category" TEXT,
    "long_description" TEXT,
    "short_description" TEXT
);

CREATE TABLE IF NOT EXISTS staging.stg_d_icd_diagnoses (
    "icd_code" TEXT,
    "icd_version" BIGINT,
    "long_title" TEXT
);

CREATE TABLE IF NOT EXISTS staging.stg_d_icd_procedures (
    "icd_code" TEXT,
    "icd_version" BIGINT,
    "long_title" TEXT
);

CREATE TABLE IF NOT EXISTS staging.stg_d_items (
    "itemid" BIGINT,
    "label" TEXT,
    "abbreviation" TEXT,
    "linksto" TEXT,
    "category" TEXT,
    "unitname" TEXT,
    "param_type" TEXT,
    "lownormalvalue" TEXT,
    "highnormalvalue" TEXT
);

CREATE TABLE IF NOT EXISTS staging.stg_d_labitems (
    "itemid" BIGINT,
    "label" TEXT,
    "fluid" TEXT,
    "category" TEXT
);

CREATE TABLE IF NOT EXISTS staging.stg_datetimeevents (
    "subject_id" BIGINT,
    "hadm_id" BIGINT,
    "stay_id" BIGINT,
    "caregiver_id" BIGINT,
    "charttime" TEXT,
    "storetime" TEXT,
    "itemid" BIGINT,
    "value" TEXT,
    "valueuom" TEXT,
    "warning" BIGINT
);

CREATE TABLE IF NOT EXISTS staging.stg_diagnoses_icd (
    "subject_id" BIGINT,
    "hadm_id" BIGINT,
    "seq_num" BIGINT,
    "icd_code" TEXT,
    "icd_version" BIGINT
);

CREATE TABLE IF NOT EXISTS staging.stg_drgcodes (
    "subject_id" BIGINT,
    "hadm_id" BIGINT,
    "drg_type" TEXT,
    "drg_code" BIGINT,
    "description" TEXT,
    "drg_severity" BIGINT,
    "drg_mortality" BIGINT
);

CREATE TABLE IF NOT EXISTS staging.stg_emar_detail (
    "subject_id" BIGINT,
    "emar_id" TEXT,
    "emar_seq" BIGINT,
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
    "infusion_rate_adjustment_amount" TEXT,
    "infusion_rate_unit" TEXT,
    "route" TEXT,
    "infusion_complete" TEXT,
    "completion_interval" TEXT,
    "new_iv_bag_hung" TEXT,
    "continued_infusion_in_other_location" TEXT,
    "restart_interval" TEXT,
    "side" TEXT,
    "site" TEXT,
    "non_formulary_visual_verification" TEXT
);

CREATE TABLE IF NOT EXISTS staging.stg_emar (
    "subject_id" BIGINT,
    "hadm_id" BIGINT,
    "emar_id" TEXT,
    "emar_seq" BIGINT,
    "poe_id" TEXT,
    "pharmacy_id" BIGINT,
    "enter_provider_id" TEXT,
    "charttime" TEXT,
    "medication" TEXT,
    "event_txt" TEXT,
    "scheduletime" TEXT,
    "storetime" TEXT
);

CREATE TABLE IF NOT EXISTS staging.stg_hcpcsevents (
    "subject_id" BIGINT,
    "hadm_id" BIGINT,
    "chartdate" TEXT,
    "hcpcs_cd" TEXT,
    "seq_num" BIGINT,
    "short_description" TEXT
);

CREATE TABLE IF NOT EXISTS staging.stg_icustays (
    "subject_id" BIGINT,
    "hadm_id" BIGINT,
    "stay_id" BIGINT,
    "first_careunit" TEXT,
    "last_careunit" TEXT,
    "intime" TEXT,
    "outtime" TEXT,
    "los" DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS staging.stg_ingredientevents (
    "subject_id" BIGINT,
    "hadm_id" BIGINT,
    "stay_id" BIGINT,
    "caregiver_id" BIGINT,
    "starttime" TEXT,
    "endtime" TEXT,
    "storetime" TEXT,
    "itemid" BIGINT,
    "amount" DOUBLE PRECISION,
    "amountuom" TEXT,
    "rate" DOUBLE PRECISION,
    "rateuom" TEXT,
    "orderid" BIGINT,
    "linkorderid" BIGINT,
    "statusdescription" TEXT,
    "originalamount" BIGINT,
    "originalrate" DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS staging.stg_inputevents (
    "subject_id" BIGINT,
    "hadm_id" BIGINT,
    "stay_id" BIGINT,
    "caregiver_id" BIGINT,
    "starttime" TEXT,
    "endtime" TEXT,
    "storetime" TEXT,
    "itemid" BIGINT,
    "amount" DOUBLE PRECISION,
    "amountuom" TEXT,
    "rate" DOUBLE PRECISION,
    "rateuom" TEXT,
    "orderid" BIGINT,
    "linkorderid" BIGINT,
    "ordercategoryname" TEXT,
    "secondaryordercategoryname" TEXT,
    "ordercomponenttypedescription" TEXT,
    "ordercategorydescription" TEXT,
    "patientweight" DOUBLE PRECISION,
    "totalamount" DOUBLE PRECISION,
    "totalamountuom" TEXT,
    "isopenbag" BIGINT,
    "continueinnextdept" BIGINT,
    "statusdescription" TEXT,
    "originalamount" DOUBLE PRECISION,
    "originalrate" DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS staging.stg_labevents (
    "labevent_id" BIGINT,
    "subject_id" BIGINT,
    "hadm_id" BIGINT,
    "specimen_id" BIGINT,
    "itemid" BIGINT,
    "order_provider_id" TEXT,
    "charttime" TEXT,
    "storetime" TEXT,
    "value" TEXT,
    "valuenum" DOUBLE PRECISION,
    "valueuom" TEXT,
    "ref_range_lower" DOUBLE PRECISION,
    "ref_range_upper" DOUBLE PRECISION,
    "flag" TEXT,
    "priority" TEXT,
    "comments" TEXT
);

CREATE TABLE IF NOT EXISTS staging.stg_microbiologyevents (
    "microevent_id" BIGINT,
    "subject_id" BIGINT,
    "hadm_id" BIGINT,
    "micro_specimen_id" BIGINT,
    "order_provider_id" TEXT,
    "chartdate" TEXT,
    "charttime" TEXT,
    "spec_itemid" BIGINT,
    "spec_type_desc" TEXT,
    "test_seq" BIGINT,
    "storedate" TEXT,
    "storetime" TEXT,
    "test_itemid" BIGINT,
    "test_name" TEXT,
    "org_itemid" BIGINT,
    "org_name" TEXT,
    "isolate_num" BIGINT,
    "quantity" TEXT,
    "ab_itemid" BIGINT,
    "ab_name" TEXT,
    "dilution_text" TEXT,
    "dilution_comparison" TEXT,
    "dilution_value" DOUBLE PRECISION,
    "interpretation" TEXT,
    "comments" TEXT
);

CREATE TABLE IF NOT EXISTS staging.stg_omr (
    "subject_id" BIGINT,
    "chartdate" TEXT,
    "seq_num" BIGINT,
    "result_name" TEXT,
    "result_value" TEXT
);

CREATE TABLE IF NOT EXISTS staging.stg_outputevents (
    "subject_id" BIGINT,
    "hadm_id" BIGINT,
    "stay_id" BIGINT,
    "caregiver_id" BIGINT,
    "charttime" TEXT,
    "storetime" TEXT,
    "itemid" BIGINT,
    "value" BIGINT,
    "valueuom" TEXT
);

CREATE TABLE IF NOT EXISTS staging.stg_patients (
    "subject_id" BIGINT,
    "gender" TEXT,
    "anchor_age" BIGINT,
    "anchor_year" BIGINT,
    "anchor_year_group" TEXT,
    "dod" TEXT
);

CREATE TABLE IF NOT EXISTS staging.stg_pharmacy (
    "subject_id" BIGINT,
    "hadm_id" BIGINT,
    "pharmacy_id" BIGINT,
    "poe_id" TEXT,
    "starttime" TEXT,
    "stoptime" TEXT,
    "medication" TEXT,
    "proc_type" TEXT,
    "status" TEXT,
    "entertime" TEXT,
    "verifiedtime" TEXT,
    "route" TEXT,
    "frequency" TEXT,
    "disp_sched" TEXT,
    "infusion_type" TEXT,
    "sliding_scale" TEXT,
    "lockout_interval" TEXT,
    "basal_rate" TEXT,
    "one_hr_max" TEXT,
    "doses_per_24_hrs" BIGINT,
    "duration" BIGINT,
    "duration_interval" TEXT,
    "expiration_value" BIGINT,
    "expiration_unit" TEXT,
    "expirationdate" TEXT,
    "dispensation" TEXT,
    "fill_quantity" TEXT
);

CREATE TABLE IF NOT EXISTS staging.stg_poe_detail (
    "poe_id" TEXT,
    "poe_seq" BIGINT,
    "subject_id" BIGINT,
    "field_name" TEXT,
    "field_value" TEXT
);

CREATE TABLE IF NOT EXISTS staging.stg_poe (
    "poe_id" TEXT,
    "poe_seq" BIGINT,
    "subject_id" BIGINT,
    "hadm_id" BIGINT,
    "ordertime" TEXT,
    "order_type" TEXT,
    "order_subtype" TEXT,
    "transaction_type" TEXT,
    "discontinue_of_poe_id" TEXT,
    "discontinued_by_poe_id" TEXT,
    "order_provider_id" TEXT,
    "order_status" TEXT
);

CREATE TABLE IF NOT EXISTS staging.stg_prescriptions (
    "subject_id" BIGINT,
    "hadm_id" BIGINT,
    "pharmacy_id" BIGINT,
    "poe_id" TEXT,
    "poe_seq" BIGINT,
    "order_provider_id" TEXT,
    "starttime" TEXT,
    "stoptime" TEXT,
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
    "doses_per_24_hrs" BIGINT,
    "route" TEXT
);

CREATE TABLE IF NOT EXISTS staging.stg_procedureevents (
    "subject_id" BIGINT,
    "hadm_id" BIGINT,
    "stay_id" BIGINT,
    "caregiver_id" BIGINT,
    "starttime" TEXT,
    "endtime" TEXT,
    "storetime" TEXT,
    "itemid" BIGINT,
    "value" DOUBLE PRECISION,
    "valueuom" TEXT,
    "location" TEXT,
    "locationcategory" TEXT,
    "orderid" BIGINT,
    "linkorderid" BIGINT,
    "ordercategoryname" TEXT,
    "ordercategorydescription" TEXT,
    "patientweight" DOUBLE PRECISION,
    "isopenbag" BIGINT,
    "continueinnextdept" BIGINT,
    "statusdescription" TEXT,
    "ORIGINALAMOUNT" DOUBLE PRECISION,
    "ORIGINALRATE" BIGINT
);

CREATE TABLE IF NOT EXISTS staging.stg_procedures_icd (
    "subject_id" BIGINT,
    "hadm_id" BIGINT,
    "seq_num" BIGINT,
    "chartdate" TEXT,
    "icd_code" TEXT,
    "icd_version" BIGINT
);

CREATE TABLE IF NOT EXISTS staging.stg_provider (
    "provider_id" TEXT
);

CREATE TABLE IF NOT EXISTS staging.stg_services (
    "subject_id" BIGINT,
    "hadm_id" BIGINT,
    "transfertime" TEXT,
    "prev_service" TEXT,
    "curr_service" TEXT
);

CREATE TABLE IF NOT EXISTS staging.stg_transfers (
    "subject_id" BIGINT,
    "hadm_id" BIGINT,
    "transfer_id" BIGINT,
    "eventtype" TEXT,
    "careunit" TEXT,
    "intime" TEXT,
    "outtime" TEXT
);
