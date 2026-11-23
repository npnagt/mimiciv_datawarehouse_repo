# MIMIC-IV Data Warehouse

**A research codebase that builds a traditional relational data warehouse for the MIMIC-IV
hospital database on Amazon RDS for PostgreSQL (Aurora PostgreSQL Serverless v2).**

## About this repository

This is a companion repository to the book *End-to-End Data Architecture for Multimodal Data*
(research edition) and its trade edition, *Multimodal Data Architecture for Intelligent
Enterprise*, by Narendra Nagtode. It covers the book's **data warehouse**
portion: a classic star schema of dimensions and facts with primary-key and foreign-key
constraints, loaded from S3 through a staging schema by AWS Glue jobs.

The warehouse serves as the baseline in the book's comparison of a traditional warehouse
with a lakehouse. The lakehouse is built from the same source data in the companion
repository `mimiciv_lakehouse_repo`, and this repository includes the matching measurement
scripts (cost, storage, and time to insight). Step-by-step build instructions are in
Appendix 9 of the research edition.

It is a **research codebase**, not a production product. The data comes from
**MIMIC-IV 3.1** on PhysioNet, which requires credentialed access under the PhysioNet data use
agreement, so **no MIMIC data is included in this repository**. The pipeline also runs on the
openly available MIMIC-IV demo dataset.

## What the pipeline does

Builds an AWS-hosted data warehouse for MIMIC-IV (the full 3.1 dataset or the openly available demo):
raw CSV.gz files -> S3 -> Aurora PostgreSQL Serverless v2 (`mimic4rds`), staged
into a `staging` schema ahead of transformation into the warehouse model.

## Layout

```
data/raw/mimic-iv-clinical-database-demo-2.2/   Source dataset (hosp/, icu/ subfolders of *.csv.gz)
src/mimic_datawarehouse/
    config.py       Settings loaded from environment / .env (DB target + AWS provisioning)
    db.py           SQLAlchemy engine + schema creation
    tables.py       Discovers (schema, table, csv_path) from the data directory
    loader.py       Streams one CSV.gz into RDS in chunks (pandas -> to_sql path)
    staging.py      stg_<table> DDL in the `staging` schema, mirroring the raw CSV columns
    warehouse.py    dim_/fact_ DDL in the `warehouse` schema, with PK/FK constraints
    etl.py          Truncate-and-reload each stg_<table> from its raw CSV.gz
    etl_warehouse.py  Load warehouse dims (upsert) and facts (insert-only) from staging
    seed_date_dim.py  Generates + upserts warehouse.dim_date (calendar spine)
    cli.py          `mimic-load` command group
    aws/
        s3.py       Create the raw-data bucket, upload the local dataset
        iam.py      IAM role Aurora assumes to read that bucket (aws_s3 import)
        aurora.py   Aurora PostgreSQL Serverless v2 cluster: create / wait / attach role / delete
        glue.py     Glue IAM role / JDBC connection / job provisioning + start/wait job runs
glue_jobs/          PySpark scripts run as AWS Glue jobs -- production equivalent of
                     etl.py / etl_warehouse.py, see "Running the ETL as AWS Glue jobs" below
    glue_common.py          Shared JDBC + upsert/insert-only-merge helpers
    staging_load.py         S3 raw CSV.gz -> staging.stg_* (Glue job 1)
    warehouse_dimensions.py staging.stg_* -> warehouse.dim_* (Glue job 2)
    warehouse_facts.py      staging.stg_* -> warehouse.fact_* (Glue job 3)
    warehouse_aggregates.py warehouse.fact_* -> warehouse.agg_* (Glue job 4)
scripts/
    run_end_to_end.py         Master wrapper: provisioning + Glue deploy/bootstrap/seed/load, start to finish
    build_datawarehouse.py    End-to-end AWS provisioning (bucket, IAM role, Aurora cluster)
    teardown_datawarehouse.py Deletes cluster/IAM role/subnet group/Glue jobs -- everything except the S3 bucket
    export_staging_ddl.py    Regenerates sql/staging_tables.sql from staging.py
    export_warehouse_ddl.py Regenerates sql/warehouse_tables.sql from warehouse.py
    measure_warehouse_size.py   On-disk storage per table / category (pg_total_relation_size + pgstattuple),
                                lined up with the lakehouse storage report -- proposition P3
    measure_aws_resources.py    Aurora ACU-hours + Glue DPU-seconds + S3 + CloudWatch -- the "Aurora Pipeline
                                Manifest"; lakehouse-vs-warehouse cost comparison. --by-run reads
                                warehouse.etl_process_log instead: detailed per-table resource/timing data per
                                full_run_id plus mean/median/stdev across every run found, written to
                                docs/cost/<demo|full>/runs_<UTC>.{json,md}.
    measure_time_to_insight_pg.py  The lakehouse's 19-query time-to-insight battery translated to Postgres,
                                timed with EXPLAIN (ANALYZE, TIMING) + client RTT -- proposition P6. T4 has no
                                obt_* table (rebuilt inline); T5/T6 have no path (no NLP tables). Writes
                                docs/tti/tti_pg_<UTC>.{json,md}
sql/
    staging_tables.sql       Plain-SQL export of the staging DDL (auto-generated, read-only)
    warehouse_tables.sql     Plain-SQL export of the warehouse DDL (auto-generated, read-only)
tests/                                          pytest suite
```

Each top-level folder in the dataset (`hosp`, `icu`) becomes a Postgres schema
of the same name; each `*.csv.gz` file becomes a table in that schema.

## The warehouse (dimensional model)

`warehouse.py` translates the lakehouse's Athena/Iceberg Gold DDL
(`mimic4_db_business`: 9 dimensions + 23 facts + 3 aggregates) into Postgres,
in a `warehouse` schema. Two deliberate differences from the source:

- **No Iceberg partitioning / S3 properties** -- storage-layer concerns with no
  Postgres equivalent, and unnecessary at this dataset's volume anyway.
- **Primary/foreign keys are added.** Athena enforces neither; Postgres does,
  which is arguably the point of using an RDBMS warehouse. Each PK's grain was
  verified against the real CSVs (not assumed) -- see the module docstring and
  `tests/test_warehouse.py` for the uniqueness checks, including the specific
  tables that get a surrogate `GENERATED ALWAYS AS IDENTITY` key because no
  natural one is reliably unique or non-null in the actual data (e.g.
  `fact_medication_administration_mini_detail`: its only natural-key
  candidate, `parent_field_ordinal`, is NULL in half the real rows). Most of
  those surrogate-key tables *do* have a reliably-unique composite natural key
  -- just not usable as the PK itself -- and get it as a `UNIQUE` constraint
  instead (`WAREHOUSE_TABLES[...]["unique_keys"]`), the same grain
  `etl_warehouse.py`'s anti-join dedup already uses. See "Populating the
  warehouse" below for the one table that doesn't (`fact_chart_observation`).

Because FKs are enforced, tables must be created in dependency order
(`WAREHOUSE_TABLE_ORDER` -- dimensions, then facts in the order that respects
inter-fact references like `fact_diagnosis` -> `fact_admission`) and populated
in that same order later. Create it with:

```
mimic-load create-warehouse
```

## Setup

```
pip install -e ".[dev]"
cp .env.example .env
```

`.env` has two groups of settings (see comments in `.env.example`):

- **RDS_\*** -- connection details for a database that already exists (used by
  `mimic-load load-raw` / `create-staging`).
- **AWS provisioning** (`S3_BUCKET_NAME`, `AURORA_*`, `DB_SUBNET_GROUP_NAME`,
  `VPC_SECURITY_GROUP_IDS`, ...) -- used to *create* that infrastructure in the
  first place. AWS credentials come from your normal credential chain
  (`aws configure`, `AWS_PROFILE`, or an instance role).

### Choosing the dataset (`--dataset` / `MIMIC_DATASET`)

The pipeline runs against either the **demo** dataset (100 patients,
`data/raw/mimic-iv-clinical-database-demo-2.2`) or the **full** MIMIC-IV 3.1 download
(`data/raw/mimic-iv-3.1-fulldataset`). Set `MIMIC_DATASET=demodataset|fulldataset` in
`.env`, or pass it per command:

```
cd /c/MIMICIV/gitmimic/mimic_datawarehouse_repo
LOGFILE="logs/run_end_to_end_$(date +%Y%m%d_%H%M%S).log"
./.venv/Scripts/python.exe scripts/run_end_to_end.py --dataset fulldataset 2>&1 | tee "$LOGFILE"


mimic-load --dataset fulldataset glue deploy
python scripts/run_end_to_end.py --dataset fulldataset 
python scripts/build_datawarehouse.py --dataset fulldataset
```

Each dataset resolves (via `DatasetProfile` in `src/mimic_datawarehouse/config.py`) to a
fully isolated resource set, so demo and full can be built side by side:

| | `demodataset` | `fulldataset` |
|---|---|---|
| local CSVs (`MIMIC_DATA_DIR`) | `data/raw/mimic-iv-clinical-database-demo-2.2` | `data/raw/mimic-iv-3.1-fulldataset` |
| Aurora database (`RDS_DATABASE` / `AURORA_DB_NAME`) | `mimic4rds_demo` | `mimic4rds_full` |
| S3 source bucket (`S3_BUCKET_NAME`) | `mimic4-dw-sourcefiles-demo` | `mimic4-dw-sourcefiles-full` |
| S3-import IAM role (`IAM_ROLE_NAME`) | `mimic4-aurora-s3-import-role-demo` | `…-full` |
| Glue job prefix (`GLUE_JOB_NAME_PREFIX`) | `mimic4-dw-etl-demo` | `mimic4-dw-etl-full` |
| Glue role / connection | `…-glue-role-demo` / `…-aurora-connection-demo` | `…-full` |

Shared: the **Aurora cluster** itself (`AURORA_CLUSTER_ID=mimic4rds`) hosts both databases,
and AWS credentials/region. Any single name above still overrides its per-dataset default
if you set that env var explicitly. `staging` / `warehouse` schema names are unchanged --
isolation is at the database level. The `--dataset` flag on `mimic-load` is a group option,
so it goes *before* the subcommand.

#### Per-table row cap (`--row-cap`, default 1,000,000)

The staging load truncates any raw table with **more than a million rows** down to a
million before it lands in Aurora — the same rule the companion lakehouse repo applies to
its Gold fact tables, so the two architectures stay row-for-row comparable and a
`fulldataset` load doesn't run up Aurora storage/compute cost on tables like
`chartevents` (hundreds of millions of rows). Tables already under the cap load whole, so
on `demodataset` the cap never bites.

It applies on every staging path — `mimic-load load-staging`, `mimic-load glue run-staging`,
`mimic-load glue run-all`, and `scripts/run_end_to_end.py` — each taking `--row-cap N`
(`--row-cap 0` disables it and loads everything). The Glue job reads it as the `--ROW_CAP`
argument. The warehouse `fact_*` tables inherit the cap from their capped staging source
(they aren't capped a second time), so a capped fact ends up at ≈1,000,000 rows minus
whatever its own dedup/FK logic drops.

### AWS resource tagging

Every AWS resource this repo provisions — the S3 source bucket, the S3-import and Glue IAM
roles, the Aurora cluster + instance, the Glue jobs and JDBC connection — is tagged
**`Project=DWHS`** on creation (override with `PROJECT_TAG` in `.env`). The data-lakehouse
repo tags its resources `Project=LAKEHSE`. Activate the `Project` key under **Billing → Cost
allocation tags** to split spend by repo in Cost Explorer.

## Operations Runbook

Three common operational tasks as copy-paste command sequences. This section is
written to stand on its own (e.g. to lift into separate ops documentation) --
the sections further down go into more design rationale on *why* each piece
behaves the way it does.

### 1. Tearing down the environment

Deletes the Aurora cluster (and its instance), the IAM role, and the DB subnet
group -- everything **except** the S3 bucket (`mimic4-dw-sourcefiles` by
default) and the raw data already uploaded to it, which is left alone.

Why keep the bucket: it costs essentially nothing to leave sitting in S3,
whereas the Aurora cluster is the expensive, continuously-billed piece.
Tearing down is about stopping that cost between work sessions without having
to re-upload the dataset on the next build.

```
python scripts/teardown_datawarehouse.py                              # dry run -- prints what would be deleted, does nothing
python scripts/teardown_datawarehouse.py --yes                        # delete, with a final snapshot of the cluster
python scripts/teardown_datawarehouse.py --yes --skip-final-snapshot  # delete, no snapshot -- faster/free, unrecoverable
```

It's a dry run unless you pass `--yes`, specifically so this can't be
triggered by accident. A final snapshot (the default) means the loaded
staging + warehouse data can be restored later if needed -- it costs a small
ongoing storage fee until you delete the snapshot yourself (this script
doesn't do that part for you). Add `--keep-subnet-group` if you want to reuse
the same subnet group on the next build instead of recreating it.

### 2. Building the environment again end to end

**One-shot version:** `python scripts/run_end_to_end.py` runs every step below
(provisioning through the Glue path's full pipeline) in a single script --
provisions the infra, resolves the cluster endpoint and writes it into `.env`
for you, then deploys/bootstraps/seeds/loads via Glue. Pass
`--skip-provisioning` to skip straight to the load steps against an
already-provisioned cluster. The manual, step-by-step version below is the
same sequence broken out, useful if you want to run/inspect each piece
yourself or use the local (non-Glue) path instead.

From a torn-down (or brand new) state, in order. Steps 0-2 are the same either
way; step 3 onward branches into the local path (runs the ETL in-process,
useful for dev) or the Glue path (runs it as managed AWS Glue jobs -- the
verified, production path; see "Running the ETL as AWS Glue jobs" below for
the full command reference).

```
# 0. One-time local setup
pip install -e ".[dev]"
cp .env.example .env      # fill in AWS_REGION, S3_BUCKET_NAME, AURORA_* etc.

# 1. Provision AWS infrastructure: S3 bucket, IAM role, Aurora cluster
python scripts/build_datawarehouse.py

# 2. Point .env at the new cluster: copy the "Cluster endpoint" the previous
#    command printed into RDS_HOST. If the account fell back to Aurora's
#    "express configuration" (no static password -- the script's output will
#    say so), also set RDS_AUTH_MODE=iam in .env. If it's a VPC-attached
#    cluster instead, also set GLUE_SUBNET_ID / GLUE_AVAILABILITY_ZONE if
#    you're going to use the Glue path below.
```

**3a. Local path** -- runs the ETL in-process against Postgres directly:

```
mimic-load create-database     # only needed if express configuration was used
mimic-load create-staging      # staging schema + one stg_<table> per raw CSV
mimic-load create-warehouse    # warehouse schema + every dim_/fact_/agg_ table

mimic-load load-staging        # truncate + reload every stg_<table> (>1M-row tables capped to 1M; --row-cap)
mimic-load seed-date-dim       # warehouse.dim_date, 2100-01-01..2230-12-31 by default
mimic-load load-warehouse      # dims (upsert) -> facts (insert-only) -> aggregates (upsert)
```

**3b. Glue path** -- runs the ETL as managed AWS Glue jobs (under 10 minutes
end to end on this dataset: staging ~3 min, dimensions ~1.5 min, facts ~3 min,
aggregates ~1.5 min):

```
mimic-load glue deploy          # scripts + DDL to S3, IAM role, JDBC connection, 5 Glue jobs
mimic-load glue run-bootstrap   # creates the staging/warehouse schemas and tables

mimic-load seed-date-dim        # warehouse.dim_date -- local, one-time (facts join it)
mimic-load glue run-all         # staging -> dimensions -> facts -> aggregates, via Glue
```

Every step in both paths is safe to rerun: `build_datawarehouse.py`, every
`create-*` command, and `glue deploy`/`run-bootstrap` are idempotent (they
skip or reuse what already exists rather than erroring), and the load
commands' upsert/insert-only/truncate-and-reload behavior means re-running the
whole sequence again always converges to the same end state without
duplicating anything. If the cluster's endpoint changed (e.g. after a
teardown + rebuild), remember to update `RDS_HOST` -- the old endpoint stops
resolving once that cluster is deleted.

**Job timing log.** Every table each of the 4 Glue jobs loads gets a row in
`warehouse.etl_process_log` -- the same column set as the lakehouse repo's
`mimic4_db_business.etl_process_log` (full_run_id, run_id, job_name,
process_name, process_type, phase, dataset, start_ts, end_ts,
execution_seconds, dpu_seconds, raw_row_count, gold_row_count, status,
error_message, logged_ts; `log_id` is the one addition, a surrogate PK
Postgres requires). `full_run_id` is one integer shared by every row from a
single `glue run-all` invocation (`MAX(full_run_id)+1`, computed once in
`cli.py`); a standalone step like `glue run-facts` on its own gets its own
fresh id. `phase` is the `glue_run_all` step name, letter-prefixed to align
with the lakehouse repo's A-J phase scheme: `A:staging`, `B:dimensions`,
`C:facts`, `D:aggregates` (this pipeline runs facts/aggregates as one
sequential per-table loop per Glue job, unlike the lakehouse's one-job-per-table
parallel jobs, so one letter per stage is enough here). `dpu_seconds` starts NULL (a table's row is
logged mid-job-run, before the job's total DPUSeconds is known) and is
backfilled by `cli.py` for every row of that run once the Glue job run
reaches a terminal state. Query it directly:

```sql
SELECT process_type, process_name, execution_seconds, gold_row_count, status
FROM warehouse.etl_process_log
WHERE full_run_id = (SELECT max(full_run_id) FROM warehouse.etl_process_log)
ORDER BY start_ts;
```

or use `scripts/measure_aws_resources.py --by-run` for the no-SQL alternative:
per-`full_run_id` detail plus mean/median/stdev across every run found.

### 3. Running the ETL incrementally when new raw files arrive

When a raw CSV.gz under `data/raw/mimic-iv-clinical-database-demo-2.2/{hosp,icu}/`
is replaced with an updated version (e.g. more rows appended) or a table you
hadn't loaded before is added, propagate it through with:

```
mimic-load load-staging                    # refresh every staging table from the current files
mimic-load load-staging --table admissions # or: just the table(s) that actually changed
mimic-load load-warehouse                  # propagate into dims/facts/aggregates
```

This is genuinely incremental at the warehouse layer, by design:

- **Staging** is a full truncate-and-reload of whichever table(s) you target --
  cheap at this dataset's size, and it means staging always exactly reflects
  the current file on disk rather than something that could drift out of
  sync. Use `--table`/`--module` to limit the reload to just the file(s) you
  changed if you don't want to pay the cost of reloading tables that didn't.
- **Dimensions** are upserted -- a business key that already existed gets its
  attributes refreshed in place; a new one is inserted. Rerunning after a
  staging refresh is exactly how a changed dimension attribute (e.g. a
  patient's `anchor_year_group`) gets picked up.
- **Facts** are insert-only -- rows already loaded are never touched or
  duplicated; only rows not yet present in the fact table get added
  (`ON CONFLICT ... DO NOTHING`, or a `NOT EXISTS` check for the handful of
  tables with no natural key). This is what makes rerunning `load-warehouse`
  safe and cheap even when only a few new rows landed: existing rows are a
  no-op, so the statement's cost is dominated by the new rows, not the whole
  table.
- **Aggregates** (`agg_admission_daily`, `agg_icu_fluid_balance_daily`,
  `agg_admission_monthly`) are fully recomputed and upserted from the current
  state of the fact tables, so they're always consistent with whatever facts
  exist at refresh time without you needing to figure out which calendar days
  were actually affected. `load-warehouse` refreshes them automatically after
  facts; run `mimic-load refresh-aggregates` standalone if you only need to
  recompute the rollups without reloading anything else.

You don't need to re-seed `dim_date` unless the new data's timestamps fall
outside the currently-seeded range (`2100-01-01`..`2230-12-31` by default) --
extend it with `mimic-load seed-date-dim --start <date> --end <date>` if so.

## Provisioning the AWS infrastructure

The cluster is created as **Aurora Serverless v2** (auto-scales `AURORA_MIN_ACU`
-`AURORA_MAX_ACU`, default 0.5-4). `aws.aurora.create_serverless_cluster` first
tries a standard, VPC-attached cluster with **`PubliclyAccessible=False`** --
reachable only from inside the VPC named by `DB_SUBNET_GROUP_NAME` /
`VPC_SECURITY_GROUP_IDS`, which you must already have set up (VPN, bastion,
peered VPC, etc.). Nothing in this repo opens a security group to the internet.

**Some AWS account tiers (new/free-plan accounts) reject that** with
`FreeTierRestrictionError` and require Aurora's *express configuration*
instead -- which forgoes VPC placement, an initial database, and a static
password in favor of AWS-managed networking (`VPCNetworkingEnabled: false`,
`InternetAccessGatewayEnabled: true`) and **IAM database authentication only**.
`create_serverless_cluster` falls back to this automatically and logs a
warning when it happens -- you'll see the cluster is reachable directly (no
VPN/bastion needed) but secured by short-lived IAM auth tokens instead of
network isolation. If that's the account you're on, set `RDS_AUTH_MODE=iam` in
`.env`; `db.make_engine()` then generates a fresh token per connection instead
of using `RDS_PASSWORD` (your AWS credentials need `rds-db:connect` on the DB
user). Also, because express configuration can't create an initial database,
run `mimic-load create-database` once the cluster is up, before
`create-staging`.

Run the whole thing end to end (creates real, billed AWS resources -- review
`scripts/build_datawarehouse.py` first):

```
python scripts/build_datawarehouse.py
```

This: creates the `mimic4-dw-sourcefiles` S3 bucket, uploads the local dataset
to it (preserving the `hosp/`/`icu/` layout as key prefixes), creates the IAM
role Aurora needs to read that bucket, creates the `mimic4rds` cluster, waits
for it to become available, and attaches the IAM role for S3 import.

Or run the same steps individually via the CLI (useful for retries -- every
step is idempotent):

```
mimic-load aws create-bucket
mimic-load aws upload-data
mimic-load aws create-iam-role
mimic-load aws create-cluster --wait
mimic-load aws attach-s3-role
```

To tear this down later (deletes the cluster/IAM role/subnet group, keeps the
S3 bucket), see "Operations Runbook" above.

Once the cluster is available, set `RDS_HOST` (and friends) in `.env` to its
endpoint -- from a host that can actually reach it -- then create the database
and the staging tables:

```
mimic-load create-database   # only needed if the account fell back to express configuration
mimic-load create-staging    # staging schema + one stg_<table> per raw CSV
mimic-load create-warehouse  # warehouse schema + every dim_/fact_ table
```

## Running the ETL as AWS Glue jobs

`mimic-load load-staging` / `load-warehouse` (below) run the ETL in-process, locally --
good for dev against a reachable Postgres. In production, the same logic runs as 5
managed **AWS Glue** jobs instead (`glue_jobs/`: a `pythonshell` schema-bootstrap job
plus 4 PySpark jobs), orchestrated by `mimic-load glue ...`, which starts each job run
via `boto3` and polls it to completion. See `glue_jobs/glue_common.py`'s docstring for
how the jobs preserve the `ON CONFLICT` upsert semantics `etl_warehouse.py` runs today
(the insert-only anti-join facts are a deliberate, documented *exception* -- see
"Behavior differences from the local path" below).

**Verified against a real Glue/Aurora run** -- `glue run-all` has loaded the full
dataset end to end (staging -> dimensions -> 23 facts -> 3 aggregates); on `fulldataset`
the staging load caps any >1M-row table at 1,000,000 (see "Per-table row cap" above).
Still worth a `glue run-staging --table patients` dry run (smallest table) the first time
you point this at a *new* cluster, since every environment's networking/IAM setup differs.

**Connectivity to Aurora has two modes, picked automatically from your `.env`:**
- **VPC-attached cluster** (standard `mimic-load aws create-cluster`, `RDS_AUTH_MODE=password`):
  set `GLUE_SUBNET_ID` / `GLUE_AVAILABILITY_ZONE` to a subnet from `DB_SUBNET_GROUP_NAME`
  -- Glue jobs run on ENIs placed there, same as a VPN/bastion would reach it.
- **Express-configuration cluster** (the fallback documented above,
  `RDS_AUTH_MODE=iam`, no VPC, internet-reachable): leave `GLUE_SUBNET_ID` unset. Jobs
  run in Glue's default (non-VPC) networking and each generates its own short-lived RDS
  IAM auth token at runtime (same mechanism `db.py`'s `_make_iam_engine` already uses
  locally) -- `mimic-load glue deploy` grants the Glue role `rds-db:connect` on
  `RDS_USER` automatically in this mode. **This is the path a free/low-tier AWS account
  needs** -- `aurora.create_serverless_cluster`'s standard (VPC-attached) path is
  rejected outright on those accounts (`FreeTierRestrictionError`), so there's no way to
  get a VPC-attached cluster to point `GLUE_SUBNET_ID` at without upgrading the account.

### Deploy

Uploads `glue_jobs/*.py` + the DDL exports in `sql/` to S3, creates/updates the IAM
role, (if `GLUE_SUBNET_ID` is set) the JDBC connection, and the 5 Glue jobs --
idempotent, safe to re-run any time after editing a script:

```
mimic-load glue deploy
```

### Bootstrap (once per cluster)

Creates the staging/warehouse schemas and tables (the other 4 jobs load data into
tables this one creates). Entirely `CREATE SCHEMA/TABLE IF NOT EXISTS`, so it's safe to
re-run any time -- e.g. after a DDL change in `staging.py`/`warehouse.py`:

```
mimic-load glue run-bootstrap
```

Two extra flags for reconciling/recovering a cluster that already has tables on it:

```
# Drop and recreate specific tables -- for a DDL change CREATE TABLE IF NOT EXISTS can't
# apply on its own (e.g. a new UNIQUE constraint). Data in dropped tables is lost.
mimic-load glue run-bootstrap --drop-tables "warehouse.fact_prescription,warehouse.fact_datetime_event"

# Diagnose + clear zombie Postgres sessions -- see "Troubleshooting" below. Run with no
# flags first to see the "other sessions" / "locks held" diagnostic output (always
# printed) and find candidate pids.
mimic-load glue run-bootstrap --terminate-pids "74908,78102"
```

### Full pipeline

Runs in dependency order, stops at the first job run that doesn't succeed. Run
`mimic-load seed-date-dim` first, same requirement as `load-warehouse` (facts join
`dim_date` via their role-playing `*_date_key` columns):

```
mimic-load seed-date-dim
mimic-load glue run-all                 # >1M-row staging tables capped to 1M (--row-cap 0 to disable)
```

### One step (or one table) at a time

`--no-wait` starts the run and returns immediately instead of polling to completion:

```
mimic-load glue run-staging --table patients
mimic-load glue run-staging --row-cap 0        # load every row of every table
mimic-load glue run-dimensions
mimic-load glue run-facts --table fact_admission
mimic-load glue run-aggregates
mimic-load glue run-facts --no-wait
```

### Behavior differences from the local path

- **Insert-only facts dedup on plain `=`, not `IS NOT DISTINCT FROM`.**
  `etl_warehouse.py`'s anti-join facts (the ones with no natural key -- chart/datetime/
  input/ingredient/output/procedure events, prescriptions, emar mini-detail) use
  `IS NOT DISTINCT FROM` so a NULL in the dedup key still matches an existing NULL on
  re-run. Glue's `insert_only_with_anti_join` (`glue_common.py`) uses `=` instead: `IS
  NOT DISTINCT FROM` can't use a hash or merge join, which forced Postgres into a
  Nested Loop Anti Join on a real run (`fact_chart_observation`: EXPLAIN showed an
  estimated cost of **13.1 billion**; `=` dropped that to ~249k and the merge from
  hours to under 2 seconds). The tradeoff: a row that's NULL across *every* column of
  its dedup key won't be recognized as already-loaded on a second run of the same
  source data. No data corruption -- at most an extra duplicate row for that narrow case.
- **Every SQL statement has a 5-minute `statement_timeout`** (`glue_common.STATEMENT_TIMEOUT_MS`,
  duplicated in `schema_bootstrap.py`) plus a 30s `connect_timeout`, and every Glue job
  has its own Timeout (60 min pythonshell / 120 min glueetl) as a backstop. A query that
  needs longer fails fast with a clear error instead of hanging silently.
- **Spark UI is enabled** on the 4 PySpark jobs (event logs at
  `s3://<bucket>/spark-logs/`, viewable via Glue Studio's job-run "Spark UI" tab) for
  stage/task-level visibility beyond what `glue_common.py`'s phase-by-phase `print()`
  logging gives you.

### Troubleshooting: a run hangs or times out

Stopping a Glue job run (`batch_stop_job_run`, or the AWS console's "Stop") kills the
Spark/Python driver -- it does **not** necessarily cancel the query its Postgres backend
was still executing server-side. A real run left a 3.8-hour-old `INSERT` running after
its Glue job was stopped, which then blocked every subsequent bootstrap/ANALYZE attempt
behind it in a lock queue (each new attempt piling up behind the last, failing at
seemingly random, unrelated steps). If a run times out or hangs with no clear cause:

```
mimic-load glue run-bootstrap
```

reads as: connect, print every other session on the database (pid, state, xact_age,
query) and every lock any of them holds on `staging.*`/`warehouse.*` tables, *then*
apply DDL. An old `active` session with a large `xact_age` (or any row in the "locks
held" output) is a zombie from a stopped run. Terminate it and retry:

```
mimic-load glue run-bootstrap --terminate-pids "<pid>,<pid>"
```

## Loading the staging tables

`mimic-load load-staging` TRUNCATEs each `staging.stg_<table>` and reloads it
from the matching raw CSV.gz -- safe to rerun, always reflects the current
file contents rather than accumulating duplicates on repeated runs. Unlike
`mimic-load load-raw` (below), it never alters the staging tables' structure --
they already have fixed types (and eventually PK/FK constraints), so rows are
appended into the existing schema rather than pandas inferring/replacing it.

```
mimic-load load-staging                          # every table
mimic-load load-staging --module hosp             # just the hosp/ tables
mimic-load load-staging --table patients          # just one table
mimic-load load-staging --row-cap 0               # don't cap >1M-row tables (default cap: 1,000,000)
```

By default any raw table with more than 1,000,000 rows is truncated to 1,000,000 on load
(`--row-cap`; see "Per-table row cap" under *Choosing the dataset*) -- once the cap is
reached the rest of the source CSV isn't read. Pass `--row-cap 0` to load every row.

Column types in `staging.py` were verified against the real CSVs, not just
copied from the source Athena DDL you get from `mimic-load aws ...` -- three
columns the source declared as numeric actually hold non-numeric values in
real MIMIC-IV data and would fail on load otherwise: `d_icd_procedures.icd_code`
(ICD-10-PCS codes are alphanumeric, e.g. `"001U3J6"`), `inputevents.totalamount`
(has fractional values), and `prescriptions.form_val_disp` (can hold a dose
*range* like `"1-2"` for variable/PRN dosing, so it's `TEXT`, not numeric at
all). `tests/test_staging.py` asserts every declared `BIGINT`/`DOUBLE PRECISION`
column actually parses cleanly against the real data, so a similar mistake
elsewhere fails the test suite instead of failing mid-load.

`load_staging_table` also forces every `TEXT`-typed column to be read as a raw
string (`dtype=str`), regardless of what its values look like. Without this,
pandas' own dtype inference (independent of our schema) upcasts a TEXT column
to float64 if it happens to look numeric but has any missing value --
`d_hcpcs.category` silently became `"1.0"` instead of `"1"` before this fix.
`test_text_columns_are_not_silently_upcast_to_float_by_pandas` guards it.

## Populating the warehouse

`warehouse.py`'s dim/fact structure is populated from staging by two ETL
paths, following the exact source-to-target mapping in
`mimic_iv_raw_to_gold_mapping_v2.xlsx` (column renames, role-playing
`*_date_key` derivations, and every calculated-column formula came from
there, not guesswork):

```
mimic-load seed-date-dim     # warehouse.dim_date: one row per day, 2100-01-01..2230-12-31 by default
mimic-load load-warehouse    # every dim (upsert), then every fact (insert-only), in FK order
```

**Dimensions are upserted** (`INSERT ... ON CONFLICT (natural_key) DO UPDATE`)
-- an existing business key gets its attributes refreshed in place.
**Facts are insert-only** -- an existing fact row is never touched; only
rows not yet present get added (`ON CONFLICT ... DO NOTHING`, or a
`NOT EXISTS` anti-join for the handful of tables with no natural key, see
below). Window-function calculated columns (`is_readmission_flag`,
`days_since_prior_discharge`, `is_icu_readmission_flag`) are computed over
the *entire* staging table each run -- correct because staging is always a
full truncate-and-reload snapshot (see above), not an incremental extract --
and only the rows genuinely new to the fact table are then inserted.

`seed_date_dim` upserts rather than truncates: fact tables hold FKs into
`dim_date`, and truncating a referenced table either fails or needs CASCADE.
A handful of real timestamps fall outside the seeded range (3 emar rows
predate 2100); rather than fail those rows' FK entirely, their `*_date_key`
is clamped to `NULL` and the rest of the row still loads.

Two real, bounded data-completeness gaps in MIMIC-IV itself (not ETL bugs --
verified against the raw CSVs) needed the same treatment: a small fraction of
`emar`/`pharmacy`/`prescriptions` rows reference a `poe_id` or `pharmacy_id`
that doesn't exist in `poe.csv`/`pharmacy.csv` (up to ~3% for `emar.poe_id`).
Rather than let one bad reference fail an entire batch insert, those columns
are resolved through a `LEFT JOIN` to the referenced fact table, so an
unmatched reference becomes `NULL` instead of violating the FK.

Tables with no reliable natural key (`fact_prescription`,
`fact_medication_administration_mini_detail`, and the ICU event-grain tables)
use a practical composite -- e.g. `(stay_id, item_id, chart_time, store_time)`
-- as a `NOT EXISTS` dedup check instead of `ON CONFLICT`. For 7 of these 8
tables that composite is genuinely unique in the real data (verified against
the raw CSVs, not assumed -- see `tests/test_warehouse.py`), so `warehouse.py`
backs it with a real `UNIQUE` constraint (and its own index) rather than just
an ordinary supporting index. The one exception is **`fact_chart_observation`**:
its equivalent composite actually collides on ~12% of real
`chartevents.csv.gz` rows, so a `UNIQUE` constraint there would reject
legitimate rows on every load -- it keeps a plain (non-unique) supporting
index instead, same as before.

### Aggregates

`agg_admission_daily`, `agg_icu_fluid_balance_daily`, and
`agg_admission_monthly` are rollups **over the already-loaded fact tables**
(not staging), refreshed with:

```
mimic-load refresh-aggregates   # standalone -- recompute after facts change
mimic-load load-warehouse       # also runs this automatically, after facts
```

Unlike facts, aggregates are **upserted** (`ON CONFLICT ... DO UPDATE`) on
their natural key (e.g. `(date_key, admission_type)`) -- a rollup's whole
point is to reflect the current state of its source facts, not accumulate
history. `agg_admission_monthly` rolls up **from `agg_admission_daily`**, not
directly from `fact_admission` (matching the source design's "coarser rolls
up from finer" principle), so it always runs after the daily refresh.
`agg_icu_fluid_balance_daily` sums `fact_input_event.amount` only where
`amount_uom = 'ml'` (that column also carries medication/electrolyte doses in
mg/mcg/units/mEq/mmol/grams) and deliberately excludes `fact_ingredient_event`
(a child-grain decomposition of the same input amount -- summing it too would
double-count intake).

The source DDL describes these as incrementally, partition-scoped refreshed
(a watermark table, `etl_control`, tracking only the calendar days touched
since the last run). That mechanism isn't implemented here -- each refresh
recomputes the full rollup via `GROUP BY` over the fact tables and upserts --
which is correct but doesn't scale the way the source's partition-scoped
overwrite does; fine at this dataset's volume, worth revisiting if the fact
tables grow far larger.

## Loading data directly (pandas -> RDS, no S3 needed)

```
mimic-load create-raw-schemas   # create the hosp/icu schemas
mimic-load load-raw             # load every table
mimic-load load-raw --module hosp --table patients   # load just one table
```

## Tests

```
pytest
```

## License

Licensed under the [Apache License, Version 2.0](LICENSE); see also [NOTICE](NOTICE).
Copyright 2026 Narendra P Nagtode.

The license covers this repository's code, SQL, configuration, and documentation only. MIMIC-IV and
MIMIC-IV-Note data are not included and are not covered by it: they are credentialed-access datasets
that each user must obtain from [PhysioNet](https://physionet.org/) under its data use agreement.
