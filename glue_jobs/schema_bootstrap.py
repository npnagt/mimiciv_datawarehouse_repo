"""Glue job 0 (Python Shell, not PySpark): create the staging/warehouse schemas and
tables on a fresh cluster.

Runs once per new cluster, before staging_load.py / warehouse_dimensions.py /
warehouse_facts.py / warehouse_aggregates.py can load anything into it -- those jobs
INSERT/UPSERT into staging.stg_* / warehouse.*, they don't create those tables.
Deliberately a Python Shell job, not PySpark: it only needs to execute plain DDL via
psycopg2, so a Spark session would be pure overhead (Python Shell jobs start in seconds
and bill a fraction of a DPU, vs. a full Spark cluster for the other 4 jobs).

Executes sql/staging_tables.sql and sql/warehouse_tables.sql verbatim (uploaded to S3 by
`mimic-load glue deploy` alongside the job scripts) -- these are the auto-generated,
single-source-of-truth exports of staging.STAGING_TABLE_COLUMNS / warehouse.WAREHOUSE_TABLES
(see scripts/export_staging_ddl.py / export_warehouse_ddl.py), not a third hand-copied
definition of the same tables. Both files are entirely CREATE SCHEMA/TABLE IF NOT EXISTS,
so this is safe to (and, after any DDL change, should be) re-run.

Deliberately does NOT import glue_common.py, unlike every other job: that module imports
`awsglue.context`/`awsglue.job` (PySpark-only) and `pyspark.sql` at module level, neither
of which exist in a Python Shell job's runtime -- importing it here would fail with
ModuleNotFoundError before main() ever runs. The IAM-token connection logic below is a
deliberately small, self-contained duplicate of glue_common.pg_connect's for that reason.

Job arguments:
  --RDS_HOST / --RDS_PORT / --RDS_DATABASE / --RDS_USER / --RDS_PASSWORD / --RDS_AUTH_MODE
  --AWS_REGION
  --SQL_S3_BUCKET   bucket the DDL files were uploaded to
  --DROP_TABLES     comma-separated "schema.table" list to DROP TABLE IF EXISTS before
                     (re)applying the DDL below -- for the cases CREATE TABLE IF NOT
                     EXISTS can't reconcile (e.g. a column/constraint changed on a table
                     that already exists). Data in dropped tables is lost; the caller is
                     responsible for reloading it afterwards. Pass the literal string
                     "none" for a normal run that shouldn't drop anything -- this must
                     always be supplied with *some* value: unlike the PySpark jobs' truly
                     optional --TABLE, getResolvedOptions on the Python Shell runtime
                     silently drops any argument not in its required list, so an
                     always-required "none" sentinel is used here instead (discovered
                     against a real Glue run: --DROP_TABLES was passed by the caller with
                     a real value but args.get("DROP_TABLES", "") still came back empty).
  --TERMINATE_PIDS  comma-separated Postgres backend pids to pg_terminate_backend()
                     before doing anything else -- for clearing zombie sessions left by a
                     forcibly-stopped prior Glue job run (batch_stop_job_run kills the
                     client, not necessarily the server-side backend process/query it
                     started -- confirmed against a real run: a 3.8-hour-old INSERT from
                     a stopped run was still `active` server-side, blocking every
                     subsequent bootstrap/ANALYZE attempt behind it in a lock queue). See
                     the "other sessions" diagnostic printed below for candidate pids.
                     Same "none" sentinel convention as --DROP_TABLES.
"""

from __future__ import annotations

import sys
import time

import boto3
import psycopg2
from awsglue.utils import getResolvedOptions

SQL_S3_PREFIX = "glue-sql"
SQL_FILES = ["staging_tables.sql", "warehouse_tables.sql"]


def _pg_connect(args: dict):
    """Same two auth modes as glue_common.pg_connect (see that module's docstring) --
    duplicated here rather than imported, see this module's docstring for why. Also
    duplicates glue_common.STATEMENT_TIMEOUT_MS / pg_connect's connect_timeout+
    statement_timeout guards for the same reason -- this script hung for 20+ minutes
    with zero output/error on a real run before either of these existed, and having only
    glue_common.py's copy protected against that would leave this script exposed again."""
    is_iam = args.get("RDS_AUTH_MODE") == "iam"
    if is_iam:
        rds = boto3.client("rds", region_name=args.get("AWS_REGION", "us-east-2"))
        password = rds.generate_db_auth_token(
            DBHostname=args["RDS_HOST"], Port=int(args["RDS_PORT"]), DBUsername=args["RDS_USER"]
        )
    else:
        password = args["RDS_PASSWORD"]
    connect_kwargs = {
        "host": args["RDS_HOST"],
        "port": int(args["RDS_PORT"]),
        "dbname": args["RDS_DATABASE"],
        "user": args["RDS_USER"],
        "password": password,
        "connect_timeout": 30,  # seconds; bounds TCP/auth handshake, not query execution
        # 15 minutes, not 5 -- see glue_common.py's pg_connect comment: an
        # AURORA_MIN_ACU=0 (scale-to-zero) cluster can be fully paused between steps and
        # needs time to wake up and scale under load, not just a bad query plan.
        "options": "-c statement_timeout=900000",  # ms; bounds each query once connected
    }
    if is_iam:
        connect_kwargs["sslmode"] = "require"
    return psycopg2.connect(**connect_kwargs)


def main() -> None:
    args = getResolvedOptions(
        sys.argv,
        [
            "RDS_HOST",
            "RDS_PORT",
            "RDS_DATABASE",
            "RDS_USER",
            "RDS_PASSWORD",
            "RDS_AUTH_MODE",
            "AWS_REGION",
            "SQL_S3_BUCKET",
            "DROP_TABLES",
            "TERMINATE_PIDS",
        ],
    )
    s3 = boto3.client("s3")
    print("Connecting to database...")
    conn = _pg_connect(args)
    print("Connected.")
    try:
        with conn, conn.cursor() as cur:
            # Diagnostics run FIRST, before any DROP_TABLES/DDL -- both are lightweight
            # system-catalog reads that don't take locks on staging.*/warehouse.* tables
            # themselves, so they return quickly even if something else is blocking
            # those. Moved here after a real run applied staging_tables.sql fine, then
            # hit the 5-minute statement_timeout applying warehouse_tables.sql -- almost
            # entirely CREATE TABLE IF NOT EXISTS against already-existing tables, which
            # should be a near-instant no-op unless something is holding a lock on one of
            # them. Running the diagnostics only *after* that step meant they never got a
            # chance to run and explain why. An idle-in-transaction session left behind
            # by a forcibly-stopped prior Glue job run (batch_stop_job_run kills the
            # Spark/Python driver, not the Postgres session it opened) holding a lock is
            # the leading suspect -- a long xact_age on an "idle in transaction" row, or
            # any row at all from the second query below, confirms it.
            cur.execute(
                "SELECT pid, state, wait_event_type, wait_event, "
                "now() - xact_start AS xact_age, now() - query_start AS query_age, left(query, 200) AS query "
                "FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() "
                "ORDER BY xact_start ASC NULLS LAST"
            )
            print(
                "Other sessions on this database (pid, state, wait_event_type, wait_event, xact_age, query_age, query):"
            )
            for row in cur.fetchall():
                print(f"  {row}")

            # JOIN through pg_namespace by name, not a `'staging'::regnamespace` cast --
            # that cast raises InvalidSchemaName if the schema doesn't exist yet, which
            # is exactly true on a genuinely fresh database (creating these schemas is
            # what this job is about to do). A JOIN just yields zero rows in that case,
            # which is what a "no locks to report" answer should look like here.
            cur.execute(
                "SELECT l.pid, l.mode, l.granted, c.relname, a.state, "
                "now() - a.xact_start AS xact_age, left(a.query, 200) AS query "
                "FROM pg_locks l "
                "JOIN pg_class c ON c.oid = l.relation "
                "JOIN pg_namespace n ON n.oid = c.relnamespace "
                "JOIN pg_stat_activity a ON a.pid = l.pid "
                "WHERE n.nspname IN ('staging', 'warehouse') "
                "AND l.pid <> pg_backend_pid() "
                "ORDER BY a.xact_start ASC NULLS LAST"
            )
            print(
                "Locks held by other sessions on staging/warehouse tables (pid, mode, granted, table, "
                "state, xact_age, query):"
            )
            for row in cur.fetchall():
                print(f"  {row}")

            raw_terminate_pids = args["TERMINATE_PIDS"]
            terminate_pids = (
                []
                if raw_terminate_pids == "none"
                else [int(p.strip()) for p in raw_terminate_pids.split(",") if p.strip()]
            )
            for pid in terminate_pids:
                cur.execute("SELECT pg_terminate_backend(%s)", (pid,))
                print(f"pg_terminate_backend({pid}): {cur.fetchone()[0]}")
            if terminate_pids:
                time.sleep(2)  # let Postgres finish tearing down the backend(s) and release locks

            raw_drop_tables = args["DROP_TABLES"]
            drop_tables = (
                [] if raw_drop_tables == "none" else [t.strip() for t in raw_drop_tables.split(",") if t.strip()]
            )
            for qualified_name in drop_tables:
                cur.execute(f"DROP TABLE IF EXISTS {qualified_name}")
                print(f"Dropped {qualified_name}")

            for filename in SQL_FILES:
                key = f"{SQL_S3_PREFIX}/{filename}"
                sql_text = s3.get_object(Bucket=args["SQL_S3_BUCKET"], Key=key)["Body"].read().decode("utf-8")
                cur.execute(sql_text)
                print(f"Applied {key}")

            # Always surfaced: fact_chart_observation is the one fact table Postgres
            # never gets a fresh set of planner statistics for via the UNIQUE-constraint
            # path the other surrogate-key facts now get (see warehouse.py) -- cheap to
            # print here, useful context if a load against it is ever slow again.
            cur.execute(
                "SELECT n_live_tup, last_analyze, last_autoanalyze FROM pg_stat_user_tables "
                "WHERE schemaname = 'warehouse' AND relname = 'fact_chart_observation'"
            )
            print(f"fact_chart_observation stats (live rows, last ANALYZE, last autoanalyze): {cur.fetchone()}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
