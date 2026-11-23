"""Shared helpers for the MIMIC-IV warehouse Glue PySpark jobs.

Runs only inside AWS Glue's managed PySpark environment: imports `awsglue`, which Glue
provides at job runtime and is not pip-installable (see pyproject.toml's `pyspark` dev
dependency, which covers `pyspark.sql` for local syntax/type checking only -- it cannot
run these scripts end to end outside Glue).

Every warehouse-load job follows the same "Spark transforms, SQL merges" pattern: Spark
computes the target DataFrame, writes it to a throwaway `landing.<table>_<uuid>` table via
JDBC, then a driver-side psycopg2 connection runs the *same* conflict-handling SQL
etl_warehouse.py runs today (INSERT ... ON CONFLICT, or a NOT EXISTS anti-join for the
facts with no natural key) to merge landing -> target, then drops the landing table.
Spark's JDBC writer has no native upsert, so this is the only way to keep merge semantics
identical to the tested SQL in etl_warehouse.py without re-deriving conflict handling in
Spark itself.

Authentication mirrors mimic_datawarehouse.db.make_engine's two modes (args["RDS_AUTH_MODE"],
always supplied by the CLI orchestrator -- see cli.py's _glue_rds_arguments):
  - "password" (default): args["RDS_PASSWORD"] used as-is. Requires VPC placement if the
    target Aurora cluster isn't publicly reachable (see aws/glue.py's create_glue_connection).
  - "iam": a fresh RDS auth token (15-minute lifetime) is generated per connection via
    boto3 -- same reason as db._make_iam_engine: it can't be generated once and reused for
    the job's lifetime. Used for Aurora clusters provisioned via Aurora's express
    configuration (see aws/aurora.py's docstring) -- no VPC, no static password, but
    reachable over the internet, which is what lets a Glue job with no VPC connection
    attached reach it at all. The Glue job's execution role needs `rds-db:connect` on the
    target DB user (see aws/glue.py's grant_rds_iam_connect).
"""

from __future__ import annotations

import time
import uuid
from datetime import datetime

import boto3
import psycopg2
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession

LANDING_SCHEMA = "landing"

# getResolvedOptions silently drops any --KEY value pair from its returned dict if KEY
# isn't in the `options` list passed to it -- true for both the PySpark and Python Shell
# runtimes, confirmed against real Glue runs (a --TABLE filter and a --DROP_TABLES value
# were each passed by the caller but came back as if never supplied). There is no such
# thing as a genuinely optional job argument: every argument a job wants to read MUST be
# listed in `required_args`/getResolvedOptions' `options`, and the caller (cli.py) must
# always supply *some* value for it -- this sentinel is what "no value" looks like.
NO_VALUE = "none"


def optional_arg(args: dict, key: str) -> str | None:
    """`args[key]` (which must have been included in init_glue_job's `required_args`),
    or None if the caller passed the NO_VALUE sentinel for it. Never use `args.get(key)`
    for an argument the caller might reasonably omit -- see NO_VALUE's comment above."""
    value = args[key]
    return None if value == NO_VALUE else value


def init_glue_job(argv: list[str], required_args: list[str]) -> tuple[GlueContext, SparkSession, Job, dict]:
    """Standard Glue job bootstrap: resolves --JOB_NAME plus `required_args`, starts the
    Glue Job bookkeeping (job-run metrics/CloudWatch), and returns
    (glue_context, spark, job, args). Every argument a job's main() reads must be listed
    in `required_args` -- see NO_VALUE's comment for why "optional" args don't work."""
    args = getResolvedOptions(argv, ["JOB_NAME", *required_args])
    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
    return glue_context, spark, job, args


def _is_iam_auth(args: dict) -> bool:
    return args.get("RDS_AUTH_MODE") == "iam"


def _db_password(args: dict) -> str:
    """A fresh RDS IAM auth token if RDS_AUTH_MODE=iam (generated per call -- tokens
    expire after 15 minutes, so this can't be cached across the job's lifetime, same
    reasoning as db._make_iam_engine), otherwise the static RDS_PASSWORD job argument."""
    if not _is_iam_auth(args):
        return args["RDS_PASSWORD"]
    rds = boto3.client("rds", region_name=args.get("AWS_REGION", "us-east-2"))
    return rds.generate_db_auth_token(
        DBHostname=args["RDS_HOST"], Port=int(args["RDS_PORT"]), DBUsername=args["RDS_USER"]
    )


def jdbc_url(args: dict) -> str:
    base = f"jdbc:postgresql://{args['RDS_HOST']}:{args['RDS_PORT']}/{args['RDS_DATABASE']}"
    # IAM-authenticated RDS connections require TLS.
    return f"{base}?ssl=true&sslmode=require" if _is_iam_auth(args) else base


def jdbc_properties(args: dict) -> dict:
    return {"user": args["RDS_USER"], "password": _db_password(args), "driver": "org.postgresql.Driver"}


STATEMENT_TIMEOUT_MS = 900_000  # 15 minutes -- see pg_connect's comment for why


def _log(message: str) -> None:
    """Printed messages land in the Glue job run's CloudWatch output stream -- this
    module's only logging mechanism. Callers log phase-by-phase (landing write, ANALYZE,
    query plan, merge), not just once per table on completion: a table's own print() (in
    each job's main()) only fires after its *entire* load returns, which gave zero
    visibility into which phase of a slow load was actually slow -- discovered the hard
    way against a real run that sat "RUNNING" for 2+ hours on fact_chart_observation with
    no output and no error."""
    print(f"[{time.strftime('%H:%M:%S', time.gmtime())}] {message}", flush=True)


def pg_connect(args: dict):
    connect_kwargs = {
        "host": args["RDS_HOST"],
        "port": int(args["RDS_PORT"]),
        "dbname": args["RDS_DATABASE"],
        "user": args["RDS_USER"],
        "password": _db_password(args),
        "connect_timeout": 30,  # seconds; bounds TCP/auth handshake, not query execution
        # Fail fast instead of hanging indefinitely on a bad query plan -- the same real
        # run above sat RUNNING for 2+ hours with no signal at all of whether it was
        # progressing or stuck. Applies per statement (libpq session-level GUC), so
        # ANALYZE / EXPLAIN / the merge INSERT each get their own full budget.
        #
        # 15 minutes, not the 5 minutes this started as: fact_chart_observation's
        # first-ever bulk INSERT (~670k rows) repeatedly hit the original 5-minute limit.
        # That was NOT Aurora scale-to-zero cold start, despite AURORA_MIN_ACU=0 (see
        # aws/aurora.py) making that a plausible-sounding theory at first -- CloudWatch's
        # ServerlessDatabaseCapacity metric showed the cluster pinned at its configured
        # max ACU throughout the entire slow window, i.e. already fully scaled up, not
        # waking from a pause. The real cause (see insert_only_with_anti_join's
        # `enable_nestloop = off`) was a Nested Loop Anti Join against a then-empty
        # target: fixed per-iteration overhead invisible to the cost model, replayed
        # 668,862 times. That's now fixed at the query level, so this table's real
        # runtime is back to ~35-40s -- 15 minutes is kept as a generous margin above
        # that, not a workaround for it, and still fails comfortably faster than the 2+
        # hour hang above if something genuinely sticks.
        "options": f"-c statement_timeout={STATEMENT_TIMEOUT_MS}",
    }
    if _is_iam_auth(args):
        connect_kwargs["sslmode"] = "require"
    return psycopg2.connect(**connect_kwargs)


def ensure_landing_schema(args: dict) -> None:
    with pg_connect(args) as conn, conn.cursor() as cur:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{LANDING_SCHEMA}"')
        conn.commit()


def truncate_table(args: dict, schema: str, table: str) -> None:
    with pg_connect(args) as conn, conn.cursor() as cur:
        cur.execute(f'TRUNCATE TABLE {schema}."{table}"')
        conn.commit()


def read_table(spark: SparkSession, args: dict, schema: str, table: str) -> DataFrame:
    return spark.read.jdbc(url=jdbc_url(args), table=f'{schema}."{table}"', properties=jdbc_properties(args))


def _quote_list(cols: list[str]) -> str:
    return ", ".join(f'"{c}"' for c in cols)


def _write_landing(df: DataFrame, args: dict, table: str) -> str:
    landing_table = f"{LANDING_SCHEMA}.{table}_{uuid.uuid4().hex[:12]}"
    _log(f"{table}: writing landing table {landing_table}")
    start = time.monotonic()
    # batchsize: Spark's JDBC writer default (1000) means one round-trip per 1000 rows --
    # a needless bottleneck for the largest fact tables (e.g. chartevents, ~670k rows).
    write_properties = {**jdbc_properties(args), "batchsize": "10000"}
    df.write.jdbc(url=jdbc_url(args), table=landing_table, mode="overwrite", properties=write_properties)
    _log(f"{table}: landing table write finished in {time.monotonic() - start:.1f}s")
    return landing_table


def _analyze(cur, table: str) -> None:
    """A landing table is brand new every run (mode="overwrite" against a fresh name --
    see _write_landing), so Postgres has zero planner statistics for it until autovacuum's
    autoanalyze eventually gets to it. Without them, the merge/anti-join query below can
    pick a badly wrong plan against a large target table (e.g. a full scan instead of
    using the target's existing index) -- ANALYZE up front avoids that."""
    start = time.monotonic()
    cur.execute(f"ANALYZE {table}")
    _log(f"ANALYZE {table} finished in {time.monotonic() - start:.1f}s")


def _log_query_plan(cur, target_table: str, sql: str) -> None:
    """Logs the planner's chosen strategy for `sql` before running it for real -- e.g.
    distinguishing a fast Hash Anti Join from a Nested Loop that would probe the target
    row by row. Meaningful right after _analyze gives the planner fresh statistics on
    both sides; EXPLAIN (without ANALYZE) never executes/mutates anything."""
    cur.execute(f"EXPLAIN {sql}")
    plan = "\n".join(row[0] for row in cur.fetchall())
    _log(f"{target_table}: query plan:\n{plan}")


def merge_into_target(
    df: DataFrame,
    args: dict,
    *,
    schema: str,
    target_table: str,
    conflict_cols: list[str] | None = None,
    conflict_exprs: list[str] | None = None,
    update_cols: list[str],
    job_name: str,
) -> int:
    """Upsert `df` into `schema.target_table`, ON CONFLICT (conflict_cols) DO UPDATE SET
    update_cols (or DO NOTHING if update_cols is empty), via a throwaway landing table.
    Mirrors etl_warehouse._dimension_upsert_sql's conflict handling. Returns rows affected.

    Pass exactly one of `conflict_cols` (plain column names) or `conflict_exprs` (raw SQL
    expressions, e.g. `(COALESCE("emar_id", ''))`) -- the latter for tables whose dedup
    grain has a nullable column and so is backed by a `unique_key_exprs` expression index
    (warehouse.py) instead of a plain UNIQUE constraint: a plain UNIQUE/ON CONFLICT column
    list treats NULL as never-equal-to-NULL, so it wouldn't recognize two NULL-grain rows
    as a real conflict, but Postgres does match ON CONFLICT against an expression index
    when the expressions are given verbatim. See warehouse_facts.py's
    fact_medication_administration_mini_detail for why this matters in practice: real
    duplicate emar_detail.csv.gz rows with a non-NULL grain triggered a UniqueViolation
    when this table used insert_only_with_anti_join instead (that helper's anti-join only
    guards against rows already committed to the target, not same-landing-table
    duplicates -- switching to ON CONFLICT here handles those for free, the same fix
    already applied to the non-Glue etl_warehouse.py SQL path)."""
    assert (conflict_cols is None) != (conflict_exprs is None), (
        "merge_into_target: pass exactly one of conflict_cols or conflict_exprs"
    )
    landing_table = _write_landing(df, args, target_table)
    col_list = _quote_list(df.columns)
    conflict_target = ", ".join(conflict_exprs) if conflict_exprs else _quote_list(conflict_cols)

    if update_cols:
        # updated_by only set if the caller's DataFrame actually has that column --
        # dimensions/facts do (see warehouse_dimensions.py / warehouse_facts.py's
        # audit_cols), but aggregate tables don't (warehouse.py's _agg_cols
        # deliberately omits created_by/updated_by, matching the original SQL in
        # etl_warehouse.AGGREGATE_LOAD_SQL). Hardcoding it unconditionally here broke a
        # real run: "UndefinedColumn: column updated_by of relation agg_admission_daily
        # does not exist".
        updated_by_clause = f", updated_by = '{job_name}'" if "updated_by" in df.columns else ""
        conflict_action = (
            "DO UPDATE SET "
            + ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols)
            + f", updated_ts = now(){updated_by_clause}"
        )
    else:
        conflict_action = "DO NOTHING"

    sql = (
        f'INSERT INTO {schema}."{target_table}" ({col_list})\n'
        f"SELECT {col_list} FROM {landing_table}\n"
        f"ON CONFLICT ({conflict_target}) {conflict_action};"
    )
    with pg_connect(args) as conn, conn.cursor() as cur:
        _analyze(cur, landing_table)
        _log(f"{target_table}: running merge (ON CONFLICT)")
        start = time.monotonic()
        cur.execute(sql)
        rowcount = cur.rowcount
        _log(f"{target_table}: merge finished in {time.monotonic() - start:.1f}s, {rowcount} rows affected")
        cur.execute(f"DROP TABLE {landing_table}")
        conn.commit()
    return rowcount


def _count_rows(args: dict, schema: str, table: str) -> int | None:
    """COUNT(*) of schema.table, or None if it can't be read (table doesn't exist yet,
    connection issue, etc.) -- best-effort, same spirit as the lakehouse orchestrator's
    _count_rows (run_lakehouse_pipeline.py), which reads Athena instead of Postgres."""
    try:
        with pg_connect(args) as conn, conn.cursor() as cur:
            cur.execute(f'SELECT count(*) FROM {schema}."{table}"')
            return cur.fetchone()[0]
    except Exception:
        return None


def log_timed_call(
    args: dict,
    *,
    full_run_id: int,
    phase: str,
    process_type: str,
    process_name: str,
    fn,
    source_table: tuple[str, str] | None = None,
    target_table: tuple[str, str] | None = None,
) -> int:
    """Call `fn()` (a zero-arg closure returning rows affected), timing it, and insert one
    row into warehouse.etl_process_log recording the outcome -- the per-table-loop
    counterpart, inside these coarser-grained Glue jobs, to the lakehouse orchestrator's
    per-job-run _log_process_run (run_lakehouse_pipeline.py). Column set mirrors the
    lakehouse's etl_process_log exactly -- see warehouse.py's table comment. `phase` is
    the glue_run_all step name, letter-prefixed to align with the lakehouse's A-J scheme
    (A:staging, B:dimensions, C:facts, D:aggregates), passed by each of the 4 Glue
    scripts' main(). source_table/target_table are
    (schema, table) pairs COUNT(*)'d right before/after fn() runs, logged as
    raw_row_count/gold_row_count (pass None where not meaningful, e.g. dim_date's seed
    step, which has no staging source). dpu_seconds is left NULL here -- it's only known
    once the whole Glue job run is terminal, so cli.py's _run_glue_step backfills it
    afterwards for every row of that run_id.

    On failure, logs status=FAILED with the exception message and RE-RAISES: a logging
    failure must never mask a real pipeline failure, but a real pipeline failure must
    still fail the Glue job, same as before this helper existed.

    Also prints a START line to CloudWatch before `fn()` runs and a DONE/FAILED line
    after -- every table's load is bounded by these two (or three, on the phase-detail
    lines merge_into_target/insert_only_with_anti_join already print in between), so
    scanning the job's CloudWatch output tells you which table is running right now and
    which ones already finished, without waiting for the whole job to reach a terminal
    state first."""
    job_name = args.get("JOB_NAME", process_type)
    dataset = args.get("DATASET", NO_VALUE)
    run_id = args.get("JOB_RUN_ID", NO_VALUE)
    raw_row_count = _count_rows(args, *source_table) if source_table else None

    _log(f"=== [{phase}] START  {process_name} ===")
    start_ts = datetime.utcnow()
    try:
        rows_affected = fn()
    except Exception as exc:
        _insert_process_log(
            args, full_run_id=full_run_id, run_id=run_id, job_name=job_name, phase=phase,
            process_type=process_type, process_name=process_name, dataset=dataset,
            start_ts=start_ts, end_ts=datetime.utcnow(), raw_row_count=raw_row_count,
            gold_row_count=None, status="FAILED", error_message=str(exc)[:2000],
        )
        _log(f"=== [{phase}] FAILED {process_name}: {exc} ===")
        raise

    end_ts = datetime.utcnow()
    gold_row_count = _count_rows(args, *target_table) if target_table else None
    _insert_process_log(
        args, full_run_id=full_run_id, run_id=run_id, job_name=job_name, phase=phase,
        process_type=process_type, process_name=process_name, dataset=dataset,
        start_ts=start_ts, end_ts=end_ts, raw_row_count=raw_row_count,
        gold_row_count=gold_row_count, status="SUCCEEDED", error_message=None,
    )
    _log(
        f"=== [{phase}] DONE   {process_name}: {rows_affected} row(s), "
        f"{(end_ts - start_ts).total_seconds():.1f}s ==="
    )
    return rows_affected


def _insert_process_log(
    args: dict,
    *,
    full_run_id: int,
    run_id: str,
    job_name: str,
    phase: str,
    process_type: str,
    process_name: str,
    dataset: str,
    start_ts,
    end_ts,
    raw_row_count: int | None,
    gold_row_count: int | None,
    status: str,
    error_message: str | None,
) -> None:
    """Best-effort INSERT into warehouse.etl_process_log -- never raises. A logging
    failure (table not bootstrapped yet, a transient connection blip) must never abort or
    be mistaken for a pipeline failure, same principle as the lakehouse's
    _log_process_run. dpu_seconds is not set here -- see log_timed_call's docstring."""
    try:
        with pg_connect(args) as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO warehouse.etl_process_log "
                "(full_run_id, run_id, job_name, process_name, process_type, phase, dataset, "
                "start_ts, end_ts, execution_seconds, raw_row_count, gold_row_count, "
                "status, error_message, logged_ts) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now())",
                (
                    full_run_id, run_id, job_name, process_name, process_type, phase, dataset,
                    start_ts, end_ts, (end_ts - start_ts).total_seconds(),
                    raw_row_count, gold_row_count, status, error_message,
                ),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 -- see docstring: must never raise
        _log(f"[process-log] WARNING: could not log {process_name}: {exc}")


def insert_only_with_anti_join(
    df: DataFrame,
    args: dict,
    *,
    schema: str,
    target_table: str,
    dedup_cols: list[str],
) -> int:
    """Insert-only load of `df` into `schema.target_table`: rows whose `dedup_cols`
    already exist (via plain `=`, not etl_warehouse.py's IS NOT DISTINCT FROM) are
    skipped. Returns rows inserted.

    As of the fact_medication_administration_mini_detail / fact_prescription /
    fact_datetime_event / fact_input_event / fact_ingredient_event / fact_output_event /
    fact_procedure_event UniqueViolation fix, this helper is used for exactly one table:
    fact_chart_observation, which deliberately has NO unique constraint at all (~12% of
    raw chartevents.csv.gz rows genuinely collide on its natural grain, so duplicates
    there are real data, not a bug). Every other surrogate-key fact now goes through
    merge_into_target's ON CONFLICT against a NULL-safe expression index instead --
    this anti-join only guards against rows already committed to the target, not
    duplicates within the same landing table, which is exactly what caused those tables'
    UniqueViolation failures.

    Deliberately `=`, not IS NOT DISTINCT FROM, despite that being a real behavior
    difference from etl_warehouse.py: IS NOT DISTINCT FROM can't use a hash or merge
    join strategy, which forced Postgres into a Nested Loop Anti Join here -- discovered
    against a real run where fact_chart_observation's EXPLAIN showed an estimated cost of
    13.1 *billion* (row_count^2 comparisons) and a 5-minute statement_timeout cancellation
    that would otherwise have been an hours-long hang. `=` lets the planner hash both
    sides instead. The tradeoff: a row with NULL in every one of `dedup_cols` is never
    `= ` to itself, so it won't be recognized as "already loaded" on a second run of the
    same source data and could be inserted again -- acceptable here since it only affects
    rows null across their *entire* dedup key, not real data loss or corruption."""
    landing_table = _write_landing(df, args, target_table)
    col_list = _quote_list(df.columns)
    dedup_predicate = " AND ".join(f'w."{c}" = l."{c}"' for c in dedup_cols)

    sql = (
        f'INSERT INTO {schema}."{target_table}" ({col_list})\n'
        f"SELECT {col_list} FROM {landing_table} l\n"
        "WHERE NOT EXISTS (\n"
        f'    SELECT 1 FROM {schema}."{target_table}" w\n'
        f"    WHERE {dedup_predicate}\n"
        ");"
    )
    with pg_connect(args) as conn, conn.cursor() as cur:
        _analyze(cur, landing_table)
        # Unlike merge_into_target's ON CONFLICT (which always resolves through the
        # unique index regardless of statistics), the query planner's choice of anti-join
        # strategy here depends heavily on both sides' row-count estimates -- the target
        # table's own stats matter just as much as the landing table's for tables like
        # fact_chart_observation with hundreds of thousands of existing rows.
        _analyze(cur, f'{schema}."{target_table}"')
        # Force a Hash Anti Join, never a Nested Loop -- confirmed against a real,
        # reproducible case (fact_chart_observation's first-ever load, target empty):
        # the planner sees ~0-1 rows on the target side and "correctly" concludes
        # building a hash table for that is pure overhead, so it picks Nested Loop
        # instead. But Nested Loop re-opens/re-scans the inner side once per outer row
        # -- 668,862 times here -- and that per-iteration restart cost (relation open,
        # snapshot/visibility check) isn't in the cost model at all, only the per-tuple
        # cost is. Measured directly against this exact data: the Nested Loop plan
        # didn't finish in 5 minutes (300s, then cancelled by statement_timeout) where a
        # plain INSERT of the same 668,862 rows with the same 7 FK checks and no anti-join
        # took 35s -- i.e. this clause alone, not FK validation or Aurora capacity
        # (CloudWatch confirmed the cluster held steady at its configured max ACU
        # throughout, so it wasn't scaling up from cold), was responsible for 500x+ the
        # runtime. Forcing Hash Anti Join brought it back down to 37s, matching the plain
        # insert. Scoped to this transaction only (SET LOCAL), so it can't affect any
        # other query on this connection or any other session.
        cur.execute("SET LOCAL enable_nestloop = off")
        _log_query_plan(cur, target_table, sql)
        _log(f"{target_table}: running anti-join merge")
        start = time.monotonic()
        cur.execute(sql)
        rowcount = cur.rowcount
        _log(f"{target_table}: anti-join merge finished in {time.monotonic() - start:.1f}s, {rowcount} rows inserted")
        cur.execute(f"DROP TABLE {landing_table}")
        conn.commit()
    return rowcount
