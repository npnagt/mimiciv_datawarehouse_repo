"""Command-line entrypoint: `mimic-load`."""

from __future__ import annotations

import logging
import os

import click
from sqlalchemy import text

from .aws import aurora, glue, iam, s3
from .config import DATASET_CHOICES, load_aws_settings, load_settings
from .db import ensure_database, ensure_schemas, make_engine
from .etl import DEFAULT_CHUNKSIZE as STAGING_DEFAULT_CHUNKSIZE
from .etl import DEFAULT_ROW_CAP as STAGING_DEFAULT_ROW_CAP
from .etl import load_all_staging_tables
from .etl_warehouse import load_all_dimensions, load_all_facts, refresh_all_aggregates
from .loader import DEFAULT_CHUNKSIZE, load_table
from .reload import truncatable_tables, truncate_all
from .seed_date_dim import SEED_END_DATE, SEED_START_DATE, seed_date_dim
from .staging import create_staging_tables
from .tables import MODULES, discover_tables
from .warehouse import WAREHOUSE_TABLE_ORDER, create_warehouse_tables

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def _set_dataset(ctx: click.Context, param: click.Parameter, value: str | None) -> None:
    """Eager --dataset callback: export MIMIC_DATASET before any load_*settings() runs so
    every subcommand's config resolves to that dataset's resources. An explicit env var
    (or one already in .env) still wins if --dataset isn't passed."""
    if value:
        os.environ["MIMIC_DATASET"] = value


@click.group()
@click.option(
    "--dataset",
    type=click.Choice(DATASET_CHOICES),
    default=None,
    is_eager=True,
    expose_value=False,
    callback=_set_dataset,
    help="Dataset + resource set to target (default: demodataset, or MIMIC_DATASET).",
)
def main() -> None:
    """Load MIMIC-IV data (demo or full) into AWS RDS."""


@main.command("create-raw-schemas")
def create_raw_schemas() -> None:
    """Create the hosp/icu schemas in RDS if they don't already exist."""
    settings = load_settings()
    engine = make_engine(settings)
    ensure_schemas(engine, list(MODULES))
    click.echo(f"Ensured schemas: {', '.join(MODULES)}")


@main.command("load-raw")
@click.option("--module", "modules", multiple=True, help="Limit to specific module(s), e.g. hosp, icu.")
@click.option("--table", "table_filter", default=None, help="Limit to a single table name.")
@click.option("--chunksize", default=DEFAULT_CHUNKSIZE, show_default=True)
@click.option(
    "--if-exists",
    type=click.Choice(["replace", "append", "fail"]),
    default="replace",
    show_default=True,
)
def load_raw(modules: tuple[str, ...], table_filter: str | None, chunksize: int, if_exists: str) -> None:
    """Load CSV.gz source tables directly into the hosp/icu schemas in RDS (pandas ->
    to_sql, bypassing staging/warehouse)."""
    settings = load_settings()
    engine = make_engine(settings)
    ensure_schemas(engine, list(modules) or list(MODULES))

    tables = discover_tables(settings.data_dir, tuple(modules) or MODULES)
    if table_filter:
        tables = [t for t in tables if t.name == table_filter]

    if not tables:
        raise click.ClickException("No matching tables found under MIMIC_DATA_DIR.")

    for table in tables:
        rows = load_table(engine, table, chunksize=chunksize, if_exists=if_exists)
        click.echo(f"Loaded {table.schema}.{table.name}: {rows} rows")


@main.command()
def create_database() -> None:
    """Create RDS_DATABASE inside the cluster if it doesn't already exist.

    Needed on accounts where Aurora's express configuration was used (see
    aws.aurora.create_serverless_cluster) -- that path can't create an initial
    database at cluster-creation time.
    """
    settings = load_settings()
    created = ensure_database(settings)
    if created:
        click.echo(f"Created database: {settings.rds_database}")
    else:
        click.echo(f"Database already exists: {settings.rds_database}")


@main.command()
def create_staging() -> None:
    """Create the `staging` schema and one stg_<table> per raw table in RDS."""
    settings = load_settings()
    engine = make_engine(settings)
    created = create_staging_tables(engine)
    click.echo(f"Created {len(created)} staging tables: {', '.join(created)}")


@main.command("load-staging")
@click.option("--module", "modules", multiple=True, help="Limit to specific module(s), e.g. hosp, icu.")
@click.option("--table", "table_filter", default=None, help="Limit to a single table name.")
@click.option("--chunksize", default=STAGING_DEFAULT_CHUNKSIZE, show_default=True)
@click.option(
    "--row-cap",
    default=STAGING_DEFAULT_ROW_CAP,
    show_default=True,
    help="Max rows to load per staging table (0 = no limit). Any raw table above the cap "
    "is truncated to it -- the same rule the lakehouse repo applies to its Gold fact tables.",
)
def load_staging_cmd(modules: tuple[str, ...], table_filter: str | None, chunksize: int, row_cap: int) -> None:
    """Truncate and reload every staging.stg_<table> from the raw CSV.gz files."""
    settings = load_settings()
    engine = make_engine(settings)

    results = load_all_staging_tables(
        engine,
        settings.data_dir,
        modules=tuple(modules) or MODULES,
        table_filter=table_filter,
        chunksize=chunksize,
        row_cap=row_cap,
    )
    if not results:
        raise click.ClickException("No matching tables found under MIMIC_DATA_DIR.")

    for name, rows in results.items():
        click.echo(f"Loaded staging.stg_{name}: {rows} rows")


@main.command()
def create_warehouse() -> None:
    """Create the `warehouse` schema and every dim_/fact_ table (dependency order)."""
    settings = load_settings()
    engine = make_engine(settings)
    created = create_warehouse_tables(engine)
    click.echo(f"Created {len(created)} warehouse tables: {', '.join(created)}")


@main.command("seed-date-dim")
@click.option("--start", "start_str", default=SEED_START_DATE.isoformat(), show_default=True)
@click.option("--end", "end_str", default=SEED_END_DATE.isoformat(), show_default=True)
def seed_date_dim_cmd(start_str: str, end_str: str) -> None:
    """Upsert warehouse.dim_date for every calendar day in [--start, --end]."""
    import datetime

    settings = load_settings()
    engine = make_engine(settings)
    start = datetime.date.fromisoformat(start_str)
    end = datetime.date.fromisoformat(end_str)
    days = seed_date_dim(engine, start, end)
    click.echo(f"Seeded warehouse.dim_date: {days} days ({start} .. {end})")


@main.command("load-warehouse")
def load_warehouse_cmd() -> None:
    """Load every warehouse dim (upsert), fact (insert-only), then aggregate (upsert)
    from staging/already-loaded facts.

    Run `seed-date-dim` first -- facts join dim_date via their role-playing
    *_date_key columns.
    """
    settings = load_settings()
    engine = make_engine(settings)

    dim_results = load_all_dimensions(engine)
    for name, rows in dim_results.items():
        click.echo(f"Upserted {name}: {rows} rows")

    fact_results = load_all_facts(engine, WAREHOUSE_TABLE_ORDER)
    for name, rows in fact_results.items():
        click.echo(f"Inserted {name}: {rows} new rows")

    agg_results = refresh_all_aggregates(engine)
    for name, rows in agg_results.items():
        click.echo(f"Refreshed {name}: {rows} rows")


@main.command("truncate-all")
@click.option(
    "--yes", is_flag=True, help="Actually truncate (default: dry run -- only prints the table list)."
)
def truncate_all_cmd(yes: bool) -> None:
    """TRUNCATE every staging.stg_* and warehouse.* table so the next load starts from
    empty, WITHOUT touching warehouse.etl_process_log -- that table is the append-only
    cross-run timing/row-count history (see README.md's "Job timing log"), kept across a
    fresh reload specifically so you can still compare this run's numbers against
    earlier ones. Run seed-date-dim + load-warehouse (or the Glue equivalents) again
    afterward to reload."""
    settings = load_settings()
    engine = make_engine(settings)
    tables = truncatable_tables()
    if not yes:
        click.echo(f"Dry run -- would TRUNCATE {len(tables)} table(s) (warehouse.etl_process_log preserved):")
        for t in tables:
            click.echo(f"  {t}")
        click.echo("Re-run with --yes to actually truncate.")
        return
    truncate_all(engine)
    click.echo(f"Truncated {len(tables)} table(s). warehouse.etl_process_log preserved.")


@main.command("refresh-aggregates")
def refresh_aggregates_cmd() -> None:
    """Recompute agg_admission_daily / agg_icu_fluid_balance_daily / agg_admission_monthly
    from the already-loaded fact tables. Run standalone to refresh rollups after facts
    change without re-running the full dim/fact load."""
    settings = load_settings()
    engine = make_engine(settings)
    results = refresh_all_aggregates(engine)
    for name, rows in results.items():
        click.echo(f"Refreshed {name}: {rows} rows")


@click.group()
def aws_cmd() -> None:
    """Provision the AWS infrastructure: S3 bucket, IAM role, Aurora cluster."""


main.add_command(aws_cmd, name="aws")


@aws_cmd.command("create-bucket")
def aws_create_bucket() -> None:
    """Create the raw-data S3 bucket."""
    settings = load_aws_settings()
    s3.create_bucket(settings.s3_bucket, settings.region)
    click.echo(f"Bucket ready: s3://{settings.s3_bucket}")


@aws_cmd.command("upload-data")
def aws_upload_data() -> None:
    """Upload the local MIMIC-IV CSV.gz files to the raw-data S3 bucket."""
    settings = load_aws_settings()
    keys = s3.upload_dataset(settings.s3_bucket, settings.data_dir, settings.region)
    click.echo(f"Uploaded {len(keys)} files to s3://{settings.s3_bucket}")


@aws_cmd.command("create-iam-role")
def aws_create_iam_role() -> None:
    """Create the IAM role Aurora uses to read the raw-data bucket from S3."""
    settings = load_aws_settings()
    role_arn = iam.create_s3_import_role(settings.iam_role_name, settings.s3_bucket)
    click.echo(f"IAM role ready: {role_arn}")


@aws_cmd.command("create-cluster")
@click.option("--wait/--no-wait", default=True, help="Block until the cluster is available.")
def aws_create_cluster(wait: bool) -> None:
    """Create the Aurora PostgreSQL Serverless v2 cluster (private, VPC-only)."""
    settings = load_aws_settings()
    aurora.create_serverless_cluster(
        cluster_id=settings.aurora_cluster_id,
        db_name=settings.aurora_db_name,
        master_username=settings.aurora_master_username,
        master_password=settings.aurora_master_password,
        region=settings.region,
        subnet_group_name=settings.db_subnet_group_name,
        security_group_ids=settings.vpc_security_group_ids,
        min_acu=settings.aurora_min_acu,
        max_acu=settings.aurora_max_acu,
    )
    click.echo(f"Aurora cluster {settings.aurora_cluster_id} creation requested.")
    if wait:
        endpoint = aurora.wait_for_cluster_available(settings.aurora_cluster_id, settings.region)
        click.echo(f"Cluster available at: {endpoint}")


@aws_cmd.command("attach-s3-role")
def aws_attach_s3_role() -> None:
    """Associate the S3-read IAM role with the Aurora cluster (required for aws_s3 imports)."""
    settings = load_aws_settings()
    role_arn = iam.create_s3_import_role(settings.iam_role_name, settings.s3_bucket)
    aurora.attach_s3_import_role(settings.aurora_cluster_id, role_arn, settings.region)
    click.echo(f"Role {role_arn} attached to cluster {settings.aurora_cluster_id}")


@click.group()
def glue_cmd() -> None:
    """Run the production ETL as managed AWS Glue jobs (staging load, then warehouse
    dimensions/facts/aggregates) -- the Glue-backed equivalent of load-staging /
    load-warehouse above. Those local commands stay available for dev against a
    reachable Postgres; these run the same logic as PySpark jobs in AWS instead."""


main.add_command(glue_cmd, name="glue")


def _glue_rds_arguments(settings) -> dict[str, str]:
    return {
        "--RDS_HOST": settings.rds_host,
        "--RDS_PORT": str(settings.rds_port),
        "--RDS_DATABASE": settings.rds_database,
        "--RDS_USER": settings.rds_user,
        # Never pass "" here: AWS Glue's start_job_run drops an Arguments entry down to
        # a bare "--RDS_PASSWORD" flag with no value when given an empty string, which
        # crashes getResolvedOptions in the job ("expected one argument") -- discovered
        # running this against a real Glue job. Ignored entirely in IAM mode (see
        # glue_common.py's docstring) -- this placeholder just avoids that Glue quirk.
        "--RDS_PASSWORD": settings.rds_password or "unused-iam-auth",
        "--RDS_AUTH_MODE": settings.auth_mode,
        "--DATASET": settings.dataset,
    }


def _next_full_run_id(settings) -> int:
    """One integer shared by every row from a single glue_run_all() invocation -- the
    Postgres/cli.py counterpart to the lakehouse orchestrator's _next_full_run_id
    (run_lakehouse_pipeline.py): MAX(full_run_id)+1 over warehouse.etl_process_log, or 1
    if the table is empty or unreachable (e.g. before `glue run-bootstrap` has ever run).
    NOT safe against concurrent invocations, same caveat as the lakehouse -- there's no
    locking around the read-then-use here."""
    try:
        engine = make_engine(settings)
        with engine.connect() as conn:
            value = conn.execute(
                text("SELECT COALESCE(MAX(full_run_id), 0) + 1 FROM warehouse.etl_process_log")
            ).scalar()
        return int(value)
    except Exception:
        return 1


@glue_cmd.command("deploy")
def glue_deploy() -> None:
    """Upload the Glue job scripts + DDL and create/update the IAM role, JDBC connection
    to Aurora, and the 5 Glue jobs. Idempotent -- safe to re-run after changing a script.
    Run `mimic-load glue run-bootstrap` once afterwards on a fresh cluster before the
    other jobs (they load data into tables the bootstrap job creates)."""
    aws_settings = load_aws_settings()

    click.echo(f"Uploading job scripts to s3://{aws_settings.s3_bucket}/{glue.GLUE_SCRIPT_PREFIX}/")
    glue.upload_job_scripts(aws_settings.s3_bucket, aws_settings.region)

    click.echo(f"Uploading DDL to s3://{aws_settings.s3_bucket}/{glue.GLUE_SQL_PREFIX}/")
    glue.upload_sql_ddl(aws_settings.s3_bucket, aws_settings.region)

    click.echo(f"Creating/reusing IAM role {aws_settings.glue_role_name}")
    role_arn = glue.create_glue_role(aws_settings.glue_role_name, aws_settings.s3_bucket)

    settings = load_settings()
    connection_name = None
    if aws_settings.glue_subnet_id:
        # A VPC-attached, non-publicly-reachable target -- see create_glue_connection's
        # docstring. Not needed (and skipped) for a cluster reachable over the internet,
        # e.g. Aurora's express configuration.
        connection_name = aws_settings.glue_connection_name
        click.echo(f"Creating/updating Glue connection {connection_name}")
        glue.create_glue_connection(
            connection_name,
            rds_host=settings.rds_host,
            rds_port=settings.rds_port,
            rds_database=settings.rds_database,
            rds_user=settings.rds_user,
            rds_password=settings.rds_password,
            subnet_id=aws_settings.glue_subnet_id,
            security_group_ids=aws_settings.vpc_security_group_ids,
            availability_zone=aws_settings.glue_availability_zone,
        )
    else:
        click.echo(
            "GLUE_SUBNET_ID not set -- skipping VPC connection (jobs use default, internet-reachable networking)."
        )

    if settings.auth_mode == "iam":
        resource_id = glue.get_db_cluster_resource_id(aws_settings.aurora_cluster_id, aws_settings.region)
        click.echo(f"Granting rds-db:connect on {settings.rds_user} to {aws_settings.glue_role_name}")
        glue.grant_rds_iam_connect(aws_settings.glue_role_name, aws_settings.region, resource_id, settings.rds_user)

    for step in glue.GLUE_JOB_STEPS:
        name = glue.create_or_update_job(
            step,
            job_name_prefix=aws_settings.glue_job_name_prefix,
            s3_bucket=aws_settings.s3_bucket,
            role_arn=role_arn,
            connection_name=connection_name,
            worker_type=aws_settings.glue_worker_type,
            num_workers=aws_settings.glue_num_workers,
            glue_version=aws_settings.glue_version,
        )
        click.echo(f"Glue job ready: {name}")


def _run_glue_step(step: str, *, extra_arguments: dict[str, str] | None, wait: bool) -> str:
    aws_settings = load_aws_settings()
    settings = load_settings()
    name = glue.job_name(aws_settings.glue_job_name_prefix, step)

    arguments = _glue_rds_arguments(settings)
    arguments["--AWS_REGION"] = aws_settings.region
    # Baseline default for every step's --TABLE (a caller's extra_arguments overrides it
    # for a real filter). Every job now requires TABLE in its getResolvedOptions list and
    # reads it via optional_arg(args, "TABLE") -- omitting it entirely, as this used to
    # do, raises "Missing option" on the Glue side (discovered against a real run: a
    # --TABLE filter silently never took effect until TABLE was made required+sentineled
    # like this, see glue_common.NO_VALUE).
    arguments["--TABLE"] = "none"
    # Baseline per-table row cap for the staging job (harmless for the other steps --
    # getResolvedOptions drops an argument a job doesn't list, see glue_common.NO_VALUE).
    # A caller's extra_arguments overrides it; "0" means unlimited.
    arguments["--ROW_CAP"] = str(STAGING_DEFAULT_ROW_CAP)
    # Baseline: a standalone single-step command (e.g. `glue run-facts` on its own) gets
    # its own fresh full_run_id, treating itself as its own "run" for analysis purposes.
    # glue_run_all overrides this via extra_arguments so all 4 of its steps share one id.
    arguments["--FULL_RUN_ID"] = str(_next_full_run_id(settings))
    if extra_arguments:
        arguments.update(extra_arguments)

    if not wait:
        run_id = glue.start_job_run(name, arguments)
        click.echo(f"Started {name} (run {run_id})")
        return "STARTED"

    # run_job_and_wait (not separate start_job_run/wait_for_job_run calls) so a job run
    # immediately after `glue deploy` created a brand-new execution role automatically
    # retries through IAM's trust-policy propagation delay instead of failing outright
    # (see aws/glue.py's _IAM_PROPAGATION_SIGNATURE -- discovered against a real run).
    state, glue_run_id = glue.run_job_and_wait(name, arguments)
    click.echo(f"{name} finished: {state}")
    # dpu_seconds can only be known once the run is terminal (see warehouse.py's
    # etl_process_log comment) -- backfill every row this run logged, success or failure.
    _backfill_dpu_seconds(settings, name, glue_run_id)
    if state != "SUCCEEDED":
        raise click.ClickException(f"{name} did not succeed (state: {state}).")
    return state


def _backfill_dpu_seconds(settings, job_name: str, run_id: str) -> None:
    """Fill in warehouse.etl_process_log.dpu_seconds for every row this Glue job run
    logged (matched by run_id), now that the run is terminal and Glue can report its real
    DPUSeconds -- unknowable from inside the job while it was still running, since a
    table's row is logged mid-run, before the job's own total is final. Best-effort: a
    logging gap must never fail an otherwise-successful (or already-failed) pipeline
    step, so any error here is swallowed and just printed."""
    try:
        dpu_seconds = glue.get_job_run_dpu_seconds(job_name, run_id)
        if dpu_seconds is None:
            return
        engine = make_engine(settings)
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE warehouse.etl_process_log SET dpu_seconds = :dpu WHERE run_id = :run_id"),
                {"dpu": dpu_seconds, "run_id": run_id},
            )
    except Exception as exc:  # noqa: BLE001 -- see docstring: must never raise
        click.echo(f"[process-log] WARNING: could not backfill dpu_seconds for {job_name} run {run_id}: {exc}")


@glue_cmd.command("run-bootstrap")
@click.option("--wait/--no-wait", default=True, help="Block until the job run finishes.")
@click.option(
    "--drop-tables",
    default=None,
    help='Comma-separated "schema.table" list to DROP TABLE IF EXISTS before (re)applying '
    "the DDL -- use when a table's definition changed in a way CREATE TABLE IF NOT EXISTS "
    "can't reconcile (e.g. a new UNIQUE constraint). Data in dropped tables is lost; "
    "reload it afterwards (e.g. via glue run-facts --table).",
)
@click.option(
    "--terminate-pids",
    default=None,
    help="Comma-separated Postgres backend pids to pg_terminate_backend() before doing anything "
    "else -- for clearing zombie sessions left by a forcibly-stopped prior Glue job run (its "
    "client dying doesn't necessarily cancel the query its backend was still running). Run once "
    "with no other flags first to see the 'other sessions' / 'locks held' diagnostic output and "
    "identify candidate pids.",
)
def glue_run_bootstrap(wait: bool, drop_tables: str | None, terminate_pids: str | None) -> None:
    """Run the schema-bootstrap Glue job (creates the staging/warehouse schemas and
    tables) -- run this once on a fresh cluster before run-staging/run-all. Safe to
    re-run any time (CREATE SCHEMA/TABLE IF NOT EXISTS)."""
    aws_settings = load_aws_settings()
    # Always supplied, even when empty (as the literal sentinel "none") -- see
    # schema_bootstrap.py's docstring for why these arguments can't be conditionally
    # omitted the way --TABLE is for the PySpark jobs.
    extra = {
        "--SQL_S3_BUCKET": aws_settings.s3_bucket,
        "--DROP_TABLES": drop_tables or "none",
        "--TERMINATE_PIDS": terminate_pids or "none",
    }
    _run_glue_step("bootstrap", extra_arguments=extra, wait=wait)


@glue_cmd.command("run-staging")
@click.option("--modules", default="hosp,icu", show_default=True, help="Comma-separated module list.")
@click.option("--table", "table_filter", default=None, help="Limit to a single table name.")
@click.option(
    "--row-cap",
    default=STAGING_DEFAULT_ROW_CAP,
    show_default=True,
    help="Max rows to load per staging table (0 = no limit). Any raw table above the cap "
    "is truncated to it -- the same rule the lakehouse repo applies to its Gold fact tables.",
)
@click.option("--wait/--no-wait", default=True, help="Block until the job run finishes.")
def glue_run_staging(modules: str, table_filter: str | None, row_cap: int, wait: bool) -> None:
    """Run the staging-load Glue job (S3 raw CSV.gz -> staging.stg_*)."""
    aws_settings = load_aws_settings()
    extra = {"--S3_BUCKET": aws_settings.s3_bucket, "--MODULES": modules, "--ROW_CAP": str(row_cap)}
    if table_filter:
        extra["--TABLE"] = table_filter
    _run_glue_step("staging", extra_arguments=extra, wait=wait)


@glue_cmd.command("run-dimensions")
@click.option("--table", "table_filter", default=None, help="Limit to a single dimension table name.")
@click.option("--wait/--no-wait", default=True, help="Block until the job run finishes.")
def glue_run_dimensions(table_filter: str | None, wait: bool) -> None:
    """Run the warehouse-dimensions Glue job (staging.stg_* -> warehouse.dim_*, upsert)."""
    extra = {"--TABLE": table_filter} if table_filter else None
    _run_glue_step("dimensions", extra_arguments=extra, wait=wait)


@glue_cmd.command("run-facts")
@click.option("--table", "table_filter", default=None, help="Limit to a single fact table name.")
@click.option("--wait/--no-wait", default=True, help="Block until the job run finishes.")
def glue_run_facts(table_filter: str | None, wait: bool) -> None:
    """Run the warehouse-facts Glue job (staging.stg_* -> warehouse.fact_*, insert-only)."""
    extra = {"--TABLE": table_filter} if table_filter else None
    _run_glue_step("facts", extra_arguments=extra, wait=wait)


@glue_cmd.command("run-aggregates")
@click.option("--table", "table_filter", default=None, help="Limit to a single aggregate table name.")
@click.option("--wait/--no-wait", default=True, help="Block until the job run finishes.")
def glue_run_aggregates(table_filter: str | None, wait: bool) -> None:
    """Run the warehouse-aggregates Glue job (warehouse.fact_* -> warehouse.agg_*, refresh)."""
    extra = {"--TABLE": table_filter} if table_filter else None
    _run_glue_step("aggregates", extra_arguments=extra, wait=wait)


@glue_cmd.command("run-all")
@click.option(
    "--row-cap",
    default=STAGING_DEFAULT_ROW_CAP,
    show_default=True,
    help="Max rows to load per staging table (0 = no limit). Any raw table above the cap "
    "is truncated to it -- the same rule the lakehouse repo applies to its Gold fact tables.",
)
@click.option(
    "--fresh-reload",
    is_flag=True,
    help="TRUNCATE every staging/warehouse data table first (see `truncate-all` --  "
    "warehouse.etl_process_log is never touched, so cross-run comparisons still work), "
    "re-seed warehouse.dim_date (truncated along with everything else, but facts need "
    "it present to run at all), then run the full staging -> dimensions -> facts -> "
    "aggregates sequence from empty.",
)
def glue_run_all(row_cap: int, fresh_reload: bool) -> None:
    """Run staging -> dimensions -> facts -> aggregates in order, via Glue -- the
    Glue-backed equivalent of `load-warehouse` (same sequence, run locally). Run
    `mimic-load seed-date-dim` first (facts join dim_date, same requirement as
    load-warehouse) unless --fresh-reload, which does that for you. Stops at the first
    job run that doesn't SUCCEED (_run_glue_step raises a ClickException on any
    non-SUCCEEDED state)."""
    aws_settings = load_aws_settings()
    settings = load_settings()

    if fresh_reload:
        engine = make_engine(settings)
        tables = truncate_all(engine)
        click.echo(f"[fresh-reload] Truncated {len(tables)} table(s). warehouse.etl_process_log preserved.")
        days = seed_date_dim(engine, SEED_START_DATE, SEED_END_DATE)
        click.echo(f"[fresh-reload] Re-seeded warehouse.dim_date: {days} days.")

    # Computed once and forced into all 4 steps below (each is an independently launched
    # Glue job, unlike the lakehouse's single long-lived orchestrator process) so every
    # row this run-all invocation logs to warehouse.etl_process_log shares one full_run_id.
    full_run_id = _next_full_run_id(settings)
    click.echo(f"[process-log] logging job timings to warehouse.etl_process_log (full_run_id {full_run_id})")

    _run_glue_step(
        "staging",
        extra_arguments={
            "--S3_BUCKET": aws_settings.s3_bucket,
            "--MODULES": "hosp,icu",
            "--ROW_CAP": str(row_cap),
            "--FULL_RUN_ID": str(full_run_id),
        },
        wait=True,
    )
    for step in ("dimensions", "facts", "aggregates"):
        _run_glue_step(step, extra_arguments={"--FULL_RUN_ID": str(full_run_id)}, wait=True)


if __name__ == "__main__":
    main()
