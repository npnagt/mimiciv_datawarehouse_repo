"""AWS Glue provisioning: IAM role, JDBC connection to Aurora, and the 5 Glue jobs
under glue_jobs/ (schema bootstrap, staging load, warehouse dimensions, warehouse
facts, warehouse aggregates).

Job names are built as "<glue_job_name_prefix>-<step suffix>" (see GLUE_JOB_STEPS) --
e.g. "mimic4-dw-etl-stg-load", "mimic4-dw-etl-wh-facts". The "dw-etl" segment in the
default prefix plus each step's "stg"/"wh" marker keep these jobs identifiable in an AWS
account/region that also runs other projects' Glue jobs.
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from ..config import project_tags, project_tags_list

logger = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).resolve().parents[3]
GLUE_SCRIPTS_DIR = _REPO_ROOT / "glue_jobs"
GLUE_SCRIPT_PREFIX = "glue-scripts"
GLUE_SQL_DIR = _REPO_ROOT / "sql"
GLUE_SQL_PREFIX = "glue-sql"
# Must match glue_jobs/schema_bootstrap.py's SQL_FILES -- the auto-generated DDL exports
# (scripts/export_staging_ddl.py / export_warehouse_ddl.py) it applies to a fresh cluster.
GLUE_SQL_FILES = ["staging_tables.sql", "warehouse_tables.sql"]
# Where PySpark jobs write Spark event logs for the Spark UI (see create_or_update_job) --
# viewable via Glue Studio's "Spark UI" tab, or a self-hosted Spark History Server pointed
# at this S3 prefix. Requires s3:PutObject there too, not just read (see create_glue_role).
SPARK_EVENT_LOGS_PREFIX = "spark-logs"

# step key -> (job name suffix, entrypoint script, Glue job type). "bootstrap" is a
# lightweight Python Shell job (plain DDL via psycopg2, no Spark needed); the other 4
# are PySpark ("glueetl") jobs, sharing glue_common.py via --extra-py-files.
GLUE_JOB_STEPS: dict[str, tuple[str, str, str]] = {
    "bootstrap": ("schema-bootstrap", "schema_bootstrap.py", "pythonshell"),
    "staging": ("stg-load", "staging_load.py", "glueetl"),
    "dimensions": ("wh-dimensions", "warehouse_dimensions.py", "glueetl"),
    "facts": ("wh-facts", "warehouse_facts.py", "glueetl"),
    "aggregates": ("wh-aggregates", "warehouse_aggregates.py", "glueetl"),
}
GLUE_COMMON_SCRIPT = "glue_common.py"

_TRUST_POLICY = {
    "Version": "2012-10-17",
    "Statement": [{"Effect": "Allow", "Principal": {"Service": "glue.amazonaws.com"}, "Action": "sts:AssumeRole"}],
}
_MANAGED_GLUE_SERVICE_POLICY_ARN = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"


def job_name(prefix: str, step: str) -> str:
    """The full Glue job name for `step` (one of GLUE_JOB_STEPS' keys), e.g.
    job_name("mimic4-dw-etl", "facts") -> "mimic4-dw-etl-wh-facts"."""
    suffix, _script, _job_type = GLUE_JOB_STEPS[step]
    return f"{prefix}-{suffix}"


def _s3_access_policy(bucket_name: str) -> dict:
    return {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": ["s3:GetObject"], "Resource": f"arn:aws:s3:::{bucket_name}/*"},
            {"Effect": "Allow", "Action": ["s3:ListBucket"], "Resource": f"arn:aws:s3:::{bucket_name}"},
            # Write access, scoped to the Spark-UI event-logs prefix only (everything
            # else this role touches is read-only) -- see SPARK_EVENT_LOGS_PREFIX.
            {
                "Effect": "Allow",
                "Action": ["s3:PutObject", "s3:GetObject"],
                "Resource": f"arn:aws:s3:::{bucket_name}/{SPARK_EVENT_LOGS_PREFIX}/*",
            },
        ],
    }


def create_glue_role(role_name: str, s3_bucket: str) -> str:
    """Create (or reuse) the IAM role Glue assumes to run these jobs: the managed
    AWSGlueServiceRole policy (CloudWatch Logs, Glue Data Catalog, ENI management for VPC
    jobs) plus an inline policy scoping S3 read access to `s3_bucket` (raw data + job
    scripts). Returns the role's ARN."""
    iam = boto3.client("iam")
    try:
        role_arn = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(_TRUST_POLICY),
            Description="Allows AWS Glue to run the MIMIC-IV data warehouse ETL jobs.",
            Tags=project_tags_list(),
        )["Role"]["Arn"]
        logger.info("Created IAM role %s", role_arn)
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "EntityAlreadyExists":
            role_arn = iam.get_role(RoleName=role_name)["Role"]["Arn"]
            logger.info("IAM role %s already exists", role_arn)
        else:
            raise

    iam.attach_role_policy(RoleName=role_name, PolicyArn=_MANAGED_GLUE_SERVICE_POLICY_ARN)
    iam.put_role_policy(
        RoleName=role_name,
        # Name unchanged (still "-s3-read") even though the policy now also grants
        # scoped write access for Spark UI event logs -- put_role_policy overwrites an
        # existing inline policy by name, so renaming it would leave an orphaned
        # duplicate on every account this already ran against instead of updating in place.
        PolicyName=f"{role_name}-s3-read",
        PolicyDocument=json.dumps(_s3_access_policy(s3_bucket)),
    )
    return role_arn


def get_db_cluster_resource_id(cluster_id: str, region: str) -> str:
    """The stable resource ID RDS IAM auth policies key on (not the cluster identifier
    itself) -- see grant_rds_iam_connect."""
    rds = boto3.client("rds", region_name=region)
    return rds.describe_db_clusters(DBClusterIdentifier=cluster_id)["DBClusters"][0]["DbClusterResourceId"]


def grant_rds_iam_connect(role_name: str, region: str, db_cluster_resource_id: str, db_user: str) -> None:
    """Grant `role_name` (a Glue job's execution role) rds-db:connect on `db_user`, for
    RDS_AUTH_MODE=iam (see glue_common.py's docstring) -- needed so a Glue job can
    generate its own RDS auth token at runtime, the same permission config.py's docstring
    already requires of local AWS credentials for the IAM-auth local dev path."""
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    resource_arn = f"arn:aws:rds-db:{region}:{account_id}:dbuser:{db_cluster_resource_id}/{db_user}"
    policy = {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": ["rds-db:connect"], "Resource": resource_arn}],
    }
    iam = boto3.client("iam")
    iam.put_role_policy(
        RoleName=role_name,
        PolicyName=f"{role_name}-rds-iam-connect",
        PolicyDocument=json.dumps(policy),
    )
    logger.info("Granted rds-db:connect on %s to role %s", resource_arn, role_name)


def delete_glue_role(role_name: str) -> None:
    """Delete the IAM role created by `create_glue_role`, its inline policy, and detach
    the managed policy. A no-op if it doesn't exist."""
    iam = boto3.client("iam")
    try:
        for policy_name in iam.list_role_policies(RoleName=role_name)["PolicyNames"]:
            iam.delete_role_policy(RoleName=role_name, PolicyName=policy_name)
            logger.info("Deleted inline policy %s from role %s", policy_name, role_name)
        try:
            iam.detach_role_policy(RoleName=role_name, PolicyArn=_MANAGED_GLUE_SERVICE_POLICY_ARN)
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "NoSuchEntity":
                raise
        iam.delete_role(RoleName=role_name)
        logger.info("Deleted IAM role %s", role_name)
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "NoSuchEntity":
            logger.info("IAM role %s does not exist -- nothing to delete", role_name)
        else:
            raise


def upload_job_scripts(bucket_name: str, region: str) -> list[str]:
    """Upload every glue_jobs/*.py script (entrypoints + the shared glue_common.py) to
    s3://<bucket>/glue-scripts/. Returns the list of keys uploaded."""
    s3 = boto3.client("s3", region_name=region)
    scripts = [GLUE_COMMON_SCRIPT] + [script for _suffix, script, _job_type in GLUE_JOB_STEPS.values()]
    uploaded: list[str] = []
    for script in scripts:
        key = f"{GLUE_SCRIPT_PREFIX}/{script}"
        s3.upload_file(str(GLUE_SCRIPTS_DIR / script), bucket_name, key)
        uploaded.append(key)
        logger.info("Uploaded s3://%s/%s", bucket_name, key)
    return uploaded


def upload_sql_ddl(bucket_name: str, region: str) -> list[str]:
    """Upload sql/staging_tables.sql and sql/warehouse_tables.sql to
    s3://<bucket>/glue-sql/, for the bootstrap job to apply to a fresh cluster. Returns
    the list of keys uploaded. Regenerate those files first (scripts/export_staging_ddl.py
    / export_warehouse_ddl.py) if staging.py/warehouse.py changed."""
    s3 = boto3.client("s3", region_name=region)
    uploaded: list[str] = []
    for filename in GLUE_SQL_FILES:
        key = f"{GLUE_SQL_PREFIX}/{filename}"
        s3.upload_file(str(GLUE_SQL_DIR / filename), bucket_name, key)
        uploaded.append(key)
        logger.info("Uploaded s3://%s/%s", bucket_name, key)
    return uploaded


def create_glue_connection(
    connection_name: str,
    *,
    rds_host: str,
    rds_port: int,
    rds_database: str,
    rds_user: str,
    rds_password: str,
    subnet_id: str = "",
    security_group_ids: list[str] | None = None,
    availability_zone: str = "",
) -> None:
    """Create (or update) the Glue JDBC connection Glue jobs use to reach Aurora.

    `subnet_id` is only needed for a VPC-attached, non-publicly-reachable cluster
    (Glue jobs run on ENIs placed in that subnet, same constraint as aurora.py's
    "PubliclyAccessible=False" standard cluster) -- pass "" to omit VPC placement
    entirely for a cluster reachable over the internet instead, e.g. one provisioned
    via Aurora's express configuration (see aurora.py's docstring): no VPC, IAM
    database auth only, but internet-reachable. `rds_password` is ignored (pass "")
    when the caller intends RDS_AUTH_MODE=iam -- glue_common.py generates its own
    token per connection at runtime instead of using a stored password."""
    glue = boto3.client("glue")
    connection_input = {
        "Name": connection_name,
        "ConnectionType": "JDBC",
        "ConnectionProperties": {
            "JDBC_CONNECTION_URL": f"jdbc:postgresql://{rds_host}:{rds_port}/{rds_database}",
            "USERNAME": rds_user,
            "PASSWORD": rds_password,
        },
    }
    if subnet_id:
        connection_input["PhysicalConnectionRequirements"] = {
            "SubnetId": subnet_id,
            "SecurityGroupIdList": security_group_ids or [],
            "AvailabilityZone": availability_zone,
        }
    try:
        glue.create_connection(ConnectionInput=connection_input, Tags=project_tags())
        logger.info("Created Glue connection %s", connection_name)
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "AlreadyExistsException":
            glue.update_connection(Name=connection_name, ConnectionInput=connection_input)
            logger.info("Updated Glue connection %s", connection_name)
        else:
            raise


def delete_glue_connection(connection_name: str) -> None:
    glue = boto3.client("glue")
    try:
        glue.delete_connection(ConnectionName=connection_name)
        logger.info("Deleted Glue connection %s", connection_name)
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "EntityNotFoundException":
            logger.info("Glue connection %s does not exist -- nothing to delete", connection_name)
        else:
            raise


def create_or_update_job(
    step: str,
    *,
    job_name_prefix: str,
    s3_bucket: str,
    role_arn: str,
    connection_name: str | None = None,
    worker_type: str = "G.1X",
    num_workers: int = 2,
    glue_version: str = "4.0",
) -> str:
    """Create (or update, if it already exists) the Glue job for `step` (a key of
    GLUE_JOB_STEPS). `connection_name` is only needed for a VPC-attached target (see
    create_glue_connection) -- pass None to run with Glue's default (non-VPC,
    internet-reachable) networking. Returns the job name."""
    name = job_name(job_name_prefix, step)
    _suffix, script, job_type = GLUE_JOB_STEPS[step]

    job_command = {
        "Name": job_type,
        "ScriptLocation": f"s3://{s3_bucket}/{GLUE_SCRIPT_PREFIX}/{script}",
        "PythonVersion": "3" if job_type == "glueetl" else "3.9",
    }
    # Every job connects to Postgres via psycopg2 (glue_common.pg_connect, or
    # schema_bootstrap.py's self-contained equivalent) -- neither Glue runtime ships a
    # working one out of the box: PySpark jobs don't have psycopg2 at all
    # (ModuleNotFoundError), and the Python Shell runtime's built-in one is linked
    # against a system OpenSSL too old to complete a TLS handshake with RDS ("SSL error:
    # sslv3 alert handshake failure") -- both discovered running this against a real
    # Aurora endpoint. psycopg2-binary bundles its own modern OpenSSL and installs
    # cleanly in both runtimes, so it fixes both problems the same way.
    default_arguments = {"--job-language": "python", "--additional-python-modules": "psycopg2-binary==2.9.9"}

    job_kwargs: dict = {
        "Role": role_arn,
        "Command": job_command,
        "DefaultArguments": default_arguments,
        "GlueVersion": glue_version,
        # Defense in depth against exactly what happened on a real run: statement_timeout
        # (glue_common.pg_connect / schema_bootstrap._pg_connect) bounds each SQL query,
        # but a hang before ever reaching one -- e.g. the 20+ minute `pip install
        # psycopg2-binary` stall this job type hit at its old, smaller MaxCapacity (see
        # below) -- isn't a SQL query, so no statement_timeout catches it. Glue's own
        # per-job-run Timeout is the backstop for that. Defaults (2880 min for glueetl,
        # comparable for pythonshell) are far too long to matter as a backstop.
        "Timeout": 60 if job_type == "pythonshell" else 120,
    }
    if connection_name:
        job_kwargs["Connections"] = {"Connections": [connection_name]}
    if job_type == "glueetl":
        # Only the PySpark jobs import glue_common.py -- the pythonshell bootstrap job
        # is deliberately self-contained (see its docstring: glue_common.py imports
        # awsglue.context/pyspark.sql, neither available in the Python Shell runtime).
        # WorkerType/NumberOfWorkers are also glueetl-only -- a pythonshell job uses
        # MaxCapacity instead, and rejects these two.
        default_arguments["--extra-py-files"] = f"s3://{s3_bucket}/{GLUE_SCRIPT_PREFIX}/{GLUE_COMMON_SCRIPT}"
        job_kwargs["WorkerType"] = worker_type
        job_kwargs["NumberOfWorkers"] = num_workers
        # Spark UI: shows the actual stage/task breakdown Spark chose (shuffle sizes,
        # skew, time per stage) -- a level of detail the print()-based logging in
        # glue_common.py can't give, useful for exactly the kind of "why is this job
        # slow" question a stuck/slow run raises. Viewable via Glue Studio's job run
        # "Spark UI" tab (reads the event log this writes to S3), or a self-hosted Spark
        # History Server pointed at the same prefix.
        default_arguments["--enable-spark-ui"] = "true"
        default_arguments["--spark-event-logs-path"] = f"s3://{s3_bucket}/{SPARK_EVENT_LOGS_PREFIX}/"
    else:
        # 1 DPU, not Python Shell's smaller 0.0625 option: two separate real runs of the
        # bootstrap job hung for 20+ minutes at the exact same step (`pip install
        # psycopg2-binary` via --additional-python-modules) at 0.0625 DPU before either
        # completing or erroring -- consistent with that tier being too resource/
        # bandwidth-constrained to reliably finish the install, not a database issue (the
        # hang was well before the job ever opens a DB connection). 1 DPU is Python
        # Shell's only other allowed value.
        job_kwargs["MaxCapacity"] = 1

    glue = boto3.client("glue")
    try:
        # Tags are a create_job-only kwarg -- update_job's JobUpdate rejects them, so
        # they can't live in job_kwargs (which is reused below for update_job).
        glue.create_job(Name=name, Tags=project_tags(), **job_kwargs)
        logger.info("Created Glue job %s", name)
    except ClientError as exc:
        # AlreadyExistsException: job exists with identical config (harmless resubmit).
        # IdempotentParameterMismatchException: job exists with *different* config --
        # what create_job actually raises for a job whose script/settings changed since
        # it was first created (discovered against real AWS; moto's mock only ever
        # raises AlreadyExistsException, regardless of whether the config differs).
        if exc.response["Error"]["Code"] in ("AlreadyExistsException", "IdempotentParameterMismatchException"):
            glue.update_job(JobName=name, JobUpdate=job_kwargs)
            logger.info("Updated Glue job %s", name)
        else:
            raise
    return name


def delete_glue_job(job_name_prefix: str, step: str) -> None:
    name = job_name(job_name_prefix, step)
    glue = boto3.client("glue")
    try:
        glue.delete_job(JobName=name)
        logger.info("Deleted Glue job %s", name)
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "EntityNotFoundException":
            logger.info("Glue job %s does not exist -- nothing to delete", name)
        else:
            raise


def start_job_run(name: str, arguments: dict[str, str] | None = None) -> str:
    """Start a run of Glue job `name`. Returns the run ID."""
    glue = boto3.client("glue")
    run_id = glue.start_job_run(JobName=name, Arguments=arguments or {})["JobRunId"]
    logger.info("Started Glue job %s, run %s", name, run_id)
    return run_id


def get_job_run_state(name: str, run_id: str) -> str:
    glue = boto3.client("glue")
    return glue.get_job_run(JobName=name, RunId=run_id)["JobRun"]["JobRunState"]


def get_job_run_error(name: str, run_id: str) -> str | None:
    glue = boto3.client("glue")
    return glue.get_job_run(JobName=name, RunId=run_id)["JobRun"].get("ErrorMessage")


def get_job_run_dpu_seconds(name: str, run_id: str) -> float | None:
    """DPUSeconds for a completed job run -- the same unit Glue bills in, and the field
    the lakehouse repo's etl_process_log.dpu_seconds mirrors. Only meaningful once the run
    has reached a terminal state; Glue computes it as the run progresses but it's read
    here, not pushed, so callers fetch it after run_job_and_wait returns."""
    glue = boto3.client("glue")
    return glue.get_job_run(JobName=name, RunId=run_id)["JobRun"].get("DPUSeconds")


_TERMINAL_STATES = {"SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT", "ERROR"}


def wait_for_job_run(name: str, run_id: str, *, poll_seconds: int = 15, timeout_seconds: int = 3600) -> str:
    """Block until job run `run_id` of job `name` reaches a terminal state. Returns that
    state (one of _TERMINAL_STATES)."""
    deadline = time.monotonic() + timeout_seconds
    state = get_job_run_state(name, run_id)
    while state not in _TERMINAL_STATES:
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"Glue job {name} run {run_id} did not finish within {timeout_seconds}s (last state: {state})"
            )
        time.sleep(poll_seconds)
        state = get_job_run_state(name, run_id)
        logger.info("Glue job %s run %s state: %s", name, run_id, state)
    return state


# A Glue job started immediately after `glue deploy` creates a brand-new execution role
# can fail instantly with this message -- the role's trust policy just hasn't finished
# propagating through IAM yet (an eventual-consistency delay, not a real permissions
# problem). Discovered against a real run: `glue deploy` immediately followed by
# `glue run-bootstrap` hit this on a freshly created role.
_IAM_PROPAGATION_SIGNATURE = "should be given assume role permissions for Glue Service"


def run_job_and_wait(
    name: str,
    arguments: dict[str, str] | None = None,
    *,
    poll_seconds: int = 15,
    timeout_seconds: int = 3600,
    max_iam_retries: int = 6,
    iam_retry_delay_seconds: int = 15,
) -> tuple[str, str]:
    """start_job_run + wait_for_job_run, retrying (up to `max_iam_retries` times, waiting
    `iam_retry_delay_seconds` between attempts) if the run fails specifically due to IAM
    role propagation lag (see _IAM_PROPAGATION_SIGNATURE) rather than failing the whole
    pipeline on a race condition that resolves itself within seconds to a couple of
    minutes. Any other failure is returned immediately, no retry. Returns
    (final_state, run_id) -- the run_id is the one that produced the final state (a
    retried attempt gets a new run_id each time), needed by callers that backfill
    etl_process_log.dpu_seconds via get_job_run_dpu_seconds(name, run_id) afterwards."""
    state = "FAILED"
    run_id = ""
    for attempt in range(max_iam_retries):
        run_id = start_job_run(name, arguments)
        state = wait_for_job_run(name, run_id, poll_seconds=poll_seconds, timeout_seconds=timeout_seconds)
        if state != "FAILED":
            return state, run_id
        error = get_job_run_error(name, run_id) or ""
        if _IAM_PROPAGATION_SIGNATURE not in error:
            return state, run_id
        if attempt < max_iam_retries - 1:
            logger.warning(
                "Glue job %s run %s failed due to IAM role propagation delay (attempt %d/%d) -- retrying in %ss",
                name,
                run_id,
                attempt + 1,
                max_iam_retries,
                iam_retry_delay_seconds,
            )
            time.sleep(iam_retry_delay_seconds)
    return state, run_id
