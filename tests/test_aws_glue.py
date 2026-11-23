"""Tests for aws/glue.py.

moto's Glue backend (as of the pinned moto version -- see pyproject.toml) implements
create_job/get_job/delete_job/start_job_run/get_job_run and create_connection/
get_connection, but not update_job/update_connection/delete_connection. Those branches
(the "already exists -> update" idempotency path, and connection teardown) are exercised
against a mocked boto3 client instead, verifying the right API call is made with the
right arguments -- moto-based tests are used everywhere moto actually supports the call,
matching the style of tests/test_aws.py / tests/test_aws_teardown.py.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from mimic_datawarehouse.aws.glue import (
    GLUE_JOB_STEPS,
    GLUE_SCRIPT_PREFIX,
    GLUE_SQL_FILES,
    GLUE_SQL_PREFIX,
    create_glue_connection,
    create_glue_role,
    create_or_update_job,
    delete_glue_connection,
    delete_glue_job,
    delete_glue_role,
    get_db_cluster_resource_id,
    get_job_run_dpu_seconds,
    grant_rds_iam_connect,
    job_name,
    run_job_and_wait,
    start_job_run,
    upload_job_scripts,
    upload_sql_ddl,
    wait_for_job_run,
)

REGION = "us-east-2"
BUCKET = "mimic4-dw-sourcefiles-test"
ROLE_NAME = "mimic4-dw-etl-glue-role-test"
CONNECTION_NAME = "mimic4-dw-etl-aurora-connection-test"
JOB_PREFIX = "mimic4-dw-etl-test"


@pytest.fixture(autouse=True)
def _fake_aws_credentials(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    # Needed for moto to recognize AWS-managed policy ARNs like AWSGlueServiceRole --
    # off by default (loading the full managed-policy catalog is expensive).
    monkeypatch.setenv("MOTO_IAM_LOAD_MANAGED_POLICIES", "true")


def _client_error(code: str, operation: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, operation)


def test_job_name_uses_prefix_and_step_suffix():
    assert job_name("mimic4-dw-etl", "staging") == "mimic4-dw-etl-stg-load"
    assert job_name("mimic4-dw-etl", "facts") == "mimic4-dw-etl-wh-facts"


@mock_aws
def test_create_glue_role_is_idempotent_and_grants_glue_service_policy():
    role_arn_1 = create_glue_role(ROLE_NAME, BUCKET)
    role_arn_2 = create_glue_role(ROLE_NAME, BUCKET)
    assert role_arn_1 == role_arn_2

    iam = boto3.client("iam")
    attached = iam.list_attached_role_policies(RoleName=ROLE_NAME)["AttachedPolicies"]
    assert any(p["PolicyName"] == "AWSGlueServiceRole" for p in attached)
    inline = iam.list_role_policies(RoleName=ROLE_NAME)["PolicyNames"]
    assert f"{ROLE_NAME}-s3-read" in inline


@mock_aws
def test_upload_job_scripts_uploads_every_entrypoint_and_common_module():
    s3 = boto3.client("s3", region_name=REGION)
    s3.create_bucket(Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": REGION})

    keys = upload_job_scripts(BUCKET, REGION)

    expected = {f"{GLUE_SCRIPT_PREFIX}/glue_common.py"} | {
        f"{GLUE_SCRIPT_PREFIX}/{script}" for _suffix, script, _job_type in GLUE_JOB_STEPS.values()
    }
    assert set(keys) == expected

    uploaded = {obj["Key"] for obj in s3.list_objects_v2(Bucket=BUCKET)["Contents"]}
    assert uploaded == expected


@mock_aws
def test_upload_sql_ddl_uploads_staging_and_warehouse_ddl():
    s3 = boto3.client("s3", region_name=REGION)
    s3.create_bucket(Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": REGION})

    keys = upload_sql_ddl(BUCKET, REGION)

    expected = {f"{GLUE_SQL_PREFIX}/{filename}" for filename in GLUE_SQL_FILES}
    assert set(keys) == expected
    uploaded = {obj["Key"] for obj in s3.list_objects_v2(Bucket=BUCKET)["Contents"]}
    assert uploaded == expected


@mock_aws
def test_create_or_update_job_creates_pythonshell_bootstrap_job():
    iam = boto3.client("iam")
    role_arn = iam.create_role(RoleName=ROLE_NAME, AssumeRolePolicyDocument="{}")["Role"]["Arn"]

    name = create_or_update_job(
        "bootstrap",
        job_name_prefix=JOB_PREFIX,
        s3_bucket=BUCKET,
        role_arn=role_arn,
        connection_name=CONNECTION_NAME,
    )
    assert name == f"{JOB_PREFIX}-schema-bootstrap"

    glue = boto3.client("glue", region_name=REGION)
    job = glue.get_job(JobName=name)["Job"]
    assert job["Command"]["Name"] == "pythonshell"
    assert job["Command"]["ScriptLocation"] == f"s3://{BUCKET}/{GLUE_SCRIPT_PREFIX}/schema_bootstrap.py"
    # schema_bootstrap.py is deliberately self-contained -- see its docstring for why it
    # can't import glue_common.py (which needs the PySpark-only awsglue submodules).
    assert "--extra-py-files" not in job["DefaultArguments"]
    # Works around the Python Shell runtime's too-old built-in psycopg2/OpenSSL (see
    # create_or_update_job's comment) by pulling in a modern, self-bundled OpenSSL.
    assert job["DefaultArguments"]["--additional-python-modules"] == "psycopg2-binary==2.9.9"
    # WorkerType/NumberOfWorkers are glueetl-only -- a pythonshell job rejects them.
    assert "WorkerType" not in job


@mock_aws
def test_create_or_update_job_omits_connections_when_no_vpc_needed():
    iam = boto3.client("iam")
    role_arn = iam.create_role(RoleName=ROLE_NAME, AssumeRolePolicyDocument="{}")["Role"]["Arn"]

    name = create_or_update_job(
        "staging", job_name_prefix=JOB_PREFIX, s3_bucket=BUCKET, role_arn=role_arn, connection_name=None
    )

    glue = boto3.client("glue", region_name=REGION)
    job = glue.get_job(JobName=name)["Job"]
    assert job.get("Connections", {}).get("Connections", []) == []


def test_get_db_cluster_resource_id_returns_stable_resource_id():
    mock_rds = MagicMock()
    mock_rds.describe_db_clusters.return_value = {"DBClusters": [{"DbClusterResourceId": "cluster-ABC123"}]}
    with patch("mimic_datawarehouse.aws.glue.boto3.client", return_value=mock_rds):
        resource_id = get_db_cluster_resource_id("mimic4rds", REGION)
    assert resource_id == "cluster-ABC123"


def test_grant_rds_iam_connect_scopes_policy_to_db_user():
    mock_sts = MagicMock()
    mock_sts.get_caller_identity.return_value = {"Account": "123456789012"}
    mock_iam = MagicMock()

    def _client(service, *a, **kw):
        return {"sts": mock_sts, "iam": mock_iam}[service]

    with patch("mimic_datawarehouse.aws.glue.boto3.client", side_effect=_client):
        grant_rds_iam_connect(ROLE_NAME, REGION, "cluster-ABC123", "mimic_admin")

    mock_iam.put_role_policy.assert_called_once()
    kwargs = mock_iam.put_role_policy.call_args.kwargs
    assert kwargs["RoleName"] == ROLE_NAME
    assert "arn:aws:rds-db:us-east-2:123456789012:dbuser:cluster-ABC123/mimic_admin" in kwargs["PolicyDocument"]


@mock_aws
def test_create_glue_connection_creates_jdbc_connection():
    create_glue_connection(
        CONNECTION_NAME,
        rds_host="db.example.com",
        rds_port=5432,
        rds_database="mimic4rds",
        rds_user="mimic_admin",
        rds_password="secret",
        subnet_id="subnet-123",
        security_group_ids=["sg-123"],
        availability_zone="us-east-2a",
    )

    glue = boto3.client("glue", region_name=REGION)
    conn = glue.get_connection(Name=CONNECTION_NAME)["Connection"]
    # moto's Glue backend doesn't echo ConnectionType back in get_connection (unlike
    # real AWS) -- ConnectionProperties/PhysicalConnectionRequirements are what it models.
    assert conn["ConnectionProperties"]["JDBC_CONNECTION_URL"] == "jdbc:postgresql://db.example.com:5432/mimic4rds"
    assert conn["PhysicalConnectionRequirements"]["SubnetId"] == "subnet-123"


def test_create_glue_connection_updates_when_already_exists():
    """moto's Glue backend doesn't implement update_connection, so the idempotent
    "already exists -> update" branch is verified against a mocked client instead."""
    mock_glue = MagicMock()
    mock_glue.create_connection.side_effect = _client_error("AlreadyExistsException", "CreateConnection")

    with patch("mimic_datawarehouse.aws.glue.boto3.client", return_value=mock_glue):
        create_glue_connection(
            CONNECTION_NAME,
            rds_host="db.example.com",
            rds_port=5432,
            rds_database="mimic4rds",
            rds_user="mimic_admin",
            rds_password="secret",
            subnet_id="subnet-123",
            security_group_ids=["sg-123"],
            availability_zone="us-east-2a",
        )

    mock_glue.update_connection.assert_called_once()
    assert mock_glue.update_connection.call_args.kwargs["Name"] == CONNECTION_NAME


def test_delete_glue_connection_deletes_and_is_a_no_op_when_missing():
    mock_glue = MagicMock()
    with patch("mimic_datawarehouse.aws.glue.boto3.client", return_value=mock_glue):
        delete_glue_connection(CONNECTION_NAME)
    mock_glue.delete_connection.assert_called_once_with(ConnectionName=CONNECTION_NAME)

    mock_glue_missing = MagicMock()
    mock_glue_missing.delete_connection.side_effect = _client_error("EntityNotFoundException", "DeleteConnection")
    with patch("mimic_datawarehouse.aws.glue.boto3.client", return_value=mock_glue_missing):
        delete_glue_connection(CONNECTION_NAME)  # must not raise


@mock_aws
def test_create_or_update_job_creates_job_with_expected_command_and_connection():
    iam = boto3.client("iam")
    role_arn = iam.create_role(RoleName=ROLE_NAME, AssumeRolePolicyDocument="{}")["Role"]["Arn"]

    name = create_or_update_job(
        "facts",
        job_name_prefix=JOB_PREFIX,
        s3_bucket=BUCKET,
        role_arn=role_arn,
        connection_name=CONNECTION_NAME,
    )
    assert name == f"{JOB_PREFIX}-wh-facts"

    glue = boto3.client("glue", region_name=REGION)
    job = glue.get_job(JobName=name)["Job"]
    assert job["Command"]["ScriptLocation"] == f"s3://{BUCKET}/{GLUE_SCRIPT_PREFIX}/warehouse_facts.py"
    assert job["Connections"]["Connections"] == [CONNECTION_NAME]
    # PySpark jobs don't ship psycopg2 at all -- see create_or_update_job's comment.
    assert job["DefaultArguments"]["--additional-python-modules"] == "psycopg2-binary==2.9.9"


def test_create_or_update_job_updates_when_config_differs_from_existing():
    """Real AWS raises IdempotentParameterMismatchException (not AlreadyExistsException)
    when a job with this name exists but its config differs -- discovered running this
    against a real account; moto's mock only ever raises AlreadyExistsException."""
    mock_glue = MagicMock()
    mock_glue.create_job.side_effect = _client_error("IdempotentParameterMismatchException", "CreateJob")

    with patch("mimic_datawarehouse.aws.glue.boto3.client", return_value=mock_glue):
        name = create_or_update_job(
            "facts",
            job_name_prefix=JOB_PREFIX,
            s3_bucket=BUCKET,
            role_arn="arn:aws:iam::123456789012:role/r",
            connection_name=CONNECTION_NAME,
        )

    assert name == f"{JOB_PREFIX}-wh-facts"
    mock_glue.update_job.assert_called_once()


def test_create_or_update_job_updates_when_already_exists():
    """moto's Glue backend doesn't implement update_job, so the idempotent
    "already exists -> update" branch is verified against a mocked client instead."""
    mock_glue = MagicMock()
    mock_glue.create_job.side_effect = _client_error("AlreadyExistsException", "CreateJob")

    with patch("mimic_datawarehouse.aws.glue.boto3.client", return_value=mock_glue):
        name = create_or_update_job(
            "staging",
            job_name_prefix=JOB_PREFIX,
            s3_bucket=BUCKET,
            role_arn="arn:aws:iam::123456789012:role/r",
            connection_name=CONNECTION_NAME,
        )

    assert name == f"{JOB_PREFIX}-stg-load"
    mock_glue.update_job.assert_called_once()
    assert mock_glue.update_job.call_args.kwargs["JobName"] == name


@mock_aws
def test_delete_glue_job_is_a_no_op_when_missing():
    delete_glue_job(JOB_PREFIX, "aggregates")  # must not raise even though it never existed


@mock_aws
def test_start_job_run_and_wait_for_job_run_returns_terminal_state():
    iam = boto3.client("iam")
    role_arn = iam.create_role(RoleName=ROLE_NAME, AssumeRolePolicyDocument="{}")["Role"]["Arn"]
    name = create_or_update_job(
        "dimensions",
        job_name_prefix=JOB_PREFIX,
        s3_bucket=BUCKET,
        role_arn=role_arn,
        connection_name=CONNECTION_NAME,
    )

    run_id = start_job_run(name, {"--RDS_HOST": "db.example.com"})
    assert run_id

    state = wait_for_job_run(name, run_id, poll_seconds=0, timeout_seconds=5)
    assert state == "SUCCEEDED"


# Regression coverage for a real bug: `glue deploy` immediately followed by a job run
# against the brand-new execution role it just created failed instantly with "should be
# given assume role permissions for Glue Service" -- IAM trust-policy propagation lag,
# not a real permissions problem. run_job_and_wait retries specifically that failure.


def test_run_job_and_wait_retries_on_iam_propagation_delay_then_succeeds(monkeypatch):
    start = MagicMock(side_effect=["run-1", "run-2"])
    wait = MagicMock(side_effect=["FAILED", "SUCCEEDED"])
    get_error = MagicMock(return_value="... should be given assume role permissions for Glue Service ...")
    monkeypatch.setattr("mimic_datawarehouse.aws.glue.start_job_run", start)
    monkeypatch.setattr("mimic_datawarehouse.aws.glue.wait_for_job_run", wait)
    monkeypatch.setattr("mimic_datawarehouse.aws.glue.get_job_run_error", get_error)
    monkeypatch.setattr("mimic_datawarehouse.aws.glue.time.sleep", MagicMock())

    state, run_id = run_job_and_wait("job", {}, max_iam_retries=3, iam_retry_delay_seconds=0)

    assert state == "SUCCEEDED"
    assert run_id == "run-2"
    assert start.call_count == 2


def test_run_job_and_wait_does_not_retry_on_an_unrelated_failure(monkeypatch):
    start = MagicMock(return_value="run-1")
    wait = MagicMock(return_value="FAILED")
    get_error = MagicMock(return_value="some unrelated error message")
    monkeypatch.setattr("mimic_datawarehouse.aws.glue.start_job_run", start)
    monkeypatch.setattr("mimic_datawarehouse.aws.glue.wait_for_job_run", wait)
    monkeypatch.setattr("mimic_datawarehouse.aws.glue.get_job_run_error", get_error)

    state, run_id = run_job_and_wait("job", {}, max_iam_retries=3, iam_retry_delay_seconds=0)

    assert state == "FAILED"
    assert run_id == "run-1"
    start.assert_called_once()


def test_run_job_and_wait_gives_up_after_max_retries(monkeypatch):
    start = MagicMock(return_value="run-1")
    wait = MagicMock(return_value="FAILED")
    get_error = MagicMock(return_value="should be given assume role permissions for Glue Service")
    monkeypatch.setattr("mimic_datawarehouse.aws.glue.start_job_run", start)
    monkeypatch.setattr("mimic_datawarehouse.aws.glue.wait_for_job_run", wait)
    monkeypatch.setattr("mimic_datawarehouse.aws.glue.get_job_run_error", get_error)
    monkeypatch.setattr("mimic_datawarehouse.aws.glue.time.sleep", MagicMock())

    state, run_id = run_job_and_wait("job", {}, max_iam_retries=3, iam_retry_delay_seconds=0)

    assert state == "FAILED"
    assert run_id == "run-1"
    assert start.call_count == 3


def test_run_job_and_wait_succeeds_immediately_without_checking_error(monkeypatch):
    start = MagicMock(return_value="run-1")
    wait = MagicMock(return_value="SUCCEEDED")
    get_error = MagicMock()
    monkeypatch.setattr("mimic_datawarehouse.aws.glue.start_job_run", start)
    monkeypatch.setattr("mimic_datawarehouse.aws.glue.wait_for_job_run", wait)
    monkeypatch.setattr("mimic_datawarehouse.aws.glue.get_job_run_error", get_error)

    state, run_id = run_job_and_wait("job", {})

    assert state == "SUCCEEDED"
    assert run_id == "run-1"
    get_error.assert_not_called()


def test_get_job_run_dpu_seconds_reads_the_completed_run():
    mock_glue = MagicMock()
    mock_glue.get_job_run.return_value = {"JobRun": {"DPUSeconds": 12.5}}

    with patch("mimic_datawarehouse.aws.glue.boto3.client", return_value=mock_glue):
        assert get_job_run_dpu_seconds("job", "run-1") == 12.5


def test_delete_glue_role_is_idempotent():
    mock_iam = MagicMock()
    mock_iam.list_role_policies.return_value = {"PolicyNames": ["p1"]}
    with patch("mimic_datawarehouse.aws.glue.boto3.client", return_value=mock_iam):
        delete_glue_role(ROLE_NAME)
    mock_iam.delete_role_policy.assert_called_once_with(RoleName=ROLE_NAME, PolicyName="p1")
    mock_iam.detach_role_policy.assert_called_once()
    mock_iam.delete_role.assert_called_once_with(RoleName=ROLE_NAME)

    mock_iam_missing = MagicMock()
    mock_iam_missing.list_role_policies.side_effect = _client_error("NoSuchEntity", "ListRolePolicies")
    with patch("mimic_datawarehouse.aws.glue.boto3.client", return_value=mock_iam_missing):
        delete_glue_role(ROLE_NAME)  # must not raise
