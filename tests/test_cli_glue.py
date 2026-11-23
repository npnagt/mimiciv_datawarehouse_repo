"""Tests for the `mimic-load glue ...` command group in cli.py.

These mock mimic_datawarehouse.aws.glue directly (the boto3-facing layer, already
covered by tests/test_aws_glue.py) so the CLI's own logic -- which arguments it builds,
what order it runs jobs in, and that it stops the chain on a non-SUCCEEDED run -- is
tested in isolation, the same way a real Glue account is never touched here.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from mimic_datawarehouse import cli

# Captured before the autouse _env fixture below stubs cli._next_full_run_id, so the
# two tests that exercise the real function can restore it for themselves.
_REAL_NEXT_FULL_RUN_ID = cli._next_full_run_id


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("RDS_HOST", "db.example.com")
    monkeypatch.setenv("RDS_DATABASE", "mimic4rds")
    monkeypatch.setenv("RDS_USER", "mimic_admin")
    monkeypatch.setenv("RDS_PASSWORD", "secret")
    # Explicit, not inherited from a real .env this suite might run alongside (see
    # config.load_settings' default) -- tests that want RDS_AUTH_MODE=iam set it themselves.
    monkeypatch.setenv("RDS_AUTH_MODE", "password")
    monkeypatch.setenv("AURORA_MASTER_USERNAME", "mimic_admin")
    monkeypatch.setenv("S3_BUCKET_NAME", "mimic4-dw-sourcefiles-test")
    monkeypatch.setenv("GLUE_JOB_NAME_PREFIX", "mimic4-dw-etl-test")
    monkeypatch.setenv("GLUE_ROLE_NAME", "mimic4-dw-etl-glue-role-test")
    monkeypatch.setenv("GLUE_CONNECTION_NAME", "mimic4-dw-etl-aurora-connection-test")
    monkeypatch.delenv("GLUE_SUBNET_ID", raising=False)
    monkeypatch.delenv("MIMIC_DATASET", raising=False)
    # _next_full_run_id makes a real Postgres connection attempt (best-effort, falls back
    # to 1 on failure -- see its docstring); stub it so these CLI-argument-shape tests
    # stay hermetic and don't depend on how fast "db.example.com" fails to connect.
    monkeypatch.setattr(cli, "_next_full_run_id", lambda settings: 42)
    # _backfill_dpu_seconds calls glue.get_job_run_dpu_seconds, a REAL boto3 Glue API call
    # -- unlike the Postgres case above, an unmocked AWS call can be genuinely slow
    # (connection/retry timeouts), not just fail fast. Stub it so every _run_glue_step
    # test stays fast; test_cli_glue's own dpu-seconds tests override this explicitly.
    monkeypatch.setattr(cli.glue, "get_job_run_dpu_seconds", lambda name, run_id: None)


@pytest.fixture
def runner():
    return CliRunner()


def test_next_full_run_id_returns_max_plus_one(monkeypatch):
    monkeypatch.setattr(cli, "_next_full_run_id", _REAL_NEXT_FULL_RUN_ID)

    class FakeResult:
        def scalar(self):
            return 5

    class FakeConn:
        def execute(self, _stmt):
            return FakeResult()

        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

    class FakeEngine:
        def connect(self):
            return FakeConn()

    monkeypatch.setattr(cli, "make_engine", lambda settings: FakeEngine())
    assert cli._next_full_run_id(object()) == 5


def test_next_full_run_id_falls_back_to_1_on_connection_failure(monkeypatch):
    """warehouse.etl_process_log doesn't exist until `glue run-bootstrap` has run at
    least once -- this must degrade to 1, not raise and abort the caller."""
    monkeypatch.setattr(cli, "_next_full_run_id", _REAL_NEXT_FULL_RUN_ID)

    def _boom(_settings):
        raise RuntimeError("connection refused")

    monkeypatch.setattr(cli, "make_engine", _boom)
    assert cli._next_full_run_id(object()) == 1


def test_backfill_dpu_seconds_updates_every_row_of_the_run(monkeypatch):
    monkeypatch.setattr(cli.glue, "get_job_run_dpu_seconds", lambda name, run_id: 12.5)
    executed = []

    class FakeConn:
        def execute(self, stmt, params):
            executed.append((str(stmt), params))

        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

    class FakeEngine:
        def begin(self):
            return FakeConn()

    monkeypatch.setattr(cli, "make_engine", lambda settings: FakeEngine())

    cli._backfill_dpu_seconds(object(), "mimic4-dw-etl-test-stg-load", "run-1")

    assert len(executed) == 1
    sql, params = executed[0]
    assert "UPDATE warehouse.etl_process_log" in sql
    assert params == {"dpu": 12.5, "run_id": "run-1"}


def test_backfill_dpu_seconds_is_a_noop_when_dpu_seconds_unavailable(monkeypatch):
    monkeypatch.setattr(cli.glue, "get_job_run_dpu_seconds", lambda name, run_id: None)
    make_engine = MagicMock()
    monkeypatch.setattr(cli, "make_engine", make_engine)

    cli._backfill_dpu_seconds(object(), "job", "run-1")

    make_engine.assert_not_called()


def test_backfill_dpu_seconds_swallows_errors(monkeypatch):
    """A logging gap must never fail an otherwise-successful (or already-failed)
    pipeline step -- see the docstring."""

    def _boom(name, run_id):
        raise RuntimeError("Glue API unavailable")

    monkeypatch.setattr(cli.glue, "get_job_run_dpu_seconds", _boom)

    cli._backfill_dpu_seconds(object(), "job", "run-1")  # must not raise


def _patch_glue_deploy_dependencies(monkeypatch):
    upload_scripts = MagicMock()
    upload_sql = MagicMock()
    create_role = MagicMock(return_value="arn:aws:iam::123456789012:role/mimic4-dw-etl-glue-role")
    create_connection = MagicMock()
    create_job = MagicMock(side_effect=lambda step, **kw: cli.glue.job_name(kw["job_name_prefix"], step))
    get_resource_id = MagicMock(return_value="cluster-ABC123")
    grant_connect = MagicMock()
    monkeypatch.setattr(cli.glue, "upload_job_scripts", upload_scripts)
    monkeypatch.setattr(cli.glue, "upload_sql_ddl", upload_sql)
    monkeypatch.setattr(cli.glue, "create_glue_role", create_role)
    monkeypatch.setattr(cli.glue, "create_glue_connection", create_connection)
    monkeypatch.setattr(cli.glue, "create_or_update_job", create_job)
    monkeypatch.setattr(cli.glue, "get_db_cluster_resource_id", get_resource_id)
    monkeypatch.setattr(cli.glue, "grant_rds_iam_connect", grant_connect)
    return {
        "upload_scripts": upload_scripts,
        "upload_sql": upload_sql,
        "create_role": create_role,
        "create_connection": create_connection,
        "create_job": create_job,
        "get_resource_id": get_resource_id,
        "grant_connect": grant_connect,
    }


def test_glue_deploy_without_vpc_or_iam_skips_connection_and_iam_grant(monkeypatch, runner):
    """Default fixture env: no GLUE_SUBNET_ID, RDS_AUTH_MODE=password -- the common case
    for a publicly-reachable, password-authenticated target (or local dev)."""
    mocks = _patch_glue_deploy_dependencies(monkeypatch)

    result = runner.invoke(cli.main, ["glue", "deploy"])

    assert result.exit_code == 0, result.output
    mocks["upload_scripts"].assert_called_once()
    mocks["upload_sql"].assert_called_once()
    mocks["create_role"].assert_called_once()
    mocks["create_connection"].assert_not_called()
    mocks["grant_connect"].assert_not_called()
    assert mocks["create_job"].call_count == len(cli.glue.GLUE_JOB_STEPS)
    called_steps = {c.args[0] for c in mocks["create_job"].call_args_list}
    assert called_steps == set(cli.glue.GLUE_JOB_STEPS)
    assert all(c.kwargs["connection_name"] is None for c in mocks["create_job"].call_args_list)


def test_glue_deploy_with_vpc_subnet_creates_connection(monkeypatch, runner):
    monkeypatch.setenv("GLUE_SUBNET_ID", "subnet-123")
    monkeypatch.setenv("VPC_SECURITY_GROUP_IDS", "sg-123")
    monkeypatch.setenv("GLUE_AVAILABILITY_ZONE", "us-east-2a")
    mocks = _patch_glue_deploy_dependencies(monkeypatch)

    result = runner.invoke(cli.main, ["glue", "deploy"])

    assert result.exit_code == 0, result.output
    mocks["create_connection"].assert_called_once()
    assert mocks["create_connection"].call_args.kwargs["subnet_id"] == "subnet-123"
    assert all(
        c.kwargs["connection_name"] == "mimic4-dw-etl-aurora-connection-test" for c in mocks["create_job"].call_args_list
    )


def test_glue_deploy_with_iam_auth_grants_rds_connect(monkeypatch, runner):
    monkeypatch.setenv("RDS_AUTH_MODE", "iam")
    mocks = _patch_glue_deploy_dependencies(monkeypatch)

    result = runner.invoke(cli.main, ["glue", "deploy"])

    assert result.exit_code == 0, result.output
    mocks["get_resource_id"].assert_called_once()
    mocks["grant_connect"].assert_called_once()
    assert mocks["grant_connect"].call_args.args[0] == "mimic4-dw-etl-glue-role-test"
    assert mocks["grant_connect"].call_args.args[2] == "cluster-ABC123"
    assert mocks["grant_connect"].call_args.args[3] == "mimic_admin"


def test_glue_run_bootstrap_passes_sql_bucket(monkeypatch, runner):
    start = MagicMock(return_value="run-1")
    wait = MagicMock(return_value="SUCCEEDED")
    monkeypatch.setattr(cli.glue, "start_job_run", start)
    monkeypatch.setattr(cli.glue, "wait_for_job_run", wait)

    result = runner.invoke(cli.main, ["glue", "run-bootstrap"])

    assert result.exit_code == 0, result.output
    job_name_arg, arguments = start.call_args.args
    assert job_name_arg == "mimic4-dw-etl-test-schema-bootstrap"
    assert arguments["--SQL_S3_BUCKET"] == "mimic4-dw-sourcefiles-test"


def test_glue_run_staging_passes_bucket_modules_and_table_filter(monkeypatch, runner):
    start = MagicMock(return_value="run-1")
    wait = MagicMock(return_value="SUCCEEDED")
    monkeypatch.setattr(cli.glue, "start_job_run", start)
    monkeypatch.setattr(cli.glue, "wait_for_job_run", wait)

    result = runner.invoke(cli.main, ["glue", "run-staging", "--table", "patients", "--modules", "hosp"])

    assert result.exit_code == 0, result.output
    start.assert_called_once()
    job_name_arg, arguments = start.call_args.args
    assert job_name_arg == "mimic4-dw-etl-test-stg-load"
    assert arguments["--S3_BUCKET"] == "mimic4-dw-sourcefiles-test"
    assert arguments["--MODULES"] == "hosp"
    assert arguments["--TABLE"] == "patients"
    assert arguments["--RDS_HOST"] == "db.example.com"
    # Default per-table row cap -- the same 1M rule the lakehouse repo applies to its
    # Gold fact tables (see cli.STAGING_DEFAULT_ROW_CAP / etl.DEFAULT_ROW_CAP).
    assert arguments["--ROW_CAP"] == str(cli.STAGING_DEFAULT_ROW_CAP)


def test_glue_run_staging_backfills_dpu_seconds_for_the_run_that_finished(monkeypatch, runner):
    start = MagicMock(return_value="run-1")
    wait = MagicMock(return_value="SUCCEEDED")
    get_dpu = MagicMock(return_value=7.0)
    monkeypatch.setattr(cli.glue, "start_job_run", start)
    monkeypatch.setattr(cli.glue, "wait_for_job_run", wait)
    monkeypatch.setattr(cli.glue, "get_job_run_dpu_seconds", get_dpu)

    result = runner.invoke(cli.main, ["glue", "run-staging"])

    assert result.exit_code == 0, result.output
    get_dpu.assert_called_once_with("mimic4-dw-etl-test-stg-load", "run-1")


def test_glue_run_staging_forwards_row_cap_override(monkeypatch, runner):
    start = MagicMock(return_value="run-1")
    wait = MagicMock(return_value="SUCCEEDED")
    monkeypatch.setattr(cli.glue, "start_job_run", start)
    monkeypatch.setattr(cli.glue, "wait_for_job_run", wait)

    result = runner.invoke(cli.main, ["glue", "run-staging", "--row-cap", "0"])

    assert result.exit_code == 0, result.output
    _job_name_arg, arguments = start.call_args.args
    assert arguments["--ROW_CAP"] == "0"


def test_glue_run_all_forwards_row_cap_to_the_staging_step(monkeypatch, runner):
    start = MagicMock(return_value="run-1")
    wait = MagicMock(return_value="SUCCEEDED")
    monkeypatch.setattr(cli.glue, "start_job_run", start)
    monkeypatch.setattr(cli.glue, "wait_for_job_run", wait)

    result = runner.invoke(cli.main, ["glue", "run-all", "--row-cap", "500000"])

    assert result.exit_code == 0, result.output
    staging_call = start.call_args_list[0]
    assert staging_call.args[0] == "mimic4-dw-etl-test-stg-load"
    assert staging_call.args[1]["--ROW_CAP"] == "500000"


def test_glue_run_all_fresh_reload_truncates_and_reseeds_before_running_steps(monkeypatch, runner):
    start = MagicMock(return_value="run-1")
    wait = MagicMock(return_value="SUCCEEDED")
    monkeypatch.setattr(cli.glue, "start_job_run", start)
    monkeypatch.setattr(cli.glue, "wait_for_job_run", wait)
    fake_engine = object()
    truncate_all = MagicMock(return_value=["staging.stg_admissions", "warehouse.fact_admission"])
    seed_date_dim = MagicMock(return_value=365)
    monkeypatch.setattr(cli, "make_engine", lambda settings: fake_engine)
    monkeypatch.setattr(cli, "truncate_all", truncate_all)
    monkeypatch.setattr(cli, "seed_date_dim", seed_date_dim)

    result = runner.invoke(cli.main, ["glue", "run-all", "--fresh-reload"])

    assert result.exit_code == 0, result.output
    truncate_all.assert_called_once_with(fake_engine)
    seed_date_dim.assert_called_once_with(fake_engine, cli.SEED_START_DATE, cli.SEED_END_DATE)
    assert "Truncated 2 table(s)" in result.output
    assert "Re-seeded warehouse.dim_date: 365 days" in result.output
    start.assert_called()  # the actual staging/dimensions/facts/aggregates steps still ran


def test_glue_run_all_without_fresh_reload_does_not_truncate_or_reseed(monkeypatch, runner):
    start = MagicMock(return_value="run-1")
    wait = MagicMock(return_value="SUCCEEDED")
    monkeypatch.setattr(cli.glue, "start_job_run", start)
    monkeypatch.setattr(cli.glue, "wait_for_job_run", wait)
    truncate_all = MagicMock()
    seed_date_dim = MagicMock()
    monkeypatch.setattr(cli, "truncate_all", truncate_all)
    monkeypatch.setattr(cli, "seed_date_dim", seed_date_dim)

    result = runner.invoke(cli.main, ["glue", "run-all"])

    assert result.exit_code == 0, result.output
    truncate_all.assert_not_called()
    seed_date_dim.assert_not_called()


def test_glue_run_facts_no_wait_does_not_poll(monkeypatch, runner):
    start = MagicMock(return_value="run-1")
    wait = MagicMock()
    monkeypatch.setattr(cli.glue, "start_job_run", start)
    monkeypatch.setattr(cli.glue, "wait_for_job_run", wait)

    result = runner.invoke(cli.main, ["glue", "run-facts", "--no-wait"])

    assert result.exit_code == 0, result.output
    start.assert_called_once()
    wait.assert_not_called()


def test_glue_run_all_runs_every_step_in_order_when_all_succeed(monkeypatch, runner):
    start = MagicMock(return_value="run-1")
    wait = MagicMock(return_value="SUCCEEDED")
    monkeypatch.setattr(cli.glue, "start_job_run", start)
    monkeypatch.setattr(cli.glue, "wait_for_job_run", wait)

    result = runner.invoke(cli.main, ["glue", "run-all"])

    assert result.exit_code == 0, result.output
    run_job_names = [c.args[0] for c in start.call_args_list]
    assert run_job_names == [
        "mimic4-dw-etl-test-stg-load",
        "mimic4-dw-etl-test-wh-dimensions",
        "mimic4-dw-etl-test-wh-facts",
        "mimic4-dw-etl-test-wh-aggregates",
    ]


def test_glue_run_all_stops_at_first_non_succeeded_step(monkeypatch, runner):
    start = MagicMock(return_value="run-1")
    # staging succeeds, dimensions fails -> facts/aggregates must never run.
    wait = MagicMock(side_effect=["SUCCEEDED", "FAILED"])
    monkeypatch.setattr(cli.glue, "start_job_run", start)
    monkeypatch.setattr(cli.glue, "wait_for_job_run", wait)
    # A "FAILED" state makes run_job_and_wait check the error message to decide whether
    # to retry (see _IAM_PROPAGATION_SIGNATURE) -- return something that isn't that
    # signature so it returns "FAILED" immediately instead of retrying/looping.
    monkeypatch.setattr(cli.glue, "get_job_run_error", MagicMock(return_value="some other failure"))

    result = runner.invoke(cli.main, ["glue", "run-all"])

    assert result.exit_code != 0
    assert "did not succeed" in result.output
    run_job_names = [c.args[0] for c in start.call_args_list]
    assert run_job_names == ["mimic4-dw-etl-test-stg-load", "mimic4-dw-etl-test-wh-dimensions"]


# Regression coverage for a real bug: getResolvedOptions silently drops any --KEY value
# from a Glue job's args if KEY isn't in the required `options` list it was resolved
# with -- true on both the PySpark and Python Shell runtimes (confirmed against real Glue
# runs). A --TABLE filter therefore has no "just omit it" optional form; every job run
# must pass *some* value, "none" when no filter is wanted (see glue_common.NO_VALUE).
# These tests assert the CLI never launches a job run missing --TABLE/--DROP_TABLES
# entirely -- the exact shape of bug that let a --table filter (and a --drop-tables
# value) silently no-op against a real Glue account.


@pytest.mark.parametrize(
    "command,job_step_name",
    [
        (["glue", "run-dimensions"], "mimic4-dw-etl-test-wh-dimensions"),
        (["glue", "run-facts"], "mimic4-dw-etl-test-wh-facts"),
        (["glue", "run-aggregates"], "mimic4-dw-etl-test-wh-aggregates"),
    ],
)
def test_glue_run_step_without_table_filter_still_passes_none_sentinel(monkeypatch, runner, command, job_step_name):
    start = MagicMock(return_value="run-1")
    wait = MagicMock(return_value="SUCCEEDED")
    monkeypatch.setattr(cli.glue, "start_job_run", start)
    monkeypatch.setattr(cli.glue, "wait_for_job_run", wait)

    result = runner.invoke(cli.main, command)

    assert result.exit_code == 0, result.output
    job_name_arg, arguments = start.call_args.args
    assert job_name_arg == job_step_name
    assert arguments["--TABLE"] == "none"


def test_glue_run_staging_without_table_filter_still_passes_none_sentinel(monkeypatch, runner):
    start = MagicMock(return_value="run-1")
    wait = MagicMock(return_value="SUCCEEDED")
    monkeypatch.setattr(cli.glue, "start_job_run", start)
    monkeypatch.setattr(cli.glue, "wait_for_job_run", wait)

    result = runner.invoke(cli.main, ["glue", "run-staging"])

    assert result.exit_code == 0, result.output
    _job_name_arg, arguments = start.call_args.args
    assert arguments["--TABLE"] == "none"


def test_glue_run_all_passes_table_none_sentinel_for_every_step(monkeypatch, runner):
    start = MagicMock(return_value="run-1")
    wait = MagicMock(return_value="SUCCEEDED")
    monkeypatch.setattr(cli.glue, "start_job_run", start)
    monkeypatch.setattr(cli.glue, "wait_for_job_run", wait)

    result = runner.invoke(cli.main, ["glue", "run-all"])

    assert result.exit_code == 0, result.output
    for call in start.call_args_list:
        _job_name_arg, arguments = call.args
        assert arguments["--TABLE"] == "none"


def test_glue_run_bootstrap_without_drop_tables_still_passes_none_sentinel(monkeypatch, runner):
    start = MagicMock(return_value="run-1")
    wait = MagicMock(return_value="SUCCEEDED")
    monkeypatch.setattr(cli.glue, "start_job_run", start)
    monkeypatch.setattr(cli.glue, "wait_for_job_run", wait)

    result = runner.invoke(cli.main, ["glue", "run-bootstrap"])

    assert result.exit_code == 0, result.output
    _job_name_arg, arguments = start.call_args.args
    assert arguments["--DROP_TABLES"] == "none"
    assert arguments["--TERMINATE_PIDS"] == "none"


def test_glue_run_staging_passes_dataset_and_full_run_id(monkeypatch, runner):
    start = MagicMock(return_value="run-1")
    wait = MagicMock(return_value="SUCCEEDED")
    monkeypatch.setattr(cli.glue, "start_job_run", start)
    monkeypatch.setattr(cli.glue, "wait_for_job_run", wait)

    result = runner.invoke(cli.main, ["glue", "run-staging"])

    assert result.exit_code == 0, result.output
    _job_name_arg, arguments = start.call_args.args
    assert arguments["--FULL_RUN_ID"] == "42"
    assert arguments["--DATASET"] == "demodataset"


def test_glue_run_all_shares_one_full_run_id_across_every_step(monkeypatch, runner):
    start = MagicMock(return_value="run-1")
    wait = MagicMock(return_value="SUCCEEDED")
    monkeypatch.setattr(cli.glue, "start_job_run", start)
    monkeypatch.setattr(cli.glue, "wait_for_job_run", wait)

    result = runner.invoke(cli.main, ["glue", "run-all"])

    assert result.exit_code == 0, result.output
    for call in start.call_args_list:
        _job_name_arg, arguments = call.args
        assert arguments["--FULL_RUN_ID"] == "42"


def test_glue_run_facts_alone_gets_its_own_full_run_id(monkeypatch, runner):
    """A standalone single-step command still gets a full_run_id -- _next_full_run_id
    is stubbed to a constant here, but in real use each standalone call computes its own
    fresh value (see cli._run_glue_step's baseline vs glue_run_all's override)."""
    start = MagicMock(return_value="run-1")
    wait = MagicMock(return_value="SUCCEEDED")
    monkeypatch.setattr(cli.glue, "start_job_run", start)
    monkeypatch.setattr(cli.glue, "wait_for_job_run", wait)

    result = runner.invoke(cli.main, ["glue", "run-facts"])

    assert result.exit_code == 0, result.output
    _job_name_arg, arguments = start.call_args.args
    assert arguments["--FULL_RUN_ID"] == "42"


def test_glue_run_bootstrap_forwards_terminate_pids(monkeypatch, runner):
    start = MagicMock(return_value="run-1")
    wait = MagicMock(return_value="SUCCEEDED")
    monkeypatch.setattr(cli.glue, "start_job_run", start)
    monkeypatch.setattr(cli.glue, "wait_for_job_run", wait)

    result = runner.invoke(cli.main, ["glue", "run-bootstrap", "--terminate-pids", "111,222"])

    assert result.exit_code == 0, result.output
    _job_name_arg, arguments = start.call_args.args
    assert arguments["--TERMINATE_PIDS"] == "111,222"
