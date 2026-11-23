import pytest

from mimic_datawarehouse.config import (
    dataset_profile,
    load_aws_settings,
    load_settings,
)


def _base_env(monkeypatch):
    monkeypatch.setenv("RDS_HOST", "example.rds.amazonaws.com")
    monkeypatch.setenv("RDS_DATABASE", "mimic4rds")
    monkeypatch.setenv("RDS_USER", "mimic_admin")


def test_password_mode_requires_rds_password(monkeypatch):
    _base_env(monkeypatch)
    monkeypatch.delenv("RDS_PASSWORD", raising=False)
    monkeypatch.delenv("RDS_AUTH_MODE", raising=False)
    with pytest.raises(RuntimeError, match="RDS_PASSWORD"):
        load_settings()


def test_iam_mode_does_not_require_rds_password(monkeypatch):
    _base_env(monkeypatch)
    monkeypatch.setenv("RDS_AUTH_MODE", "iam")
    monkeypatch.delenv("RDS_PASSWORD", raising=False)

    settings = load_settings()

    assert settings.auth_mode == "iam"
    assert settings.rds_password == ""


def test_dataset_profiles_are_isolated_but_share_the_cluster(monkeypatch):
    monkeypatch.delenv("MIMIC_DATASET", raising=False)
    demo = dataset_profile("demodataset")
    full = dataset_profile("fulldataset")

    assert demo.aurora_db_name == "mimic4rds_demo"
    assert full.aurora_db_name == "mimic4rds_full"
    assert demo.s3_bucket == "mimic4-dw-sourcefiles-demo"
    assert full.glue_job_name_prefix == "mimic4-dw-etl-full"
    assert demo.data_dir == "data/raw/mimic-iv-clinical-database-demo-2.2"
    assert full.data_dir == "data/raw/mimic-iv-3.1-fulldataset"


def test_unknown_dataset_raises():
    with pytest.raises(RuntimeError, match="Unknown dataset"):
        dataset_profile("teradataset")


def test_mimic_dataset_env_drives_load_settings(monkeypatch):
    _base_env(monkeypatch)
    monkeypatch.delenv("RDS_DATABASE", raising=False)
    monkeypatch.delenv("MIMIC_DATA_DIR", raising=False)
    monkeypatch.setenv("RDS_PASSWORD", "secret")
    monkeypatch.setenv("MIMIC_DATASET", "fulldataset")

    settings = load_settings()

    assert settings.dataset == "fulldataset"
    assert settings.rds_database == "mimic4rds_full"
    assert str(settings.data_dir) == str(__import__("pathlib").Path("data/raw/mimic-iv-3.1-fulldataset"))


def test_explicit_env_overrides_dataset_default(monkeypatch):
    _base_env(monkeypatch)  # sets RDS_DATABASE=mimic4rds
    monkeypatch.setenv("RDS_PASSWORD", "secret")
    monkeypatch.setenv("MIMIC_DATASET", "fulldataset")

    settings = load_settings()

    assert settings.dataset == "fulldataset"
    assert settings.rds_database == "mimic4rds"  # explicit env wins over the profile


def test_aws_settings_follow_mimic_dataset(monkeypatch):
    monkeypatch.setenv("AURORA_MASTER_USERNAME", "mimic_admin")
    for key in ("S3_BUCKET_NAME", "IAM_ROLE_NAME", "AURORA_DB_NAME", "GLUE_ROLE_NAME",
                "GLUE_CONNECTION_NAME", "GLUE_JOB_NAME_PREFIX", "MIMIC_DATA_DIR"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("MIMIC_DATASET", "fulldataset")

    aws = load_aws_settings()

    assert aws.dataset == "fulldataset"
    assert aws.s3_bucket == "mimic4-dw-sourcefiles-full"
    assert aws.aurora_db_name == "mimic4rds_full"
    assert aws.glue_job_name_prefix == "mimic4-dw-etl-full"
    assert aws.aurora_cluster_id == "mimic4rds"  # cluster stays shared
