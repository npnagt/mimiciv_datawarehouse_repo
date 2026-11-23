import os
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from mimic_datawarehouse.aws.iam import create_s3_import_role
from mimic_datawarehouse.aws.s3 import create_bucket, upload_dataset
from mimic_datawarehouse.config import PROJECT_TAG_VALUE
from mimic_datawarehouse.tables import discover_tables

DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "raw" / "mimic-iv-clinical-database-demo-2.2"
REGION = "us-east-2"


@pytest.fixture(autouse=True)
def _fake_aws_credentials(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)


@mock_aws
def test_create_bucket_is_idempotent():
    bucket = "mimic4-dw-sourcefiles-test"
    create_bucket(bucket, REGION)
    create_bucket(bucket, REGION)  # must not raise on second call

    s3 = boto3.client("s3", region_name=REGION)
    assert bucket in {b["Name"] for b in s3.list_buckets()["Buckets"]}


@mock_aws
def test_create_bucket_applies_project_cost_allocation_tag():
    bucket = "mimic4-dw-sourcefiles-test"
    create_bucket(bucket, REGION)

    s3 = boto3.client("s3", region_name=REGION)
    tags = {t["Key"]: t["Value"] for t in s3.get_bucket_tagging(Bucket=bucket)["TagSet"]}
    assert tags["Project"] == PROJECT_TAG_VALUE == "DWHS"


@mock_aws
def test_create_s3_import_role_is_tagged():
    create_s3_import_role("mimic4-aurora-s3-import-role-test", "mimic4-dw-sourcefiles-test")

    iam = boto3.client("iam")
    tags = {t["Key"]: t["Value"] for t in iam.list_role_tags(RoleName="mimic4-aurora-s3-import-role-test")["Tags"]}
    assert tags["Project"] == "DWHS"


@mock_aws
def test_upload_dataset_preserves_module_prefix_layout():
    bucket = "mimic4-dw-sourcefiles-test"
    create_bucket(bucket, REGION)

    keys = upload_dataset(bucket, DATA_DIR, REGION)

    expected = {f"{t.schema}/{t.name}.csv.gz" for t in discover_tables(DATA_DIR)}
    assert set(keys) == expected

    s3 = boto3.client("s3", region_name=REGION)
    uploaded_keys = {obj["Key"] for obj in s3.list_objects_v2(Bucket=bucket)["Contents"]}
    assert uploaded_keys == expected


@mock_aws
def test_upload_dataset_skips_files_already_in_the_bucket():
    bucket = "mimic4-dw-sourcefiles-test"
    create_bucket(bucket, REGION)

    first = upload_dataset(bucket, DATA_DIR, REGION)
    assert first  # sanity: the demo dataset actually has files to upload

    # A second call against the same (already-populated) bucket should re-upload
    # nothing -- the whole point of the head_object check.
    second = upload_dataset(bucket, DATA_DIR, REGION)
    assert second == []

    s3 = boto3.client("s3", region_name=REGION)
    expected = {f"{t.schema}/{t.name}.csv.gz" for t in discover_tables(DATA_DIR)}
    uploaded_keys = {obj["Key"] for obj in s3.list_objects_v2(Bucket=bucket)["Contents"]}
    assert uploaded_keys == expected  # still all there, just not re-uploaded


@mock_aws
def test_create_s3_import_role_is_idempotent_and_scoped_to_bucket():
    role_arn_1 = create_s3_import_role("mimic4-aurora-s3-import-role", "mimic4-dw-sourcefiles-test")
    role_arn_2 = create_s3_import_role("mimic4-aurora-s3-import-role", "mimic4-dw-sourcefiles-test")
    assert role_arn_1 == role_arn_2
    assert role_arn_1.endswith("role/mimic4-aurora-s3-import-role")
