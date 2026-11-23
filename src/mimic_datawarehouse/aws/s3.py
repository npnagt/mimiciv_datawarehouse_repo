"""S3 bucket provisioning and raw dataset upload."""

from __future__ import annotations

import logging
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from ..config import project_tags_list
from ..tables import discover_tables

logger = logging.getLogger(__name__)


def create_bucket(bucket_name: str, region: str) -> None:
    """Create the raw-data bucket and apply the Project cost-allocation tag. Safe to
    call repeatedly."""
    s3 = boto3.client("s3", region_name=region)
    try:
        if region == "us-east-1":
            s3.create_bucket(Bucket=bucket_name)
        else:
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
        logger.info("Created bucket %s in %s", bucket_name, region)
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code == "BucketAlreadyOwnedByYou":
            logger.info("Bucket %s already exists and is owned by you", bucket_name)
        else:
            raise

    try:
        s3.put_bucket_tagging(
            Bucket=bucket_name,
            Tagging={"TagSet": project_tags_list()},
        )
    except ClientError as exc:
        logger.warning("Could not tag bucket %s: %s", bucket_name, exc)


def _object_exists(s3, bucket_name: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket_name, Key=key)
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return False
        raise


def upload_dataset(bucket_name: str, data_dir: Path, region: str) -> list[str]:
    """Upload every discovered CSV.gz file to `s3://bucket/<module>/<table>.csv.gz`.

    Preserves the local hosp/icu subdirectory layout as the S3 key prefix. Skips a file
    that's already at its key (re-running the pipeline against an already-uploaded
    dataset shouldn't re-upload every multi-GB CSV.gz each time) -- checked with
    head_object, not a local cache, so it's correct even against a bucket populated by a
    prior run/machine. Returns the list of keys this call actually uploaded (already-
    present files are logged but not included).
    """
    s3 = boto3.client("s3", region_name=region)
    uploaded: list[str] = []
    for table in discover_tables(data_dir):
        key = f"{table.schema}/{table.name}.csv.gz"
        if _object_exists(s3, bucket_name, key):
            logger.info("Skipped s3://%s/%s (already exists)", bucket_name, key)
            continue
        s3.upload_file(str(table.csv_path), bucket_name, key)
        uploaded.append(key)
        logger.info("Uploaded s3://%s/%s", bucket_name, key)
    return uploaded
