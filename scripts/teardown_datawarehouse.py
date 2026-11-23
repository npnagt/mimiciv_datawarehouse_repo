"""Tear down the AWS infrastructure provisioned by build_datawarehouse.py and
`mimic-load glue deploy`. By default the S3 source bucket is left untouched (its raw
data, Glue scripts/DDL, Spark UI event logs) -- pass --delete-bucket to empty and
remove it too.

Deletes, in order:
  1. The Aurora cluster's DB instance(s), then the cluster itself (`mimic4rds`) -- by
     default with a final snapshot (pass --skip-final-snapshot to skip it and delete
     faster/for free; there is no way to recover the data once skipped).
  2. The IAM role Aurora used to read the S3 bucket, and its inline policy.
  3. The DB subnet group (only possible once the cluster is fully gone) -- pass
     --keep-subnet-group to leave it for reuse. Skipped automatically for an
     express-configuration cluster (no subnet group).
  4. The 5 Glue jobs (schema-bootstrap, staging load, warehouse dimensions/facts/
     aggregates), the Glue JDBC connection (a no-op if `glue deploy` never created one --
     see aws/glue.py's create_glue_connection), and the Glue execution role and its
     inline policies -- everything `mimic-load glue deploy` created. Pass --keep-glue to
     leave these in place (e.g. if you're about to re-run `glue deploy` again shortly).
  5. With --delete-bucket: empty (all object versions) and delete the S3 source bucket.

This is destructive and irreversible (aside from the optional final snapshot). By
default it only PRINTS what would be deleted -- pass --yes to actually do it.

The Aurora CLUSTER is shared between the demo and full datasets (only the database
inside it differs). To tear down just one dataset while the other still needs the
cluster, pass --keep-cluster (skips the cluster / S3-import role / subnet group) with
--drop-database (DROP DATABASE mimic4rds_<demo|full>) and --dataset.

Usage:
    python scripts/teardown_datawarehouse.py                          # dry run
    python scripts/teardown_datawarehouse.py --yes                    # delete, with final snapshot
    python scripts/teardown_datawarehouse.py --yes --skip-final-snapshot
    python scripts/teardown_datawarehouse.py --yes --keep-glue        # leave the Glue jobs/role/connection
    python scripts/teardown_datawarehouse.py --dataset demodataset --keep-cluster --drop-database --yes
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import boto3  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402

from mimic_datawarehouse.aws import aurora, glue, iam  # noqa: E402
from mimic_datawarehouse.config import (  # noqa: E402
    DATASET_CHOICES,
    load_aws_settings,
    load_settings,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("teardown_datawarehouse")


def _empty_and_delete_bucket(bucket_name: str, region: str) -> None:
    """Delete every object version in `bucket_name`, then the bucket shell. No-op if the
    bucket doesn't exist."""
    s3 = boto3.client("s3", region_name=region)
    try:
        to_delete = []
        for page in s3.get_paginator("list_object_versions").paginate(Bucket=bucket_name):
            for v in page.get("Versions", []):
                to_delete.append({"Key": v["Key"], "VersionId": v["VersionId"]})
            for m in page.get("DeleteMarkers", []):
                to_delete.append({"Key": m["Key"], "VersionId": m["VersionId"]})
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("NoSuchBucket", "404"):
            logger.info("Bucket %s does not exist -- nothing to delete", bucket_name)
            return
        raise
    for i in range(0, len(to_delete), 1000):
        chunk = to_delete[i:i + 1000]
        if chunk:
            s3.delete_objects(Bucket=bucket_name, Delete={"Objects": chunk, "Quiet": True})
    logger.info("Emptied bucket %s (%d object version(s))", bucket_name, len(to_delete))
    s3.delete_bucket(Bucket=bucket_name)
    logger.info("Deleted bucket %s", bucket_name)


def _drop_database(db_name: str) -> None:
    """DROP DATABASE <db_name> via a connection to the `postgres` maintenance DB (the
    inverse of db.ensure_database). Needs the RDS_* settings in .env to reach the cluster."""
    from dataclasses import replace

    from sqlalchemy import text

    from mimic_datawarehouse.db import make_engine

    engine = make_engine(replace(load_settings(), rds_database="postgres"))
    try:
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text(f'DROP DATABASE IF EXISTS "{db_name}" WITH (FORCE)'))
        logger.info("Dropped database %s", db_name)
    finally:
        engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--yes", action="store_true", help="Actually delete resources (default: dry run only).")
    parser.add_argument(
        "--skip-final-snapshot",
        action="store_true",
        help="Don't take a final snapshot of the Aurora cluster before deleting it. Faster and free, "
        "but the loaded data (staging + warehouse) cannot be recovered afterwards.",
    )
    parser.add_argument(
        "--keep-subnet-group",
        action="store_true",
        help="Don't delete the DB subnet group (harmless to leave -- no cost, no bucket involvement).",
    )
    parser.add_argument(
        "--keep-glue",
        action="store_true",
        help="Don't delete the Glue jobs/connection/role created by `mimic-load glue deploy` "
        "(e.g. if you're about to re-run `glue deploy` again shortly).",
    )
    parser.add_argument(
        "--delete-bucket",
        action="store_true",
        help="Also empty and delete the S3 source bucket (default: left untouched).",
    )
    parser.add_argument(
        "--dataset", choices=DATASET_CHOICES, default=None,
        help="Dataset whose resources to tear down (default: demodataset, or MIMIC_DATASET). "
        "Names below are that dataset's -demo / -full variants.",
    )
    parser.add_argument(
        "--keep-cluster",
        action="store_true",
        help="Don't delete the Aurora cluster, its S3-import IAM role, or the DB subnet group "
        "-- the cluster is SHARED between datasets. Use this (optionally with --drop-database) "
        "to tear down just one dataset while the other still needs the cluster.",
    )
    parser.add_argument(
        "--drop-database",
        action="store_true",
        help="DROP the dataset's Aurora database (mimic4rds_demo / mimic4rds_full) -- needs a "
        "reachable cluster and the RDS_* connection settings in .env. Implied target for a "
        "per-dataset teardown; pair with --keep-cluster.",
    )
    args = parser.parse_args()
    if args.dataset:
        os.environ["MIMIC_DATASET"] = args.dataset

    settings = load_aws_settings()
    glue_job_names = [glue.job_name(settings.glue_job_name_prefix, step) for step in glue.GLUE_JOB_STEPS]

    subnet_group = settings.db_subnet_group_name.strip()

    print(f"Dataset: {settings.dataset}")
    if args.delete_bucket:
        print("The following will be deleted (INCLUDING the S3 source bucket):")
    else:
        print("The following will be deleted (S3 bucket is NOT touched):")
    if args.keep_cluster:
        print(f"  Aurora cluster   : {settings.aurora_cluster_id} -- KEPT (shared between datasets)")
    else:
        print(f"  Aurora cluster   : {settings.aurora_cluster_id} (region {settings.region})")
        print(f"  IAM role         : {settings.iam_role_name}")
    if args.drop_database:
        print(f"  Aurora database  : DROP DATABASE {settings.aurora_db_name}")
    if not args.keep_cluster and not args.keep_subnet_group and subnet_group:
        print(f"  DB subnet group  : {subnet_group}")
    if not args.keep_cluster:
        print(f"  Final snapshot   : {'NO -- data will be unrecoverable' if args.skip_final_snapshot else 'yes'}")
    if not args.keep_glue:
        print(f"  Glue jobs        : {', '.join(glue_job_names)}")
        print(f"  Glue connection  : {settings.glue_connection_name} (no-op if none was ever created)")
        print(f"  Glue IAM role    : {settings.glue_role_name}")
    if args.delete_bucket:
        print(f"  S3 bucket        : s3://{settings.s3_bucket}  (emptied + deleted)")
    print()

    if not args.yes:
        print("Dry run only -- nothing was deleted. Re-run with --yes to actually delete these resources.")
        return

    if args.drop_database:
        logger.info("Step 0: DROP DATABASE %s", settings.aurora_db_name)
        _drop_database(settings.aurora_db_name)

    if not args.keep_cluster:
        logger.info("Step 1: deleting Aurora cluster %s (and its instance(s))", settings.aurora_cluster_id)
        aurora.delete_cluster(
            settings.aurora_cluster_id,
            settings.region,
            skip_final_snapshot=args.skip_final_snapshot,
        )
        aurora.wait_for_cluster_deleted(settings.aurora_cluster_id, settings.region)

        logger.info("Step 2: deleting IAM role %s", settings.iam_role_name)
        iam.delete_s3_import_role(settings.iam_role_name)

        if not args.keep_subnet_group and subnet_group:
            logger.info("Step 3: deleting DB subnet group %s", subnet_group)
            aurora.delete_subnet_group(subnet_group, settings.region)

    if not args.keep_glue:
        logger.info("Step 4: deleting Glue jobs: %s", ", ".join(glue_job_names))
        for step in glue.GLUE_JOB_STEPS:
            glue.delete_glue_job(settings.glue_job_name_prefix, step)

        logger.info("Step 5: deleting Glue connection %s", settings.glue_connection_name)
        glue.delete_glue_connection(settings.glue_connection_name)

        logger.info("Step 6: deleting Glue IAM role %s", settings.glue_role_name)
        glue.delete_glue_role(settings.glue_role_name)

    if args.delete_bucket:
        logger.info("Step 7: emptying + deleting S3 bucket %s", settings.s3_bucket)
        _empty_and_delete_bucket(settings.s3_bucket, settings.region)

    print()
    if args.delete_bucket:
        print("Teardown complete.")
    else:
        print("Teardown complete. Untouched:")
        print(f"  S3 bucket: s3://{settings.s3_bucket}")


if __name__ == "__main__":
    main()
