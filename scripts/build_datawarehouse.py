"""Provision the AWS infrastructure for the MIMIC-IV data warehouse, end to end:

  1. Create the per-dataset S3 source bucket (mimic4-dw-sourcefiles-<demo|full>) and
     upload the local raw CSV.gz files.
  2. Create the IAM role Aurora needs to read that bucket, and the shared Aurora
     PostgreSQL Serverless v2 cluster (`mimic4rds`), then attach the role to it.

Pass --dataset demodataset|fulldataset (or set MIMIC_DATASET) to pick which dataset's
resources to provision; both datasets share one Aurora cluster, each with its own
database (mimic4rds_demo / mimic4rds_full).

Step 3 (creating the `staging` schema/tables) is intentionally left to
`mimic-load create-staging`, run separately: the cluster is private (VPC-only,
`PubliclyAccessible=False`), so it must be run from a host that can actually reach it
(VPN, bastion, etc.) -- which may not be this machine.

Usage:
    python scripts/build_datawarehouse.py
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mimic_datawarehouse.aws import aurora, iam, s3  # noqa: E402
from mimic_datawarehouse.config import DATASET_CHOICES, load_aws_settings  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("build_datawarehouse")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--dataset", choices=DATASET_CHOICES, default=None,
        help="Dataset + resource set to provision (default: demodataset, or MIMIC_DATASET).",
    )
    args = parser.parse_args()
    if args.dataset:
        os.environ["MIMIC_DATASET"] = args.dataset

    settings = load_aws_settings()
    logger.info("Dataset: %s", settings.dataset)

    logger.info("Step 1a: creating S3 bucket s3://%s", settings.s3_bucket)
    s3.create_bucket(settings.s3_bucket, settings.region)

    logger.info("Step 1b: uploading raw dataset from %s", settings.data_dir)
    keys = s3.upload_dataset(settings.s3_bucket, settings.data_dir, settings.region)
    logger.info("Uploaded %d files", len(keys))

    logger.info("Step 1c: creating IAM role %s", settings.iam_role_name)
    role_arn = iam.create_s3_import_role(settings.iam_role_name, settings.s3_bucket)

    logger.info("Step 2a: creating Aurora PostgreSQL Serverless v2 cluster %s", settings.aurora_cluster_id)
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

    logger.info("Step 2b: waiting for cluster to become available (this can take 10-15 minutes)")
    endpoint = aurora.wait_for_cluster_available(settings.aurora_cluster_id, settings.region)

    logger.info("Step 2c: attaching IAM role to cluster for S3 import")
    aurora.attach_s3_import_role(settings.aurora_cluster_id, role_arn, settings.region)

    print()
    print("Provisioning complete.")
    print(f"  Cluster endpoint : {endpoint}")
    print(f"  Database name    : {settings.aurora_db_name}")
    print(f"  S3 import role   : {role_arn}")
    print()
    print("Next: since the cluster is private, from a host that can reach it,")
    print("set RDS_HOST/RDS_PORT/RDS_DATABASE/RDS_USER/RDS_PASSWORD in .env, then run:")
    print("  mimic-load create-staging")
    print("  mimic-load create-warehouse")
    print()
    print("To run the production ETL as AWS Glue jobs instead of locally, see the")
    print("'Running the ETL as AWS Glue jobs' section of README.md, then run:")
    print("  mimic-load glue deploy")


if __name__ == "__main__":
    main()
