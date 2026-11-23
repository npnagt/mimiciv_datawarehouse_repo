"""Master wrapper: provisions the AWS infrastructure, then loads the full MIMIC-IV
dataset via AWS Glue -- everything in README.md's "Operations Runbook" section 2
(the `build_datawarehouse.py` step, plus the "3b. Glue path" command sequence) as one
script, run start to finish.

Steps, in order:
  1. Provision AWS infra: S3 bucket + upload dataset, IAM role, Aurora Serverless v2
     cluster (same calls as build_datawarehouse.py) -- idempotent, safe to re-run.
  2. Resolve the cluster's actual endpoint and auth mode (password, or the IAM-only
     "express configuration" fallback -- see aws/aurora.py's docstring) and write
     RDS_HOST/RDS_PORT/RDS_DATABASE/RDS_USER/RDS_PASSWORD/RDS_AUTH_MODE into .env, so
     you don't have to copy-paste the endpoint in by hand before the next step, and any
     interactive `mimic-load ...` command afterwards already has the right target.
  3. `create-database`, only if step 2 detected the express-configuration fallback
     (that path can't create an initial database at cluster-creation time).
  4. `glue deploy` -- scripts/DDL to S3, Glue IAM role, JDBC connection (VPC-attached
     clusters only), and the 5 Glue jobs.
  5. `glue run-bootstrap` -- creates the staging/warehouse schemas and tables.
  6. `seed-date-dim` -- warehouse.dim_date. Runs locally (not via Glue), so it needs
     *this host* to reach Postgres directly -- true for the IAM/express-configuration
     path (internet-reachable), but NOT for a VPC-attached cluster unless this host is
     on a VPN/bastion into that VPC (same constraint the README already documents for
     `mimic-load create-staging` etc.).
  7. `glue run-all` -- staging -> dimensions -> facts -> aggregates, via Glue.

Every step is idempotent: re-running the whole script (or resuming it after a failure
partway through) converges to the same end state without duplicating anything.

Usage:
    python scripts/run_end_to_end.py
    python scripts/run_end_to_end.py --skip-provisioning   # infra + .env RDS_* already correct
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import boto3

from mimic_datawarehouse.aws import aurora, iam, s3
from mimic_datawarehouse.config import (
    DATASET_CHOICES,
    Settings,
    load_aws_settings,
    load_settings,
)
from mimic_datawarehouse.db import ensure_database, make_engine
from mimic_datawarehouse.seed_date_dim import (
    SEED_END_DATE,
    SEED_START_DATE,
    seed_date_dim,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("run_end_to_end")

ENV_FILE = Path(__file__).resolve().parents[1] / ".env"


def _update_env_file(updates: dict[str, str]) -> None:
    """In-place update of KEY=VALUE lines in .env (appends any key not already present).
    Leaves comments and every other line untouched."""
    lines = ENV_FILE.read_text(encoding="utf-8").splitlines() if ENV_FILE.exists() else []
    seen: set[str] = set()
    for i, line in enumerate(lines):
        stripped = line.strip()
        if "=" in stripped and not stripped.startswith("#"):
            key = stripped.split("=", 1)[0].strip()
            if key in updates:
                lines[i] = f"{key}={updates[key]}"
                seen.add(key)
    for key, value in updates.items():
        if key not in seen:
            lines.append(f"{key}={value}")
    ENV_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _run_cli(*args: str, env: dict[str, str]) -> None:
    """Run `python -m mimic_datawarehouse.cli <args>` (same console script `mimic-load`
    installs), streaming output live, raising (stopping the whole script) if it exits
    non-zero. The dataset is carried in `env` (MIMIC_DATASET), so the child's config
    resolves to the same dataset without threading --dataset through every subcommand."""
    cmd = [sys.executable, "-m", "mimic_datawarehouse.cli", *args]
    logger.info("Running: mimic-load %s", " ".join(args))
    subprocess.run(cmd, check=True, env=env)


def _is_express_configuration(cluster_id: str, region: str) -> bool:
    """True if the cluster has no VPC subnet group -- Aurora's "express configuration"
    fallback (see aws/aurora.py's docstring), which requires RDS_AUTH_MODE=iam."""
    rds = boto3.client("rds", region_name=region)
    cluster = rds.describe_db_clusters(DBClusterIdentifier=cluster_id)["DBClusters"][0]
    return not cluster.get("DBSubnetGroup")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--skip-provisioning",
        action="store_true",
        help="Skip step 1 (AWS infra). Use when the bucket/role/cluster already exist and "
        "RDS_HOST/RDS_AUTH_MODE (and friends) in .env are already correct for them.",
    )
    parser.add_argument(
        "--dataset", choices=DATASET_CHOICES, default=None,
        help="Dataset + resource set to build (default: demodataset, or MIMIC_DATASET). "
        "Uses a separate Aurora database, S3 source bucket and Glue jobs per dataset.",
    )
    parser.add_argument(
        "--row-cap", type=int, default=None,
        help="Max rows to load per staging table (0 = no limit). Any raw table above the "
        "cap is truncated to it -- the same rule the lakehouse repo applies to its Gold "
        "fact tables. Default: 1,000,000 (see mimic-load glue run-all).",
    )
    parser.add_argument(
        "--fresh-reload", action="store_true",
        help="TRUNCATE every staging/warehouse data table (warehouse.etl_process_log is "
        "never touched, so cross-run comparisons still work) and re-seed dim_date before "
        "step 7's load, so the whole dataset reloads from empty instead of incrementally "
        "-- see `mimic-load glue run-all --fresh-reload`.",
    )
    args = parser.parse_args()
    if args.dataset:
        os.environ["MIMIC_DATASET"] = args.dataset

    aws_settings = load_aws_settings()
    logger.info("Dataset: %s (db %s, bucket %s)", aws_settings.dataset,
                aws_settings.aurora_db_name, aws_settings.s3_bucket)

    if not args.skip_provisioning:
        logger.info("Step 1a: creating S3 bucket s3://%s", aws_settings.s3_bucket)
        s3.create_bucket(aws_settings.s3_bucket, aws_settings.region)

        logger.info("Step 1b: uploading raw dataset from %s", aws_settings.data_dir)
        keys = s3.upload_dataset(aws_settings.s3_bucket, aws_settings.data_dir, aws_settings.region)
        logger.info("Uploaded %d files", len(keys))

        logger.info("Step 1c: creating IAM role %s", aws_settings.iam_role_name)
        role_arn = iam.create_s3_import_role(aws_settings.iam_role_name, aws_settings.s3_bucket)

        logger.info("Step 1d: creating Aurora PostgreSQL Serverless v2 cluster %s", aws_settings.aurora_cluster_id)
        aurora.create_serverless_cluster(
            cluster_id=aws_settings.aurora_cluster_id,
            db_name=aws_settings.aurora_db_name,
            master_username=aws_settings.aurora_master_username,
            master_password=aws_settings.aurora_master_password,
            region=aws_settings.region,
            subnet_group_name=aws_settings.db_subnet_group_name,
            security_group_ids=aws_settings.vpc_security_group_ids,
            min_acu=aws_settings.aurora_min_acu,
            max_acu=aws_settings.aurora_max_acu,
        )
        logger.info("Step 1e: waiting for cluster to become available (this can take 10-15 minutes)")
        endpoint = aurora.wait_for_cluster_available(aws_settings.aurora_cluster_id, aws_settings.region)
        aurora.attach_s3_import_role(aws_settings.aurora_cluster_id, role_arn, aws_settings.region)

        auth_mode = (
            "iam" if _is_express_configuration(aws_settings.aurora_cluster_id, aws_settings.region) else "password"
        )
        logger.info("Step 2: cluster endpoint %s, auth mode %s -- writing RDS_* to .env", endpoint, auth_mode)
        _update_env_file(
            {
                "RDS_HOST": endpoint,
                "RDS_PORT": "5432",
                "RDS_DATABASE": aws_settings.aurora_db_name,
                "RDS_USER": aws_settings.aurora_master_username,
                "RDS_AUTH_MODE": auth_mode,
                "RDS_PASSWORD": aws_settings.aurora_master_password,
            }
        )
        settings = Settings(
            rds_host=endpoint,
            rds_port=5432,
            rds_database=aws_settings.aurora_db_name,
            rds_user=aws_settings.aurora_master_username,
            rds_password="" if auth_mode == "iam" else aws_settings.aurora_master_password,
            data_dir=aws_settings.data_dir,
            auth_mode=auth_mode,
            region=aws_settings.region,
            dataset=aws_settings.dataset,
        )
    else:
        logger.info("Step 1-2: --skip-provisioning -- trusting .env's current RDS_* settings")
        settings = load_settings()

    # Explicit env overrides for every subprocess below, not just the .env file on disk:
    # config.py's load_dotenv() won't override a value this process (or a child process
    # inheriting its environment) already had cached from *before* step 2 rewrote .env.
    subprocess_env = {
        **os.environ,
        "MIMIC_DATASET": aws_settings.dataset,
        "RDS_HOST": settings.rds_host,
        "RDS_PORT": str(settings.rds_port),
        "RDS_DATABASE": settings.rds_database,
        "RDS_USER": settings.rds_user,
        "RDS_PASSWORD": settings.rds_password,
        "RDS_AUTH_MODE": settings.auth_mode,
    }

    if settings.auth_mode == "iam":
        logger.info("Step 3: creating database %s (express-configuration cluster)", settings.rds_database)
        created = ensure_database(settings)
        logger.info("Database %s", "created" if created else "already existed")
    else:
        logger.info(
            "Step 3: skipped (standard/VPC-attached cluster already has %s from creation)", settings.rds_database
        )
        logger.warning(
            "This is a VPC-attached cluster -- steps 6/7 below need to reach it directly. "
            "Run this script from a host on that VPC (VPN/bastion/peered), or run seed-date-dim "
            "and glue run-all yourself from such a host instead."
        )

    logger.info("Step 4: mimic-load glue deploy")
    _run_cli("glue", "deploy", env=subprocess_env)

    logger.info("Step 5: mimic-load glue run-bootstrap")
    _run_cli("glue", "run-bootstrap", env=subprocess_env)

    if args.fresh_reload:
        # --fresh-reload's TRUNCATE (run inside step 7, via `glue run-all --fresh-reload`)
        # wipes dim_date along with everything else and re-seeds it itself right after --
        # seeding it here first would just be immediately-truncated wasted work.
        logger.info("Step 6: skipped (--fresh-reload re-seeds warehouse.dim_date itself, in step 7)")
    else:
        logger.info("Step 6: seeding warehouse.dim_date (%s..%s)", SEED_START_DATE, SEED_END_DATE)
        engine = make_engine(settings)
        try:
            days = seed_date_dim(engine, SEED_START_DATE, SEED_END_DATE)
            logger.info("Seeded %d days", days)
        finally:
            engine.dispose()

    logger.info("Step 7: mimic-load glue run-all (staging -> dimensions -> facts -> aggregates)")
    run_all_args = ["glue", "run-all"]
    if args.row_cap is not None:
        run_all_args += ["--row-cap", str(args.row_cap)]
    if args.fresh_reload:
        run_all_args += ["--fresh-reload"]
    _run_cli(*run_all_args, env=subprocess_env)

    print()
    print("Build + load complete.")
    print(f"  Cluster endpoint : {settings.rds_host}")
    print(f"  Database         : {settings.rds_database}")
    print(f"  Auth mode        : {settings.auth_mode}")


if __name__ == "__main__":
    main()
