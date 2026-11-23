"""Runtime configuration, sourced from environment variables / .env."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------------------
# Cost-allocation tag
# ---------------------------------------------------------------------------------------
# Every AWS resource this repo provisions (S3 bucket, IAM roles, Aurora cluster, Glue
# jobs / connection) is tagged Project=<PROJECT_TAG_VALUE> so spend can be split from the
# data-lakehouse repo (which tags Project=LAKEHSE). Activate "Project" under Billing >
# Cost allocation tags to see it broken out in Cost Explorer.

PROJECT_TAG_KEY = "Project"
PROJECT_TAG_VALUE = os.environ.get("PROJECT_TAG") or "DWHS"


def project_tags() -> dict[str, str]:
    """Cost-allocation tags as a {key: value} dict (Glue create_* APIs take this shape)."""
    return {PROJECT_TAG_KEY: PROJECT_TAG_VALUE}


def project_tags_list() -> list[dict[str, str]]:
    """Same tags as a [{"Key":.., "Value":..}] list (IAM / S3 / RDS APIs take this shape)."""
    return [{"Key": k, "Value": v} for k, v in project_tags().items()]


# ---------------------------------------------------------------------------------------
# Dataset-mode profiles
# ---------------------------------------------------------------------------------------
# The pipeline runs against either the 100-patient demo dataset or the full MIMIC-IV 3.1
# download. `MIMIC_DATASET` (env / --dataset) picks the mode; every dataset-specific
# resource name below is derived from it so demo and full stay isolated -- a separate
# Aurora *database* inside the shared cluster, a separate S3 source bucket, separate Glue
# jobs / roles. Any individual name can still be pinned by setting its own env var.

DEFAULT_DATASET = "demodataset"
DATASET_CHOICES = ("demodataset", "fulldataset")


@dataclass(frozen=True)
class DatasetProfile:
    dataset: str          # "demodataset" | "fulldataset"
    mode: str             # "demo" | "full"
    data_dir: str
    s3_bucket: str
    iam_role_name: str
    aurora_db_name: str
    glue_role_name: str
    glue_connection_name: str
    glue_job_name_prefix: str


_PROFILES: dict[str, DatasetProfile] = {
    "demodataset": DatasetProfile(
        dataset="demodataset", mode="demo",
        data_dir="data/raw/mimic-iv-clinical-database-demo-2.2",
        s3_bucket="mimic4-dw-sourcefiles-demo",
        iam_role_name="mimic4-aurora-s3-import-role-demo",
        aurora_db_name="mimic4rds_demo",
        glue_role_name="mimic4-dw-etl-glue-role-demo",
        glue_connection_name="mimic4-dw-etl-aurora-connection-demo",
        glue_job_name_prefix="mimic4-dw-etl-demo",
    ),
    "fulldataset": DatasetProfile(
        dataset="fulldataset", mode="full",
        data_dir="data/raw/mimic-iv-3.1-fulldataset",
        s3_bucket="mimic4-dw-sourcefiles-full",
        iam_role_name="mimic4-aurora-s3-import-role-full",
        aurora_db_name="mimic4rds_full",
        glue_role_name="mimic4-dw-etl-glue-role-full",
        glue_connection_name="mimic4-dw-etl-aurora-connection-full",
        glue_job_name_prefix="mimic4-dw-etl-full",
    ),
}
_PROFILES["demo"] = _PROFILES["demodataset"]
_PROFILES["full"] = _PROFILES["fulldataset"]


def dataset_name() -> str:
    """Current dataset mode: MIMIC_DATASET env var, else the demo default."""
    return (os.environ.get("MIMIC_DATASET") or DEFAULT_DATASET).strip().lower()


def dataset_profile(dataset: str | None = None) -> DatasetProfile:
    name = (dataset or dataset_name()).strip().lower()
    if name not in _PROFILES:
        raise RuntimeError(
            f"Unknown dataset {name!r}. Use one of: {', '.join(DATASET_CHOICES)}."
        )
    return _PROFILES[name]


@dataclass(frozen=True)
class Settings:
    rds_host: str
    rds_port: int
    rds_database: str
    rds_user: str
    rds_password: str
    data_dir: Path
    auth_mode: str = "password"  # "password" or "iam"
    region: str = "us-east-2"  # only used when auth_mode == "iam"
    dataset: str = DEFAULT_DATASET

    @property
    def sqlalchemy_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.rds_user}:{self.rds_password}"
            f"@{self.rds_host}:{self.rds_port}/{self.rds_database}"
        )


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def load_settings() -> Settings:
    auth_mode = os.environ.get("RDS_AUTH_MODE", "password")
    profile = dataset_profile()
    return Settings(
        rds_host=_require("RDS_HOST"),
        rds_port=int(os.environ.get("RDS_PORT", "5432")),
        # RDS_DATABASE pins the target DB; without it, the per-dataset default
        # (mimic4rds_demo / mimic4rds_full) so demo and full stay separate.
        rds_database=os.environ.get("RDS_DATABASE") or profile.aurora_db_name,
        rds_user=_require("RDS_USER"),
        # IAM auth clusters have no static password -- a fresh token is generated per
        # connection instead (see db.make_engine).
        rds_password="" if auth_mode == "iam" else _require("RDS_PASSWORD"),
        data_dir=Path(os.environ.get("MIMIC_DATA_DIR") or profile.data_dir),
        auth_mode=auth_mode,
        region=os.environ.get("AWS_REGION", "us-east-2"),
        dataset=profile.dataset,
    )


@dataclass(frozen=True)
class AwsSettings:
    """Settings for provisioning the AWS infrastructure itself (bucket, IAM role, Aurora
    cluster). Distinct from `Settings`, which targets a database that already exists.

    `aurora_master_password` / `db_subnet_group_name` / `vpc_security_group_ids` are only
    used on accounts that support full VPC-attached clusters with a static password; on
    accounts restricted to Aurora's "express configuration" (no VPC control, IAM auth
    only) they're ignored -- see `aws.aurora.create_serverless_cluster`.
    """

    region: str
    data_dir: Path
    s3_bucket: str
    iam_role_name: str
    aurora_cluster_id: str
    aurora_db_name: str
    aurora_master_username: str
    aurora_master_password: str
    aurora_min_acu: float
    aurora_max_acu: float
    db_subnet_group_name: str
    vpc_security_group_ids: list[str]
    glue_role_name: str
    glue_connection_name: str
    # Job names are built as "<prefix>-<stg|wh>-<step>" (see aws/glue.py's
    # GLUE_JOB_STEPS) -- "dw-etl" plus the stg/wh segment keeps these jobs identifiable
    # in an AWS account/region shared with other projects' Glue jobs.
    glue_job_name_prefix: str
    glue_worker_type: str
    glue_num_workers: int
    glue_version: str
    # A Glue JDBC connection binds to one specific subnet/AZ (unlike RDS's
    # db_subnet_group_name, which spans several) -- must be a subnet from that same
    # group so Glue's ENIs can reach the private Aurora cluster.
    glue_subnet_id: str
    glue_availability_zone: str
    dataset: str = DEFAULT_DATASET


def load_aws_settings() -> AwsSettings:
    profile = dataset_profile()
    return AwsSettings(
        region=os.environ.get("AWS_REGION", "us-east-2"),
        data_dir=Path(os.environ.get("MIMIC_DATA_DIR") or profile.data_dir),
        s3_bucket=os.environ.get("S3_BUCKET_NAME") or profile.s3_bucket,
        iam_role_name=os.environ.get("IAM_ROLE_NAME") or profile.iam_role_name,
        # One Aurora cluster hosts both datasets' databases -- only the DB name differs.
        aurora_cluster_id=os.environ.get("AURORA_CLUSTER_ID", "mimic4rds"),
        aurora_db_name=os.environ.get("AURORA_DB_NAME") or profile.aurora_db_name,
        aurora_master_username=_require("AURORA_MASTER_USERNAME"),
        aurora_master_password=os.environ.get("AURORA_MASTER_PASSWORD", ""),
        aurora_min_acu=float(os.environ.get("AURORA_MIN_ACU", "0.5")),
        aurora_max_acu=float(os.environ.get("AURORA_MAX_ACU", "4")),
        db_subnet_group_name=os.environ.get("DB_SUBNET_GROUP_NAME", ""),
        vpc_security_group_ids=[
            sg.strip() for sg in os.environ.get("VPC_SECURITY_GROUP_IDS", "").split(",") if sg.strip()
        ],
        glue_role_name=os.environ.get("GLUE_ROLE_NAME") or profile.glue_role_name,
        glue_connection_name=os.environ.get("GLUE_CONNECTION_NAME") or profile.glue_connection_name,
        glue_job_name_prefix=os.environ.get("GLUE_JOB_NAME_PREFIX") or profile.glue_job_name_prefix,
        glue_worker_type=os.environ.get("GLUE_WORKER_TYPE", "G.1X"),
        glue_num_workers=int(os.environ.get("GLUE_NUM_WORKERS", "2")),
        glue_version=os.environ.get("GLUE_VERSION", "4.0"),
        glue_subnet_id=os.environ.get("GLUE_SUBNET_ID", ""),
        glue_availability_zone=os.environ.get("GLUE_AVAILABILITY_ZONE", ""),
        dataset=profile.dataset,
    )
