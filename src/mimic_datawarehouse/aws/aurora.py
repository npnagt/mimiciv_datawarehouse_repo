"""Aurora PostgreSQL Serverless v2 cluster provisioning.

The cluster is created with `PubliclyAccessible=False` -- it is reachable only from
inside the VPC named by `subnet_group_name` / `security_group_ids` (e.g. via VPN,
bastion host, or a peered/connected network). This module never modifies security
group ingress rules.
"""

from __future__ import annotations

import logging
import time

import boto3
from botocore.exceptions import ClientError

from ..config import project_tags_list

logger = logging.getLogger(__name__)

_POLL_SECONDS = 15
_INSTANCE_SUFFIX = "-instance-1"


# DBSubnetGroupNotFoundFault: a brand-new AWS account has no pre-existing custom VPC
# subnet group either (see this module's docstring -- creating one is out of scope here,
# it's assumed already set up for the VPC-attached path) -- discovered against a real
# fresh account, same underlying situation as the other two codes (this account/setup
# can't support the standard VPC-attached path), so it gets the same express-configuration
# fallback rather than crashing.
_EXPRESS_FALLBACK_CODES = {"FreeTierRestrictionError", "InvalidParameterCombination", "DBSubnetGroupNotFoundFault"}


def create_serverless_cluster(
    *,
    cluster_id: str,
    db_name: str,
    master_username: str,
    master_password: str,
    region: str,
    subnet_group_name: str,
    security_group_ids: list[str],
    min_acu: float,
    max_acu: float,
) -> None:
    """Create the Aurora PostgreSQL Serverless v2 cluster and its writer instance.

    Tries a standard, VPC-attached, password-authenticated cluster first. Some AWS
    account tiers (e.g. new/free-plan accounts) reject that with
    `FreeTierRestrictionError` and require Aurora's "express configuration" instead --
    which forgoes VPC placement, an initial database, and a static password in favor of
    AWS-managed networking and IAM database authentication. If that happens, this falls
    back automatically and logs a warning; `create_staging_tables` and friends then need
    `RDS_AUTH_MODE=iam` (see config.py / README).

    Safe to call repeatedly -- already-existing cluster/instance are left untouched.
    """
    rds = boto3.client("rds", region_name=region)
    scaling_config = {"ServerlessV2ScalingConfiguration": {"MinCapacity": min_acu, "MaxCapacity": max_acu}}

    try:
        rds.create_db_cluster(
            DBClusterIdentifier=cluster_id,
            Engine="aurora-postgresql",
            DatabaseName=db_name,
            MasterUsername=master_username,
            MasterUserPassword=master_password,
            DBSubnetGroupName=subnet_group_name,
            VpcSecurityGroupIds=security_group_ids,
            StorageEncrypted=True,
            Tags=project_tags_list(),
            **scaling_config,
        )
        logger.info("Creating Aurora cluster %s (standard VPC configuration)", cluster_id)
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code == "DBClusterAlreadyExistsFault":
            logger.info("Aurora cluster %s already exists", cluster_id)
            return
        if code not in _EXPRESS_FALLBACK_CODES:
            raise
        logger.warning(
            "Standard cluster creation rejected (%s: %s); this account requires Aurora's "
            "express configuration -- retrying without VPC placement/static password "
            "(IAM database authentication only).",
            code,
            exc.response["Error"]["Message"],
        )
        try:
            rds.create_db_cluster(
                DBClusterIdentifier=cluster_id,
                Engine="aurora-postgresql",
                MasterUsername=master_username,
                WithExpressConfiguration=True,
                Tags=project_tags_list(),
                **scaling_config,
            )
            logger.info("Creating Aurora cluster %s (express configuration, IAM auth)", cluster_id)
        except ClientError as exc2:
            if exc2.response["Error"]["Code"] == "DBClusterAlreadyExistsFault":
                logger.info("Aurora cluster %s already exists", cluster_id)
            else:
                raise

    instance_id = f"{cluster_id}{_INSTANCE_SUFFIX}"
    try:
        rds.create_db_instance(
            DBInstanceIdentifier=instance_id,
            DBClusterIdentifier=cluster_id,
            Engine="aurora-postgresql",
            DBInstanceClass="db.serverless",
            PubliclyAccessible=False,
            Tags=project_tags_list(),
        )
        logger.info("Creating Aurora instance %s", instance_id)
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in ("DBInstanceAlreadyExists", "InvalidParameterCombination"):
            # Express configuration auto-creates the writer instance.
            logger.info("Aurora instance %s already exists (or was auto-created)", instance_id)
        else:
            raise


def wait_for_cluster_available(cluster_id: str, region: str, timeout_seconds: int = 1800) -> str:
    """Block until the cluster's status is `available`. Returns its writer endpoint."""
    rds = boto3.client("rds", region_name=region)
    deadline = time.monotonic() + timeout_seconds
    status = None
    while time.monotonic() < deadline:
        cluster = rds.describe_db_clusters(DBClusterIdentifier=cluster_id)["DBClusters"][0]
        status = cluster["Status"]
        logger.info("Cluster %s status: %s", cluster_id, status)
        if status == "available":
            return cluster["Endpoint"]
        time.sleep(_POLL_SECONDS)
    raise TimeoutError(
        f"Aurora cluster {cluster_id} did not become available within {timeout_seconds}s "
        f"(last status: {status})"
    )


def attach_s3_import_role(cluster_id: str, role_arn: str, region: str) -> None:
    """Associate the S3-read IAM role with the cluster so `aws_s3.table_import_from_s3`
    can be used from inside Postgres -- only needed for the local ("mimic-load load-raw")
    ingestion path; the Glue path (staging_load.py, via Spark's JDBC writer) never calls
    it and works fine without this association.

    Best-effort: discovered against a real express-configuration cluster (see
    create_serverless_cluster's docstring -- the fallback for accounts without a
    pre-existing VPC subnet group) that add_role_to_db_cluster can reject a
    perfectly-valid role with "IAM role ARN value is invalid or does not include the
    required permissions for: AWS_ROLE_INTEGRATION" -- express configuration's
    AWS-managed networking apparently doesn't support customer role associations the
    same way a standard cluster does. Since this feature is optional for the Glue path
    this repo's `run_end_to_end.py`/`build_datawarehouse.py` default to, any failure
    here (other than the role already being attached) is logged and swallowed rather
    than aborting the whole provisioning run."""
    rds = boto3.client("rds", region_name=region)
    try:
        rds.add_role_to_db_cluster(DBClusterIdentifier=cluster_id, RoleArn=role_arn, FeatureName="s3Import")
        logger.info("Attached role %s to cluster %s for s3Import", role_arn, cluster_id)
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "DBClusterRoleAlreadyExists":
            logger.info("Role already attached to cluster %s", cluster_id)
        else:
            logger.warning(
                "Could not attach S3 import role to cluster %s (%s: %s) -- only needed for "
                "the local ingestion path (mimic-load load-raw); the Glue path doesn't use it. Continuing.",
                cluster_id,
                exc.response["Error"]["Code"],
                exc.response["Error"].get("Message"),
            )


def delete_cluster(
    cluster_id: str,
    region: str,
    *,
    skip_final_snapshot: bool = False,
    final_snapshot_id: str | None = None,
) -> None:
    """Delete every DB instance in `cluster_id`, then the cluster itself.

    By default takes a final snapshot (`final_snapshot_id`, or
    `<cluster_id>-final-<unix-timestamp>` if not given) -- pass
    `skip_final_snapshot=True` to skip it and delete faster/for free. A no-op (logs and
    returns) if the cluster doesn't exist.
    """
    rds = boto3.client("rds", region_name=region)

    try:
        cluster = rds.describe_db_clusters(DBClusterIdentifier=cluster_id)["DBClusters"][0]
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "DBClusterNotFoundFault":
            logger.info("Aurora cluster %s does not exist -- nothing to delete", cluster_id)
            return
        raise

    for member in cluster.get("DBClusterMembers", []):
        instance_id = member["DBInstanceIdentifier"]
        try:
            rds.delete_db_instance(DBInstanceIdentifier=instance_id)
            logger.info("Deleting Aurora instance %s", instance_id)
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "DBInstanceNotFound":
                logger.info("Instance %s already gone", instance_id)
            else:
                raise
        _wait_for_instance_deleted(rds, instance_id)

    delete_kwargs: dict = {"DBClusterIdentifier": cluster_id}
    if skip_final_snapshot:
        delete_kwargs["SkipFinalSnapshot"] = True
    else:
        delete_kwargs["FinalDBSnapshotIdentifier"] = final_snapshot_id or f"{cluster_id}-final-{int(time.time())}"

    try:
        rds.delete_db_cluster(**delete_kwargs)
        if skip_final_snapshot:
            logger.info("Deleting Aurora cluster %s (no final snapshot)", cluster_id)
        else:
            logger.info(
                "Deleting Aurora cluster %s (final snapshot: %s)",
                cluster_id,
                delete_kwargs["FinalDBSnapshotIdentifier"],
            )
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code == "DBClusterNotFoundFault":
            logger.info("Cluster %s already gone", cluster_id)
        elif not skip_final_snapshot and code in _EXPRESS_FALLBACK_CODES:
            # Clusters created via express configuration (see create_serverless_cluster)
            # may not support final snapshots the same way -- fall back to skipping it
            # rather than fail the whole teardown.
            logger.warning(
                "Final snapshot rejected (%s: %s); retrying delete without one.",
                code,
                exc.response["Error"]["Message"],
            )
            rds.delete_db_cluster(DBClusterIdentifier=cluster_id, SkipFinalSnapshot=True)
            logger.info("Deleting Aurora cluster %s (no final snapshot)", cluster_id)
        else:
            raise


def _wait_for_instance_deleted(rds, instance_id: str, timeout_seconds: int = 1800) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            status = rds.describe_db_instances(DBInstanceIdentifier=instance_id)["DBInstances"][0]["DBInstanceStatus"]
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "DBInstanceNotFound":
                logger.info("Instance %s deleted", instance_id)
                return
            raise
        logger.info("Instance %s status: %s", instance_id, status)
        time.sleep(_POLL_SECONDS)
    raise TimeoutError(f"Instance {instance_id} was not deleted within {timeout_seconds}s")


def wait_for_cluster_deleted(cluster_id: str, region: str, timeout_seconds: int = 1800) -> None:
    """Block until `cluster_id` no longer exists."""
    rds = boto3.client("rds", region_name=region)
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            status = rds.describe_db_clusters(DBClusterIdentifier=cluster_id)["DBClusters"][0]["Status"]
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "DBClusterNotFoundFault":
                logger.info("Cluster %s deleted", cluster_id)
                return
            raise
        logger.info("Cluster %s status: %s", cluster_id, status)
        time.sleep(_POLL_SECONDS)
    raise TimeoutError(f"Cluster {cluster_id} was not deleted within {timeout_seconds}s")


def delete_subnet_group(name: str, region: str) -> None:
    """Delete a DB subnet group. A no-op if it doesn't exist or is still in use (the
    cluster must be fully deleted first -- call after `wait_for_cluster_deleted`)."""
    rds = boto3.client("rds", region_name=region)
    try:
        rds.delete_db_subnet_group(DBSubnetGroupName=name)
        logger.info("Deleted DB subnet group %s", name)
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "DBSubnetGroupNotFoundFault":
            logger.info("DB subnet group %s does not exist -- nothing to delete", name)
        else:
            raise
