"""IAM role granting Aurora PostgreSQL read access to the raw-data S3 bucket.

Aurora imports CSVs directly from S3 via the `aws_s3` extension, which requires an
IAM role the RDS service can assume (attached to the cluster with feature `s3Import`).
"""

from __future__ import annotations

import json
import logging

import boto3
from botocore.exceptions import ClientError

from ..config import project_tags_list

logger = logging.getLogger(__name__)

_TRUST_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {"Service": "rds.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }
    ],
}


def _s3_read_policy(bucket_name: str) -> dict:
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3:GetObject"],
                "Resource": f"arn:aws:s3:::{bucket_name}/*",
            },
            {
                "Effect": "Allow",
                "Action": ["s3:ListBucket"],
                "Resource": f"arn:aws:s3:::{bucket_name}",
            },
        ],
    }


def create_s3_import_role(role_name: str, bucket_name: str) -> str:
    """Create (or reuse) the IAM role Aurora assumes to read from `bucket_name`.

    Returns the role's ARN.
    """
    iam = boto3.client("iam")
    try:
        role_arn = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(_TRUST_POLICY),
            Description="Allows Aurora PostgreSQL to read MIMIC-IV raw CSVs from S3 for staging loads.",
            Tags=project_tags_list(),
        )["Role"]["Arn"]
        logger.info("Created IAM role %s", role_arn)
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "EntityAlreadyExists":
            role_arn = iam.get_role(RoleName=role_name)["Role"]["Arn"]
            logger.info("IAM role %s already exists", role_arn)
        else:
            raise

    iam.put_role_policy(
        RoleName=role_name,
        PolicyName=f"{role_name}-s3-read",
        PolicyDocument=json.dumps(_s3_read_policy(bucket_name)),
    )
    return role_arn


def delete_s3_import_role(role_name: str) -> None:
    """Delete the IAM role created by `create_s3_import_role`, and its inline policy
    (a role can't be deleted while it still has one attached). A no-op if it doesn't
    exist."""
    iam = boto3.client("iam")
    try:
        for policy_name in iam.list_role_policies(RoleName=role_name)["PolicyNames"]:
            iam.delete_role_policy(RoleName=role_name, PolicyName=policy_name)
            logger.info("Deleted inline policy %s from role %s", policy_name, role_name)
        iam.delete_role(RoleName=role_name)
        logger.info("Deleted IAM role %s", role_name)
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "NoSuchEntity":
            logger.info("IAM role %s does not exist -- nothing to delete", role_name)
        else:
            raise
