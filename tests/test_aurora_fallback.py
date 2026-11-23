from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError

from mimic_datawarehouse.aws.aurora import attach_s3_import_role, create_serverless_cluster


def _client_error(code: str, message: str = "boom") -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": message}}, "CreateDBCluster")


def test_falls_back_to_express_configuration_on_free_tier_restriction():
    """Reproduces what happened against the real account: the standard, VPC-attached,
    password-authenticated create_db_cluster call is rejected, and the code should
    retry with express configuration (no DB name / password / VPC / encryption flag)."""
    mock_rds = MagicMock()
    mock_rds.create_db_cluster.side_effect = [
        _client_error("FreeTierRestrictionError"),
        None,  # express-configuration retry succeeds
    ]
    mock_rds.create_db_instance.side_effect = _client_error("InvalidParameterCombination")

    with patch("mimic_datawarehouse.aws.aurora.boto3.client", return_value=mock_rds):
        create_serverless_cluster(
            cluster_id="mimic4rds",
            db_name="mimic4rds",
            master_username="mimic_admin",
            master_password="s3cret!",
            region="us-east-2",
            subnet_group_name="my-subnet-group",
            security_group_ids=["sg-123"],
            min_acu=0.5,
            max_acu=4,
        )

    assert mock_rds.create_db_cluster.call_count == 2

    standard_kwargs = mock_rds.create_db_cluster.call_args_list[0].kwargs
    assert standard_kwargs["MasterUserPassword"] == "s3cret!"
    assert standard_kwargs["DBSubnetGroupName"] == "my-subnet-group"

    express_kwargs = mock_rds.create_db_cluster.call_args_list[1].kwargs
    assert express_kwargs["WithExpressConfiguration"] is True
    for forbidden in ("DatabaseName", "MasterUserPassword", "DBSubnetGroupName", "VpcSecurityGroupIds", "StorageEncrypted"):
        assert forbidden not in express_kwargs


def test_falls_back_to_express_configuration_on_missing_subnet_group():
    """A brand-new AWS account has no pre-existing custom VPC subnet group either --
    reproduces the real failure (DBSubnetGroupNotFoundFault) hit against a fresh account
    with no subnet group ever created, and the code should fall back the same way it
    does for FreeTierRestrictionError."""
    mock_rds = MagicMock()
    mock_rds.create_db_cluster.side_effect = [
        _client_error("DBSubnetGroupNotFoundFault", "DB subnet group 'mimic4-dw-subnet-group' does not exist."),
        None,  # express-configuration retry succeeds
    ]
    mock_rds.create_db_instance.side_effect = _client_error("InvalidParameterCombination")

    with patch("mimic_datawarehouse.aws.aurora.boto3.client", return_value=mock_rds):
        create_serverless_cluster(
            cluster_id="mimic4rds",
            db_name="mimic4rds",
            master_username="mimic_admin",
            master_password="s3cret!",
            region="us-east-2",
            subnet_group_name="my-subnet-group",
            security_group_ids=["sg-123"],
            min_acu=0.5,
            max_acu=4,
        )

    assert mock_rds.create_db_cluster.call_count == 2
    express_kwargs = mock_rds.create_db_cluster.call_args_list[1].kwargs
    assert express_kwargs["WithExpressConfiguration"] is True


def test_already_existing_cluster_is_not_an_error():
    mock_rds = MagicMock()
    mock_rds.create_db_cluster.side_effect = _client_error("DBClusterAlreadyExistsFault")

    with patch("mimic_datawarehouse.aws.aurora.boto3.client", return_value=mock_rds):
        create_serverless_cluster(
            cluster_id="mimic4rds",
            db_name="mimic4rds",
            master_username="mimic_admin",
            master_password="s3cret!",
            region="us-east-2",
            subnet_group_name="my-subnet-group",
            security_group_ids=["sg-123"],
            min_acu=0.5,
            max_acu=4,
        )

    mock_rds.create_db_instance.assert_not_called()


def test_attach_s3_import_role_succeeds():
    mock_rds = MagicMock()

    with patch("mimic_datawarehouse.aws.aurora.boto3.client", return_value=mock_rds):
        attach_s3_import_role("mimic4rds", "arn:aws:iam::123456789012:role/my-role", "us-east-2")

    mock_rds.add_role_to_db_cluster.assert_called_once_with(
        DBClusterIdentifier="mimic4rds", RoleArn="arn:aws:iam::123456789012:role/my-role", FeatureName="s3Import"
    )


def test_attach_s3_import_role_already_attached_is_not_an_error():
    mock_rds = MagicMock()
    mock_rds.add_role_to_db_cluster.side_effect = _client_error("DBClusterRoleAlreadyExists")

    with patch("mimic_datawarehouse.aws.aurora.boto3.client", return_value=mock_rds):
        attach_s3_import_role("mimic4rds", "arn:aws:iam::123456789012:role/my-role", "us-east-2")  # must not raise


def test_attach_s3_import_role_is_best_effort_on_express_configuration():
    """Reproduces a real failure: express-configuration clusters (see
    create_serverless_cluster's fallback) can reject add_role_to_db_cluster entirely --
    this feature is only needed for the local ingestion path, not the Glue path this
    repo's provisioning scripts default to, so the failure must not abort the run."""
    mock_rds = MagicMock()
    mock_rds.add_role_to_db_cluster.side_effect = _client_error(
        "InvalidParameterValue",
        "IAM role ARN value is invalid or does not include the required permissions for: AWS_ROLE_INTEGRATION",
    )

    with patch("mimic_datawarehouse.aws.aurora.boto3.client", return_value=mock_rds):
        attach_s3_import_role("mimic4rds", "arn:aws:iam::123456789012:role/my-role", "us-east-2")  # must not raise
