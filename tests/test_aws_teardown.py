from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError

from mimic_datawarehouse.aws.aurora import delete_cluster, delete_subnet_group, wait_for_cluster_deleted
from mimic_datawarehouse.aws.iam import delete_s3_import_role


def _client_error(code: str, message: str = "boom") -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": message}}, "Operation")


def test_delete_cluster_deletes_each_instance_then_the_cluster_with_a_final_snapshot():
    mock_rds = MagicMock()
    mock_rds.describe_db_clusters.return_value = {
        "DBClusters": [{"DBClusterMembers": [{"DBInstanceIdentifier": "mimic4rds-instance-1"}]}]
    }
    # instance deletion wait: first call still exists, second call raises not-found.
    # "DBInstanceNotFound" (no "Fault" suffix) is the real AWS error Code -- see
    # test_aws_error_codes.py, which verifies this and every other hardcoded code
    # against botocore's own service model. Getting this string wrong here previously
    # meant the test passed while the real code crashed on the exact same mismatch.
    mock_rds.describe_db_instances.side_effect = [
        {"DBInstances": [{"DBInstanceStatus": "deleting"}]},
        _client_error("DBInstanceNotFound"),
    ]

    with patch("mimic_datawarehouse.aws.aurora.boto3.client", return_value=mock_rds), patch(
        "mimic_datawarehouse.aws.aurora._POLL_SECONDS", 0
    ):
        delete_cluster("mimic4rds", "us-east-2", skip_final_snapshot=False)

    mock_rds.delete_db_instance.assert_called_once_with(DBInstanceIdentifier="mimic4rds-instance-1")
    mock_rds.delete_db_cluster.assert_called_once()
    call_kwargs = mock_rds.delete_db_cluster.call_args.kwargs
    assert call_kwargs["DBClusterIdentifier"] == "mimic4rds"
    assert "FinalDBSnapshotIdentifier" in call_kwargs


def test_delete_cluster_is_a_noop_when_cluster_does_not_exist():
    mock_rds = MagicMock()
    mock_rds.describe_db_clusters.side_effect = _client_error("DBClusterNotFoundFault")

    with patch("mimic_datawarehouse.aws.aurora.boto3.client", return_value=mock_rds):
        delete_cluster("mimic4rds", "us-east-2")

    mock_rds.delete_db_instance.assert_not_called()
    mock_rds.delete_db_cluster.assert_not_called()


def test_delete_cluster_falls_back_to_skip_snapshot_on_express_config_rejection():
    mock_rds = MagicMock()
    mock_rds.describe_db_clusters.return_value = {"DBClusters": [{"DBClusterMembers": []}]}
    mock_rds.delete_db_cluster.side_effect = [_client_error("InvalidParameterCombination"), None]

    with patch("mimic_datawarehouse.aws.aurora.boto3.client", return_value=mock_rds):
        delete_cluster("mimic4rds", "us-east-2", skip_final_snapshot=False)

    assert mock_rds.delete_db_cluster.call_count == 2
    second_call_kwargs = mock_rds.delete_db_cluster.call_args_list[1].kwargs
    assert second_call_kwargs == {"DBClusterIdentifier": "mimic4rds", "SkipFinalSnapshot": True}


def test_wait_for_cluster_deleted_returns_once_not_found():
    mock_rds = MagicMock()
    mock_rds.describe_db_clusters.side_effect = _client_error("DBClusterNotFoundFault")

    with patch("mimic_datawarehouse.aws.aurora.boto3.client", return_value=mock_rds):
        wait_for_cluster_deleted("mimic4rds", "us-east-2")  # must not raise/hang


def test_delete_subnet_group_is_a_noop_when_missing():
    mock_rds = MagicMock()
    mock_rds.delete_db_subnet_group.side_effect = _client_error("DBSubnetGroupNotFoundFault")

    with patch("mimic_datawarehouse.aws.aurora.boto3.client", return_value=mock_rds):
        delete_subnet_group("mimic4-dw-subnet-group", "us-east-2")  # must not raise


def test_delete_s3_import_role_deletes_inline_policies_before_the_role():
    mock_iam = MagicMock()
    mock_iam.list_role_policies.return_value = {"PolicyNames": ["mimic4-aurora-s3-import-role-s3-read"]}

    with patch("mimic_datawarehouse.aws.iam.boto3.client", return_value=mock_iam):
        delete_s3_import_role("mimic4-aurora-s3-import-role")

    mock_iam.delete_role_policy.assert_called_once_with(
        RoleName="mimic4-aurora-s3-import-role", PolicyName="mimic4-aurora-s3-import-role-s3-read"
    )
    mock_iam.delete_role.assert_called_once_with(RoleName="mimic4-aurora-s3-import-role")


def test_delete_s3_import_role_is_a_noop_when_missing():
    mock_iam = MagicMock()
    mock_iam.list_role_policies.side_effect = _client_error("NoSuchEntity")

    with patch("mimic_datawarehouse.aws.iam.boto3.client", return_value=mock_iam):
        delete_s3_import_role("mimic4-aurora-s3-import-role")  # must not raise

    mock_iam.delete_role.assert_not_called()
