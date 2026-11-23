"""Every `exc.response["Error"]["Code"] == "..."` string in aws/ was, at some point,
written by guessing that boto3's generated exception *class* name (e.g.
`DBInstanceNotFoundFault`) matches the actual `Code` field AWS returns in the response
(e.g. `DBInstanceNotFound` -- no "Fault" suffix). It often doesn't. That exact mismatch
caused a real crash: teardown_datawarehouse.py successfully deleted an Aurora instance,
then blew up checking whether it was gone, because the code compared against
"DBInstanceNotFoundFault" instead of the real code "DBInstanceNotFound".

This test is the fix that generalizes: it looks up every code this codebase relies on
against botocore's own service model (the actual source of truth for what AWS returns)
instead of trusting the class name. Run this after adding any new
`.response["Error"]["Code"] == "..."` check -- add the pair below rather than hoping the
name and the code happen to match.
"""

import gzip
import json
from pathlib import Path

import botocore
import pytest

_BOTOCORE_DATA = Path(botocore.__file__).resolve().parent / "data"


def _load_shapes(service: str, version: str) -> dict:
    path = _BOTOCORE_DATA / service / version / "service-2.json.gz"
    with gzip.open(path, "rb") as f:
        return json.loads(f.read().decode("utf-8"))["shapes"]


_RDS_SHAPES = _load_shapes("rds", "2014-10-31")
_IAM_SHAPES = _load_shapes("iam", "2010-05-08")
_S3_SHAPES = _load_shapes("s3", "2006-03-01")
_GLUE_SHAPES = _load_shapes("glue", "2017-03-31")

# (code literal used in source, botocore shape name it's generated from, shapes dict)
# Every `.response["Error"]["Code"] == "<code literal>"` check in src/mimic_datawarehouse/aws/
# should have an entry here, keyed by the boto3 exception class it corresponds to.
_CHECKED_CODES = [
    ("DBClusterAlreadyExistsFault", "DBClusterAlreadyExistsFault", _RDS_SHAPES),
    ("DBInstanceAlreadyExists", "DBInstanceAlreadyExistsFault", _RDS_SHAPES),
    ("DBClusterRoleAlreadyExists", "DBClusterRoleAlreadyExistsFault", _RDS_SHAPES),
    ("DBClusterNotFoundFault", "DBClusterNotFoundFault", _RDS_SHAPES),
    ("DBInstanceNotFound", "DBInstanceNotFoundFault", _RDS_SHAPES),
    ("DBSubnetGroupNotFoundFault", "DBSubnetGroupNotFoundFault", _RDS_SHAPES),
    ("EntityAlreadyExists", "EntityAlreadyExistsException", _IAM_SHAPES),
    ("NoSuchEntity", "NoSuchEntityException", _IAM_SHAPES),
    ("BucketAlreadyOwnedByYou", "BucketAlreadyOwnedByYou", _S3_SHAPES),
    ("AlreadyExistsException", "AlreadyExistsException", _GLUE_SHAPES),
    ("EntityNotFoundException", "EntityNotFoundException", _GLUE_SHAPES),
    ("IdempotentParameterMismatchException", "IdempotentParameterMismatchException", _GLUE_SHAPES),
]

# Generic/free-form errors (e.g. "InvalidParameterCombination") and account-tier-specific
# ones (e.g. "FreeTierRestrictionError", discovered via live trial against a real
# restricted account, not present in any public service model) aren't backed by a
# modeled exception shape at all, so there's nothing to cross-check them against here.


@pytest.mark.parametrize("code_literal,shape_name,shapes", _CHECKED_CODES, ids=[c[1] for c in _CHECKED_CODES])
def test_hardcoded_error_code_matches_botocore_shape(code_literal, shape_name, shapes):
    shape = shapes.get(shape_name)
    assert shape is not None, f"{shape_name} is not a shape in this botocore version's service model"
    actual_code = shape.get("error", {}).get("code", shape_name)
    assert code_literal == actual_code, (
        f"Code literal {code_literal!r} used in source doesn't match the real AWS error "
        f"code {actual_code!r} for exception class {shape_name} -- this exact mismatch "
        f"pattern (class name has a 'Fault'/'Exception' suffix the real Code lacks) is "
        f"what caused teardown_datawarehouse.py to crash instead of treating "
        f"'already deleted' as a no-op."
    )
