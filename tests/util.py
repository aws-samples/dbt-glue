import os


DEFAULT_REGION = "eu-west-1"


def get_account_id():
    if "DBT_AWS_ACCOUNT" in os.environ:
        return os.environ.get("DBT_AWS_ACCOUNT")
    else:
        raise ValueError("DBT_AWS_ACCOUNT must be configured")


def get_region():
    r = os.environ.get("DBT_GLUE_REGION", DEFAULT_REGION)
    return r


def get_s3_location():
    if "DBT_S3_LOCATION" in os.environ:
        return os.environ.get("DBT_S3_LOCATION")
    else:
        raise ValueError("DBT_S3_LOCATION must be configured")

def get_role_arn():
    return os.environ.get("DBT_GLUE_ROLE_ARN", f"arn:aws:iam::{get_account_id()}:role/GlueInteractiveSessionRole")

