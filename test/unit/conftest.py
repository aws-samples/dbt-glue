import boto3
import os
import pytest
import logging
from dbt.adapters.glue.gluedbapi import GlueConnection
from dbt.adapters.glue.credentials import GlueCredentials

logger = logging.getLogger(name="dbt-glue-tes")
account = "xxxxxxxxxxxx"
region = "eu-west-1"


@pytest.fixture(scope="module")
def role():
    arn = os.environ.get("DBT_GLUE_ROLE_ARN", f"arn:aws:iam::{account}:role/GlueInteractiveSessionRole")
    yield arn


@pytest.fixture(scope="module")
def region():
    r = os.environ.get("DBT_GLUE_REGION", region)
    yield r


@pytest.fixture(scope="module", autouse=True)
def clean():
    yield
    return
    logger.warning("cleanup ")
    r = os.environ.get("DBT_GLUE_REGION", region)
    client = boto3.client("glue", region_name=r)
    sessions = client.list_sessions()
    logger.warning(f"Found {len(sessions['Ids'])} {'-'.join(sessions['Ids'])}")
    for session_id in sessions.get("Ids", []):
        session = client.get_session(Id=session_id)
        status = session.get("Session", {}).get("Status", "")
        logger.warning(f"session {session_id} has status `{status}`")
        if status == "READY":
            client.delete_session(Id=session_id)
            logger.warning(f"deleted session {session_id}")


@pytest.fixture(scope="module")
def credentials():
    return GlueCredentials(
        role_arn=f"arn:aws:iam::{account}:role/GlueInteractiveSessionRole",
        region=region,
        database=None,
        schema="airbotinigo",
        worker_type="G.1X",
        session_provisioning_timeout_in_seconds=30,
        workers=3,
    )


@pytest.fixture(scope="module", autouse=False)
def session(role, region, credentials) -> GlueConnection:
    s = GlueConnection(credentials=credentials)
    s.connect()
    yield s
    # s.client.delete_session(Id=s.session_id)
    # logger.warning(f"Deleted session {s.session_id}`")
