import boto3
import os
import pytest
import logging
from dbt.adapters.glue.gluedbapi import GlueConnection
from dbt.adapters.glue.credentials import GlueCredentials
from tests.util import get_role_arn, get_region

logger = logging.getLogger(name="dbt-glue-test")


@pytest.fixture(scope="module")
def role():
    arn = get_role_arn()
    yield arn


@pytest.fixture(scope="module")
def region():
    r = get_region()
    yield r


@pytest.fixture(scope="module", autouse=True)
def clean():
    yield
    return
    logger.warning("cleanup")
    r = get_region()
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
        role_arn=get_role_arn(),
        region=get_region(),
        database=None,
        schema="airbotinigo",
        worker_type="G.1X",
        session_provisioning_timeout_in_seconds=300,
        workers=3
    )


@pytest.fixture(scope="module", autouse=False)
def session(role, region, credentials) -> GlueConnection:
    s = GlueConnection(credentials=credentials)
    s._connect()
    yield s
    # s.client.delete_session(Id=s.session_id)
    # logger.warning(f"Deleted session {s.session_id}`")
