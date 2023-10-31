import time
import boto3
from botocore.waiter import WaiterModel
from botocore.waiter import create_waiter_with_client
from botocore.exceptions import WaiterError
import pytest
import logging
import uuid


logger = logging.getLogger()


waiter_config = {
    "version": 2,
    "waiters": {
        "SessionReady": {
            "operation": "GetSession",
            "delay": 60,
            "maxAttempts": 10,
            "acceptors": [
                {
                    "matcher": "path",
                    "expected": "READY",
                    "argument": "Session.Status",
                    "state": "success"
                },
                {
                    "matcher": "path",
                    "expected": "STOPPED",
                    "argument": "Session.Status",
                    "state": "failure"
                },
                {
                    "matcher": "path",
                    "expected": "TIMEOUT",
                    "argument": "Session.Status",
                    "state": "failure"
                },
                {
                    "matcher": "path",
                    "expected": "FAILED",
                    "argument": "Session.Status",
                    "state": "failure"
                }
            ]
        }
    }
}
waiter_name = "SessionReady"
waiter_model = WaiterModel(waiter_config)

@pytest.fixture(scope="module", autouse=True)
def client(region):
    yield boto3.client("glue", region_name=region)


@pytest.fixture(scope="module")
def session_id(client, role, region):
    args = {
        "--enable-glue-datacatalog": "true",
    }
    additional_args = {"NumberOfWorkers": 5, "WorkerType": "G.1X"}

    session_waiter = create_waiter_with_client(waiter_name, waiter_model, client)

    session_uuid = uuid.uuid4()
    session_uuid_str = str(session_uuid)
    id = f"test-dbt-glue-{session_uuid_str}"

    session = client.create_session(
        Id=id,
        Role=role,
        DefaultArguments=args,
        Command={
            "Name": "glueetl",
            "PythonVersion": "3"
        },
        **additional_args
    )
    _session_id = session.get("Session", {}).get("Id")
    logger.warning(f"Session Id = {_session_id}")
    logger.warning("Clearing sessions")

    try:
        session_waiter.wait(Id=_session_id)
    except WaiterError as e:
        if "Max attempts exceeded" in str(e):
            raise Exception(f"Timeout waiting for session provisioning: {str(e)}")
        else:
            logger.debug(f"session is already stopped or failed")

    yield _session_id


def test_example(client, session_id):
    response = client.get_session(Id=session_id)
    assert response.get("Session", {}).get("Id") == session_id

    queries = [
        "select 1 as A "
    ]

    for q in queries:
        statement = client.run_statement(
            SessionId=session_id,
            Code=f"spark.sql(q)"
        )
        statement_id = statement["Id"]
        attempts = 10
        done = False
        while not done:
            response = client.get_statement(
                SessionId=session_id,
                Id=statement_id
            )
            print(response)
            state = response.get("Statement", {}).get("State")
            if state in ["RUNNING", "WAITING"]:
                time.sleep(2)
            else:
                break
