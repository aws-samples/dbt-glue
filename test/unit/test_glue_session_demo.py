import time
import boto3
import pytest
import logging

logger = logging.getLogger()


@pytest.fixture(scope="module", autouse=True)
def client(region):
    yield boto3.client("glue", region_name=region)


@pytest.fixture(scope="module")
def session_id(client, role, region):
    args = {
        "--enable-glue-datacatalog": "true",
    }
    additional_args = {}
    additional_args["NumberOfWorkers"] = 5
    additional_args["WorkerType"] = "G.1X"

    session = client.create_session(
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
    ready = False
    attempts = 0
    while not ready:
        session = client.get_session(Id=_session_id)
        if session.get("Session", {}).get("Status") != "PROVISIONING":
            client.delete_session(Id=_session_id)
            break

        attempts += 1
        if attempts > 16:
            raise Exception("Timeout waiting for session provisonning")
        time.sleep(1)

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
