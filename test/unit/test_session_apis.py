import pytest
from dbt.adapters.glue import GlueSession, errors, GlueSessionConfig, GlueSessionHandle, GlueSessionCursor


def test_start(session):
    assert session.state == "open"
    assert session.handle is not None


def test_handle(session):
    h: GlueSessionHandle = session.handle
    cursor = h.cursor()
    response = cursor.execute(sql="use default")
    print(response)
    response = cursor.execute(sql="show tables")
    print(response)
    assert True
