import unittest
from dbt.adapters.glue.credentials import GlueCredentials
from dbt.adapters.glue.gluedbapi.connection import GlueConnection
from moto import mock_aws
import boto3

class TestGlueConnection(unittest.TestCase):
    @mock_aws
    def test_connection_state_is_none_for_not_found_session_id(self) -> None:
        connection = GlueConnection(GlueCredentials())
        connection._client = boto3.client("glue", region_name="us-east-1")
        connection._session = {"Session": {"Id": "mock-session-id"}}
        assert connection.state is None
        
