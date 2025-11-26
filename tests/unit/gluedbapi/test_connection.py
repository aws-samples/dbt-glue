import unittest
from unittest import mock

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

    @mock.patch("dbt.adapters.glue.gluedbapi.connection.get_session_waiter")
    @mock.patch("dbt.adapters.glue.gluedbapi.connection.boto3")
    def test_client_uses_credentials_retry_settings(self, mock_boto3, mock_waiter) -> None:
        mock_waiter.return_value = mock.Mock()
        mock_session = mock_boto3.session.Session.return_value
        mock_client = mock_session.client.return_value

        credentials = GlueCredentials(
            boto_retry_mode="standard",
            boto_retry_max_attempts=4,
        )

        connection = GlueConnection(credentials)
        client = connection.client

        assert client is mock_client
        assert mock_session.client.call_count == 1

        _, kwargs = mock_session.client.call_args
        retries = kwargs["config"].retries
        assert retries["max_attempts"] == 4
        assert retries["mode"] == "standard"
