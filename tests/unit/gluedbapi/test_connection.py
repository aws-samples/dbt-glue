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

    def test_create_session_tolerates_concurrent_already_exists(self) -> None:
        """A concurrent dbt process may create the reusable session first.

        `create_session` then raises AlreadyExistsException, which must be
        swallowed so the caller attaches to the existing session instead of
        failing the run.
        """
        connection = GlueConnection(GlueCredentials())

        mock_client = mock.Mock()

        class AlreadyExistsException(Exception):
            pass

        mock_client.exceptions.AlreadyExistsException = AlreadyExistsException
        mock_client.create_session.side_effect = AlreadyExistsException("session exists")
        connection._client = mock_client

        # Should not raise, and should attach to the existing session id.
        connection._create_session(session_id="dbt-glue")

        assert connection.session_id == "dbt-glue"

    def test_create_session_reraises_other_errors(self) -> None:
        """Errors other than AlreadyExistsException must still propagate."""
        connection = GlueConnection(GlueCredentials())

        mock_client = mock.Mock()

        class AlreadyExistsException(Exception):
            pass

        class InvalidInputException(Exception):
            pass

        mock_client.exceptions.AlreadyExistsException = AlreadyExistsException
        mock_client.create_session.side_effect = InvalidInputException("bad input")
        connection._client = mock_client

        with self.assertRaises(InvalidInputException):
            connection._create_session(session_id="dbt-glue")
