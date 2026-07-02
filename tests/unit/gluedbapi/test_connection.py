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

    @mock.patch("dbt.adapters.glue.gluedbapi.connection.get_session_waiter")
    @mock.patch("dbt.adapters.glue.gluedbapi.connection.boto3")
    def test_client_assumes_role_when_flag_enabled(self, mock_boto3, mock_waiter) -> None:
        mock_waiter.return_value = mock.Mock()
        mock_sts_client = mock.Mock()
        mock_glue_client = mock.Mock()
        mock_boto3.client.side_effect = lambda service, **kwargs: (
            mock_sts_client if service == "sts" else mock_glue_client
        )
        mock_sts_client.assume_role.return_value = {
            "Credentials": {
                "AccessKeyId": "AKIA_ASSUMED",
                "SecretAccessKey": "secret_assumed",
                "SessionToken": "token_assumed",
            }
        }

        credentials = GlueCredentials(
            role_arn="arn:aws:iam::123456789012:role/MyRole",
            region="us-east-1",
            use_interactive_session_role_for_api_calls=True,
        )

        connection = GlueConnection(credentials)
        client = connection.client

        assert client is mock_glue_client
        mock_boto3.client.assert_any_call("sts", region_name="us-east-1")
        mock_sts_client.assume_role.assert_called_once_with(
            RoleArn="arn:aws:iam::123456789012:role/MyRole",
            RoleSessionName="dbt-glue-session",
        )
        _, glue_kwargs = [
            call for call in mock_boto3.client.call_args_list if call[0][0] == "glue"
        ][-1]
        assert glue_kwargs["aws_access_key_id"] == "AKIA_ASSUMED"
        assert glue_kwargs["aws_secret_access_key"] == "secret_assumed"
        assert glue_kwargs["aws_session_token"] == "token_assumed"
        assert glue_kwargs["region_name"] == "us-east-1"
