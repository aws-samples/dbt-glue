import unittest
from unittest import mock

import boto3
from moto import mock_aws

from dbt.adapters.glue.credentials import GlueCredentials
from dbt.adapters.glue.gluedbapi.connection import GlueConnection


class TestGlueConnection(unittest.TestCase):
    @mock_aws
    def test_connection_state_is_none_for_not_found_session_id(self) -> None:
        connection = GlueConnection(GlueCredentials())
        connection._client = boto3.client("glue", region_name="us-east-1")
        connection._session = {"Session": {"Id": "mock-session-id"}}
        assert connection.state is None

    @mock.patch("dbt.adapters.glue.gluedbapi.connection.get_session_waiter")
    @mock.patch("dbt.adapters.glue.gluedbapi.connection.boto3")
    def test_client_uses_credentials_retry_settings(
        self, mock_boto3, mock_waiter
    ) -> None:
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
    def test_create_session_with_packages(self, mock_boto3, mock_waiter) -> None:
        mock_waiter.return_value = mock.Mock()
        mock_session = mock_boto3.session.Session.return_value
        mock_client = mock_session.client.return_value

        credentials = GlueCredentials(
            role_arn="arn:aws:iam::123456789012:role/GlueRole",
            region="us-east-1",
            workers=2,
            worker_type="G.1X",
            schema="test_schema",
            packages=["validoopsie", "asciimatics"],
        )

        connection = GlueConnection(credentials)
        connection._create_session("test-session-id")

        mock_client.create_session.assert_called_once()
        call_kwargs = mock_client.create_session.call_args[1]

        default_args = call_kwargs["DefaultArguments"]
        assert "--additional-python-modules" in default_args
        assert default_args["--additional-python-modules"] == "validoopsie,asciimatics"

    @mock.patch("dbt.adapters.glue.gluedbapi.connection.get_session_waiter")
    @mock.patch("dbt.adapters.glue.gluedbapi.connection.boto3")
    def test_create_session_without_packages(self, mock_boto3, mock_waiter) -> None:
        mock_waiter.return_value = mock.Mock()
        mock_session = mock_boto3.session.Session.return_value
        mock_client = mock_session.client.return_value

        credentials = GlueCredentials(
            role_arn="arn:aws:iam::123456789012:role/GlueRole",
            region="us-east-1",
            workers=2,
            worker_type="G.1X",
            schema="test_schema",
        )

        connection = GlueConnection(credentials)
        connection._create_session("test-session-id")

        call_kwargs = mock_client.create_session.call_args[1]
        default_args = call_kwargs["DefaultArguments"]
        assert "--additional-python-modules" not in default_args

    @mock.patch("dbt.adapters.glue.gluedbapi.connection.get_session_waiter")
    @mock.patch("dbt.adapters.glue.gluedbapi.connection.boto3")
    def test_create_session_with_default_arguments_dict_and_list_values(
        self, mock_boto3, mock_waiter
    ) -> None:
        """Test that default_arguments as a dict with list values joins them into comma-separated strings."""
        mock_waiter.return_value = mock.Mock()
        mock_session = mock_boto3.session.Session.return_value
        mock_client = mock_session.client.return_value

        credentials = GlueCredentials(
            role_arn="arn:aws:iam::123456789012:role/GlueRole",
            region="us-east-1",
            workers=2,
            worker_type="G.1X",
            schema="test_schema",
            default_arguments="--enable-metrics: true, --additional-python-modules:numpy,pandas,scikit-learn",
        )

        connection = GlueConnection(credentials)
        connection._create_session("test-session-id")

        mock_client.create_session.assert_called_once()
        call_kwargs = mock_client.create_session.call_args[1]
        default_args = call_kwargs["DefaultArguments"]

        assert default_args["--enable-metrics"] == "true"
        assert "--additional-python-modules" in default_args
        assert (
            default_args["--additional-python-modules"] == "numpy,pandas,scikit-learn"
        )
