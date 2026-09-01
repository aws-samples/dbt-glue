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

    def test_render_sqlproxy_injects_retry_config(self) -> None:
        credentials = GlueCredentials(
            boto_retry_mode="standard",
            boto_retry_max_attempts=7,
        )
        connection = GlueConnection(credentials)

        rendered = connection._render_sqlproxy()

        assert "${BOTO_PREAMBLE}" not in rendered
        assert "${BOTO_RETRIES}" not in rendered
        assert "Config(retries={'max_attempts': 7, 'mode': 'standard'})" in rendered
        assert "boto3.client('glue', config=BOTO_CONFIG)" in rendered
        assert "boto3.client('s3', config=BOTO_CONFIG)" in rendered
        compile(rendered, "<sqlproxy>", "exec")

    def test_render_sqlproxy_omits_unset_retry_mode(self) -> None:
        credentials = GlueCredentials(
            boto_retry_mode=None,
            boto_retry_max_attempts=10,
        )
        connection = GlueConnection(credentials)

        rendered = connection._render_sqlproxy()

        assert "Config(retries={'max_attempts': 10})" in rendered
        compile(rendered, "<sqlproxy>", "exec")

    def _exec_preamble(self, credentials, glue_client, namespace=None):
        """Execs the rendered preamble with a stubbed boto3. Pass a `namespace` to
        re-send it into a session that already ran it, as cursor() does per query."""
        connection = GlueConnection(credentials)
        code = connection._render_boto_preamble()

        def fake_client(service, **kwargs):
            return glue_client if service == "glue" else mock.Mock()

        if namespace is None:
            namespace = {
                "boto3": mock.Mock(client=mock.Mock(side_effect=fake_client)),
                "Config": mock.Mock(),
                "session_id": "mock-session-id",
            }
        exec(compile(code, "<preamble>", "exec"), namespace)
        return namespace

    def test_retry_mode_cannot_inject_code_into_the_session(self) -> None:
        """The retry options are interpolated into code executed inside the session."""
        malicious = "standard'}) ; raise AssertionError('injected')  #"
        glue_client = mock.Mock()
        glue_client.get_session.return_value = {"Session": {}}

        namespace = self._exec_preamble(
            GlueCredentials(boto_retry_mode=malicious), glue_client
        )

        namespace["Config"].assert_called_once_with(
            retries={"max_attempts": 10, "mode": malicious}
        )

    def test_in_session_security_config_lookup_is_cached(self) -> None:
        """GetSession/GetSecurityConfiguration must not run per query: doing so is
        what exhausted the rate limit and surfaced as ThrottlingException."""
        glue_client = mock.Mock()
        glue_client.get_session.return_value = {
            "Session": {"SecurityConfiguration": "my-sec-config"}
        }
        glue_client.get_security_configuration.return_value = {
            "SecurityConfiguration": {
                "EncryptionConfiguration": {
                    "S3Encryption": [{"S3EncryptionMode": "SSE-S3"}]
                }
            }
        }

        namespace = self._exec_preamble(GlueCredentials(), glue_client)

        results = [namespace["_get_upload_extra_args"]() for _ in range(5)]

        assert glue_client.get_session.call_count == 1
        assert glue_client.get_security_configuration.call_count == 1
        assert all(r == {"ServerSideEncryption": "AES256"} for r in results)

    def test_in_session_cache_survives_session_re_init(self) -> None:
        """cursor() re-sends SQLPROXY before every query into the same interpreter, so
        resetting the sentinels there would leave GetSession running once per query."""
        glue_client = mock.Mock()
        glue_client.get_session.return_value = {
            "Session": {"SecurityConfiguration": "my-sec-config"}
        }
        glue_client.get_security_configuration.return_value = {
            "SecurityConfiguration": {
                "EncryptionConfiguration": {
                    "S3Encryption": [{"S3EncryptionMode": "SSE-S3"}]
                }
            }
        }

        credentials = GlueCredentials()
        namespace = None
        results = []
        for _ in range(5):  # each iteration is one cursor(): re-init, then query
            namespace = self._exec_preamble(credentials, glue_client, namespace)
            results.append(namespace["_get_upload_extra_args"]())

        assert glue_client.get_session.call_count == 1
        assert glue_client.get_security_configuration.call_count == 1
        assert all(r == {"ServerSideEncryption": "AES256"} for r in results)

    def test_in_session_clients_are_cached(self) -> None:
        glue_client = mock.Mock()
        glue_client.get_session.return_value = {"Session": {}}

        namespace = self._exec_preamble(GlueCredentials(), glue_client)

        assert namespace["_get_glue_client"]() is namespace["_get_glue_client"]()
        assert namespace["_get_s3_client"]() is namespace["_get_s3_client"]()

    def test_in_session_extra_args_cache_is_not_mutable_by_callers(self) -> None:
        glue_client = mock.Mock()
        glue_client.get_session.return_value = {"Session": {}}

        namespace = self._exec_preamble(GlueCredentials(), glue_client)

        first = namespace["_get_upload_extra_args"]()
        first["ServerSideEncryption"] = "tampered"

        assert namespace["_get_upload_extra_args"]() == {}

    def test_in_session_sse_kms_encryption_args(self) -> None:
        glue_client = mock.Mock()
        glue_client.get_session.return_value = {
            "Session": {"SecurityConfiguration": "my-sec-config"}
        }
        glue_client.get_security_configuration.return_value = {
            "SecurityConfiguration": {
                "EncryptionConfiguration": {
                    "S3Encryption": [
                        {
                            "S3EncryptionMode": "SSE-KMS",
                            "KmsKeyArn": "arn:aws:kms:us-east-1:123456789012:key/abc-123",
                        }
                    ]
                }
            }
        }

        namespace = self._exec_preamble(GlueCredentials(), glue_client)

        assert namespace["_get_upload_extra_args"]() == {
            "ServerSideEncryption": "aws:kms",
            "SSEKMSKeyId": "abc-123",
        }
