import unittest

from dbt_common.exceptions import DbtRuntimeError
from dbt.adapters.glue.connections import GlueCredentials


class TestGlueCredentials(unittest.TestCase):
    def test_credentials(self) -> None:
        credentials = GlueCredentials(
            database="tests",
            schema="tests",
            role_arn="arn:aws:iam::123456789101:role/GlueInteractiveSessionRole",
            region="ap-northeast-1",
            workers=4,
            worker_type="G.2X",
        )
        assert credentials.schema == "tests"
        assert credentials.database is None
        assert credentials.glue_version == "5.0"    # default Glue version is 5.0
        assert credentials.custom_iceberg_catalog_namespace == "glue_catalog"

    def test_statement_poll_interval_validation(self) -> None:
        with self.assertRaises(DbtRuntimeError):
            GlueCredentials(
                statement_poll_interval=0,
            )

    def test_statement_poll_interval_accepts_float(self) -> None:
        credentials = GlueCredentials(
            statement_poll_interval=1.5,
        )
        assert credentials.statement_poll_interval == 1.5

    def test_boto_retry_max_attempts_validation(self) -> None:
        with self.assertRaises(DbtRuntimeError):
            GlueCredentials(
                boto_retry_max_attempts=0,
            )
