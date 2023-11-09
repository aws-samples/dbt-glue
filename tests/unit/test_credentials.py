import unittest

from dbt.adapters.glue.connections import GlueCredentials


class TestGlueRelation(unittest.TestCase):
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
        assert credentials.glue_version == "4.0"    # default Glue version is 4.0
