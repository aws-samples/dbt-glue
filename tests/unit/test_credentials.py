from dbt.adapters.glue.connections import GlueCredentials


def test_credentials_database_schema() -> None:
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
