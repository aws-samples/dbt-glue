import pytest
import os
from tests.util import get_s3_location


@pytest.fixture(scope="class")
def dbt_profile_target():
    return {
        'type': 'glue',
        'query-comment': 'dbt-glue s3_tables tests',
        'role_arn': os.getenv('DBT_GLUE_ROLE_ARN'),
        'region': os.getenv('DBT_GLUE_REGION'),
        'glue_version': "4.0",
        'workers': 2,
        'worker_type': 'G.1X',
        'schema': os.getenv('DBT_TEST_USER_1', 'dbt_test_user_1'),
        'session_provisioning_timeout_in_seconds': 300,
        'location': get_s3_location(),
        'conf': 'spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.warehouse={} --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.catalog.glue_catalog.glue.id={}'.format(get_s3_location(), os.getenv('DBT_S3_TABLES_BUCKET')),
        'datalake_formats': 'iceberg',
        'glue_session_id': os.getenv('DBT_GLUE_SESSION_ID', 'dbt-s3-tables-test-session')
    }
