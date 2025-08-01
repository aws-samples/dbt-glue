import pytest
import os
import random
import string
from tests.util import get_s3_location, get_region, cleanup_s3_location

s3bucket = get_s3_location()
region = get_region()

# Use different database for each test class
@pytest.fixture(scope="class")
def unique_schema(request, prefix) -> str:
    provided_test_schema = os.getenv('DBT_GLUE_TEST_SCHEMA')
    if provided_test_schema:
        return provided_test_schema
    else:
        database_suffix = ''.join(random.choices(string.digits, k=4))
        return f"dbt_functional_test_{database_suffix}"

@pytest.fixture(scope="class")
def use_arrow():
    return False

# The profile dictionary with ICEBERG-ONLY configuration (no Delta to avoid conflicts)
@pytest.fixture(scope="class")
def dbt_profile_target(unique_schema, use_arrow):
    # Generate unique session ID to avoid reusing sessions with Delta configuration
    session_suffix = ''.join(random.choices(string.digits, k=4))
    
    return {
        'type': 'glue',
        'query-comment': 'test-glue-adapter-python-iceberg',
        'role_arn': os.getenv('DBT_GLUE_ROLE_ARN'),
        'user': os.getenv('DBT_GLUE_ROLE_ARN'),
        'region': get_region(),
        'workers': 2,
        'worker_type': 'G.1X',
        'schema': unique_schema,
        'database': unique_schema,
        'session_provisioning_timeout_in_seconds': 300,
        'location': get_s3_location(),
        'datalake_formats': 'iceberg',  # ONLY Iceberg, no Delta
        'conf': f"spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.warehouse={get_s3_location()} --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.sources.partitionOverwriteMode=dynamic",
        'glue_session_reuse': False,  # Force new session to avoid Delta conflicts
        'glue_session_id': f'dbt-python-iceberg-test-{session_suffix}',  # Unique session ID
        'use_arrow': use_arrow
    }

@pytest.fixture(scope='class', autouse=True)
def cleanup(unique_schema):
    cleanup_s3_location(s3bucket + unique_schema, region)
    yield
    cleanup_s3_location(s3bucket + unique_schema, region)
