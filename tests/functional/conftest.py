import pytest
import os
import random
import string
from tests.util import get_s3_location, get_region, cleanup_s3_location

s3bucket = get_s3_location()
region = get_region()

# Import the standard functional fixtures as a plugin
# Note: fixtures with session scope need to be local
pytest_plugins = ["dbt.tests.fixtures.project"]

# Use different datatabase for each test class
@pytest.fixture(scope="class")
def unique_schema(request, prefix) -> str:
    provided_test_schema = os.getenv('DBT_GLUE_TEST_SCHEMA')
    if provided_test_schema:
        return provided_test_schema
    else:
        custom_suffix = '_' + os.getenv('DBT_GLUE_CUSTOM_SCHEMA_SUFFIX') if os.getenv('DBT_GLUE_CUSTOM_SCHEMA_SUFFIX') else ''
        database_suffix = ''.join(random.choices(string.digits, k=4))
        return f"dbt_functional_test_{database_suffix}{custom_suffix}"

@pytest.fixture(scope="class")
def use_arrow():
    return False

# The profile dictionary, used to write out profiles.yml
# dbt will supply a unique schema per test, so we do not specify 'schema' here
@pytest.fixture(scope="class")
def dbt_profile_target(unique_schema, use_arrow):
    return {
        'type': 'glue',
        'query-comment': 'test-glue-adapter',
        'role_arn': os.getenv('DBT_GLUE_ROLE_ARN'),
        'user': os.getenv('DBT_GLUE_ROLE_ARN'),
        'region': get_region(),
        'workers': 2,
        'worker_type': 'G.1X',
        'schema': unique_schema,
        'database': unique_schema,
        'session_provisioning_timeout_in_seconds': 300,
        'location': get_s3_location(),
        'datalake_formats': 'delta,iceberg',
        'conf': f"spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --conf spark.sql.legacy.allowNonEmptyLocationInCTAS=true --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.warehouse={get_s3_location()} --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.sources.partitionOverwriteMode=dynamic",
        'glue_session_reuse': True,
        'use_arrow': use_arrow
    }


@pytest.fixture(scope='class', autouse=True)
def cleanup(unique_schema):
    cleanup_s3_location(s3bucket + unique_schema, region)
    yield
    cleanup_s3_location(s3bucket + unique_schema, region)
