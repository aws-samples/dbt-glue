import pytest
import os
import random
import string
from tests.util import get_s3_location, get_region, cleanup_s3_location

s3bucket = get_s3_location()
region = get_region()
database_suffix = ''.join(random.choices(string.digits, k=4))
schema_name = f"dbt_functional_test_{database_suffix}"

# Import the standard functional fixtures as a plugin
# Note: fixtures with session scope need to be local
pytest_plugins = ["dbt.tests.fixtures.project"]

# Use different datatabase for each test class
@pytest.fixture(scope="class")
def unique_schema(request, prefix) -> str:
    database_suffix = ''.join(random.choices(string.digits, k=4))
    return f"dbt_functional_test_{database_suffix}"


# The profile dictionary, used to write out profiles.yml
# dbt will supply a unique schema per test, so we do not specify 'schema' here
@pytest.fixture(scope="class")
def dbt_profile_target(unique_schema):
    return {
        'type': 'glue',
        'query-comment': 'test-glue-adapter',
        'role_arn': os.getenv('DBT_GLUE_ROLE_ARN'),
        'user': os.getenv('DBT_GLUE_ROLE_ARN'),
        'region': os.getenv("DBT_GLUE_REGION", 'eu-west-1'),
        'workers': 2,
        'worker_type': 'G.1X',
        'schema': unique_schema,
        'database': unique_schema,
        'session_provisioning_timeout_in_seconds': 300,
        'location': os.getenv('DBT_S3_LOCATION'),
        'datalake_formats': 'delta',
        'conf': "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --conf spark.sql.legacy.allowNonEmptyLocationInCTAS=true",
        'glue_session_reuse': True
    }


@pytest.fixture(scope='class', autouse=True)
def cleanup(unique_schema):
    cleanup_s3_location(s3bucket + unique_schema, region)
    yield
