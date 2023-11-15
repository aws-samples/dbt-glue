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

# The profile dictionary, used to write out profiles.yml
# dbt will supply a unique schema per test, so we do not specify 'schema' here
@pytest.fixture(scope="class")
def dbt_profile_target():
    return {
        'type': 'glue',
        'query-comment': 'test-glue-adapter',
        'role_arn': os.getenv('DBT_GLUE_ROLE_ARN'),
        'user': os.getenv('DBT_GLUE_ROLE_ARN'),
        'region': os.getenv("DBT_GLUE_REGION", 'eu-west-1'),
        'workers': 2,
        'worker_type': 'G.1X',
        'schema': 'dbt_functional_test_01',
        'database': 'dbt_functional_test_01',
        'session_provisioning_timeout_in_seconds': 300,
        'location': os.getenv('DBT_S3_LOCATION'),
        'datalake_formats': 'delta',
        'conf': "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --conf spark.sql.legacy.allowNonEmptyLocationInCTAS=true",
        'glue_session_reuse': True
    }


@pytest.fixture(scope="class")
def unique_schema(request, prefix) -> str:
    return schema_name


@pytest.fixture(scope="class")
def profiles_config_update(dbt_profile_target, unique_schema):
    outputs = {"default": dbt_profile_target}
    outputs["default"]["database"] = unique_schema
    outputs["default"]["schema"] = unique_schema
    return {"test": {"outputs": outputs, "target": "default"}}


@pytest.fixture(scope='class', autouse=True)
def cleanup():
    cleanup_s3_location(s3bucket + schema_name, region)
    yield
