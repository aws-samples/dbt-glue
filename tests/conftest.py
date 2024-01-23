import pytest
import os

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
        'use_interactive_session_role_for_api_calls': False,
        'glue_session_reuse': True
    }