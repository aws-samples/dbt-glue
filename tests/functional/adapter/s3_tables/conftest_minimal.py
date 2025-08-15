import pytest
import os
import random
import string
import boto3
from tests.util import get_s3_location, get_region, cleanup_s3_location


def get_s3_tables_bucket_arn():
    """Get the S3 tables bucket ARN from environment variable"""
    bucket_id = os.getenv('DBT_S3_TABLES_BUCKET')
    if not bucket_id:
        raise ValueError("DBT_S3_TABLES_BUCKET environment variable is required")
    
    if ':' in bucket_id and '/' in bucket_id:
        account_id = bucket_id.split(':')[0]
        bucket_name = bucket_id.split('/')[-1]
        region = get_region()
        return f"arn:aws:s3tables:{region}:{account_id}:bucket/{bucket_name}"
    else:
        raise ValueError(f"Invalid DBT_S3_TABLES_BUCKET format: {bucket_id}")


@pytest.fixture(scope="class")
def unique_schema(request, prefix) -> str:
    provided_test_schema = os.getenv('DBT_GLUE_TEST_SCHEMA')
    if provided_test_schema:
        return provided_test_schema
    else:
        database_suffix = ''.join(random.choices(string.digits, k=4))
        return f"dbt_functional_test_{database_suffix}"


@pytest.fixture(scope="class")
def s3_tables_namespace(unique_schema):
    """Create and manage S3 tables namespace for testing"""
    bucket_arn = get_s3_tables_bucket_arn()
    region = get_region()
    namespace = unique_schema
    s3_tables_bucket = os.getenv('DBT_S3_TABLES_BUCKET')
    
    # Create clients
    s3tables_client = boto3.client('s3tables', region_name=region)
    glue_client = boto3.client('glue', region_name=region)
    
    # Setup
    try:
        # Create namespace
        s3tables_client.create_namespace(
            tableBucketARN=bucket_arn,
            namespace=[namespace]
        )
        
        # Create federated database
        account_id = s3_tables_bucket.split(':')[0]
        glue_client.create_database(
            CatalogId=s3_tables_bucket,
            DatabaseInput={
                'Name': namespace,
                'FederatedDatabase': {
                    'Identifier': f'arn:aws:s3tables:{region}:{account_id}:bucket/*',
                    'ProfileName': 'aws:s3tables',
                    'ConnectionName': 'aws:s3tables'
                }
            }
        )
        
    except Exception as e:
        # If setup fails, skip the test rather than complex error handling
        pytest.skip(f"S3 tables setup failed: {str(e)}")
    
    yield namespace
    
    # Cleanup
    try:
        # Delete tables
        tables_response = s3tables_client.list_tables(
            tableBucketARN=bucket_arn,
            namespace=namespace
        )
        for table in tables_response.get('tables', []):
            s3tables_client.delete_table(
                tableBucketARN=bucket_arn,
                namespace=namespace,
                name=table['name']
            )
        
        # Delete database and namespace
        glue_client.delete_database(CatalogId=s3_tables_bucket, Name=namespace)
        s3tables_client.delete_namespace(tableBucketARN=bucket_arn, namespace=namespace)
        
    except Exception:
        # Ignore cleanup errors in tests
        pass


@pytest.fixture(scope="class")
def dbt_profile_target(unique_schema, s3_tables_namespace):
    session_suffix = ''.join(random.choices(string.digits, k=4))
    
    return {
        'type': 'glue',
        'role_arn': os.getenv('DBT_GLUE_ROLE_ARN'),
        'region': get_region(),
        'glue_version': "4.0",
        'workers': 2,
        'worker_type': 'G.1X',
        'schema': unique_schema,
        'session_provisioning_timeout_in_seconds': 300,
        'location': get_s3_location(),
        'conf': f'spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.warehouse={get_s3_location()} --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.catalog.glue_catalog.glue.id={os.getenv("DBT_S3_TABLES_BUCKET")}',
        'datalake_formats': 'iceberg',
        'glue_session_id': f'dbt-s3-tables-test-{session_suffix}',
        'glue_session_reuse': False
    }


@pytest.fixture(scope='class', autouse=True)
def cleanup_s3_data(unique_schema):
    """Cleanup S3 data files"""
    s3bucket = get_s3_location()
    region = get_region()
    cleanup_s3_location(s3bucket + unique_schema, region)
    yield
    cleanup_s3_location(s3bucket + unique_schema, region)
