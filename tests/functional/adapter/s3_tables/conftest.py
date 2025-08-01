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
    
    # Extract account and bucket name from the format: "account:s3tablescatalog/bucket-name"
    if ':' in bucket_id and '/' in bucket_id:
        account_id = bucket_id.split(':')[0]
        bucket_name = bucket_id.split('/')[-1]
        region = get_region()
        return f"arn:aws:s3tables:{region}:{account_id}:bucket/{bucket_name}"
    else:
        raise ValueError(f"Invalid DBT_S3_TABLES_BUCKET format: {bucket_id}. Expected format: 'account:s3tablescatalog/bucket-name'")


# Use different namespace for each test class
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
    
    # Create S3 tables client
    s3tables_client = boto3.client('s3tables', region_name=region)
    
    # Create namespace before tests
    try:
        print(f"Creating S3 tables namespace: {namespace}")
        s3tables_client.create_namespace(
            tableBucketARN=bucket_arn,
            namespace=[namespace]
        )
        print(f"✅ Created S3 tables namespace: {namespace}")
    except Exception as e:
        print(f"❌ Failed to create S3 tables namespace {namespace}: {str(e)}")
        raise
    
    yield namespace
    
    # Cleanup namespace after tests
    try:
        print(f"Cleaning up S3 tables namespace: {namespace}")
        
        # First, list and delete all tables in the namespace
        try:
            tables_response = s3tables_client.list_tables(
                tableBucketARN=bucket_arn,
                namespace=namespace
            )
            
            for table in tables_response.get('tables', []):
                table_name = table['name']
                print(f"Deleting table: {table_name}")
                s3tables_client.delete_table(
                    tableBucketARN=bucket_arn,
                    namespace=namespace,
                    name=table_name
                )
        except Exception as e:
            print(f"Warning: Failed to list/delete tables in namespace {namespace}: {str(e)}")
        
        # Then delete the namespace
        s3tables_client.delete_namespace(
            tableBucketARN=bucket_arn,
            namespace=namespace
        )
        print(f"✅ Cleaned up S3 tables namespace: {namespace}")
    except Exception as e:
        print(f"❌ Failed to cleanup S3 tables namespace {namespace}: {str(e)}")


@pytest.fixture(scope="class")
def dbt_profile_target(unique_schema, s3_tables_namespace):
    # Generate unique session ID
    session_suffix = ''.join(random.choices(string.digits, k=4))
    
    return {
        'type': 'glue',
        'query-comment': 'dbt-glue s3_tables tests',
        'role_arn': os.getenv('DBT_GLUE_ROLE_ARN'),
        'region': get_region(),
        'glue_version': "4.0",
        'workers': 2,
        'worker_type': 'G.1X',
        'schema': unique_schema,
        'session_provisioning_timeout_in_seconds': 300,
        'location': get_s3_location(),
        'conf': 'spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.warehouse={} --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.catalog.glue_catalog.glue.id={}'.format(get_s3_location(), os.getenv('DBT_S3_TABLES_BUCKET')),
        'datalake_formats': 'iceberg',
        'glue_session_id': f'dbt-s3-tables-test-{session_suffix}',
        'glue_session_reuse': False  # Force new session for clean testing
    }


@pytest.fixture(scope='class', autouse=True)
def cleanup_s3_data(unique_schema):
    """Cleanup S3 data files"""
    s3bucket = get_s3_location()
    region = get_region()
    cleanup_s3_location(s3bucket + unique_schema, region)
    yield
    cleanup_s3_location(s3bucket + unique_schema, region)
