import pytest
import os
import random
import string
import boto3
from tests.util import get_s3_location, get_region, cleanup_s3_location


def grant_minimal_lake_formation_permissions(database_name: str, role_arn: str, s3_tables_bucket: str):
    """Grant minimal Lake Formation permissions for S3 tables operations"""
    try:
        lf_client = boto3.client('lakeformation', region_name=get_region())
        account_id = s3_tables_bucket.split(':')[0]
        
        # Grant database permissions
        lf_client.grant_permissions(
            CatalogId=account_id,
            Principal={'DataLakePrincipalIdentifier': role_arn},
            Resource={'Database': {'CatalogId': s3_tables_bucket, 'Name': database_name}},
            Permissions=['ALL'],
            PermissionsWithGrantOption=['ALL']
        )
        
        # Grant table wildcard permissions
        lf_client.grant_permissions(
            CatalogId=account_id,
            Principal={'DataLakePrincipalIdentifier': role_arn},
            Resource={'Table': {'CatalogId': s3_tables_bucket, 'DatabaseName': database_name, 'TableWildcard': {}}},
            Permissions=['ALL'],
            PermissionsWithGrantOption=['ALL']
        )
        
        print(f"Granted Lake Formation permissions for database: {database_name}")
        
    except Exception as e:
        print(f"Failed to grant Lake Formation permissions: {str(e)}")
        # Don't fail the test, just warn


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
        
        # Create federated database - try different configurations
        account_id = s3_tables_bucket.split(':')[0]
        database_created = False
        
        # Try different database creation approaches
        for attempt, (use_catalog_id, use_profile) in enumerate([
            (True, False),   # CatalogId + ConnectionName
            (True, True),    # CatalogId + ProfileName  
            (False, False),  # Default catalog + ConnectionName
        ]):
            try:
                db_input = {
                    'Name': namespace,
                    'FederatedDatabase': {
                        'Identifier': f'arn:aws:s3tables:{region}:{account_id}:bucket/*',
                    }
                }
                
                if use_profile:
                    db_input['FederatedDatabase']['ProfileName'] = 'aws:s3tables'
                else:
                    db_input['FederatedDatabase']['ConnectionName'] = 'aws:s3tables'
                
                if use_catalog_id:
                    glue_client.create_database(CatalogId=s3_tables_bucket, DatabaseInput=db_input)
                else:
                    glue_client.create_database(DatabaseInput=db_input)
                
                database_created = True
                break
                
            except Exception as e:
                error_msg = str(e)
                # If database already exists, that's fine - continue
                if 'AlreadyExistsException' in error_msg or 'already exists' in error_msg:
                    database_created = True
                    break
                # For other errors, try next approach
                continue
        
        if not database_created:
            print("Warning: Could not create database, but continuing with test")
        
        # Grant Lake Formation permissions for this database
        role_arn = os.getenv('DBT_GLUE_ROLE_ARN')
        
        if role_arn:
            grant_minimal_lake_formation_permissions(namespace, role_arn, s3_tables_bucket)
        else:
            print("DBT_GLUE_ROLE_ARN not set - skipping Lake Formation permissions")
        
    except Exception as e:
        # Show the actual error for debugging but don't fail setup
        print(f"S3 tables setup warning: {str(e)}")
        # Continue with the test even if setup has issues
    
    yield namespace
    
    # Cleanup
    try:
        # Check if cleanup should be skipped for troubleshooting
        skip_cleanup = os.getenv('DBT_SKIP_RESOURCE_CLEANUP', 'false').lower() == 'true'
        if skip_cleanup:
            print(f"⚠️ Skipping S3 Tables cleanup for troubleshooting - namespace: {namespace}")
            print(f"   Bucket ARN: {bucket_arn}")
            print(f"   Tables and namespace will be retained for investigation")
            return
        
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
        
        # Delete database - try with catalog ID first, then without
        try:
            glue_client.delete_database(CatalogId=s3_tables_bucket, Name=namespace)
        except Exception:
            try:
                glue_client.delete_database(Name=namespace)
            except Exception:
                pass  # Ignore database deletion errors
        
        # Delete namespace
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
        'glue_version': "5.0",
        'workers': 2,
        'worker_type': 'G.1X',
        'schema': unique_schema,
        'session_provisioning_timeout_in_seconds': 300,
        'location': get_s3_location(),
        'conf': f'spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.defaultCatalog=glue_catalog --conf spark.sql.catalog.glue_catalog.warehouse={get_s3_location()} --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.catalog.glue_catalog.glue.id={os.getenv("DBT_S3_TABLES_BUCKET")} --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog --conf spark.sql.catalog.spark_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog',
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

    # Check if cleanup should be skipped for troubleshooting
    skip_cleanup = os.getenv('DBT_SKIP_RESOURCE_CLEANUP', 'false').lower() == 'true'
    if skip_cleanup:
        print(f"⚠️ Skipping S3 cleanup for troubleshooting - schema: {unique_schema}")
        print(f"   S3 location: {s3bucket + unique_schema}")
        return

    cleanup_s3_location(s3bucket + unique_schema, region)
