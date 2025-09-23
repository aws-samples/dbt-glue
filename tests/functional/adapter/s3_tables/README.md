# S3 Tables Integration Tests

This directory contains integration tests for Amazon S3 Tables support in dbt-glue.

## Overview

Amazon S3 Tables is a new table type that provides:
- Automatic optimization and compaction
- Built-in metadata management  
- Integration with AWS analytics services
- Simplified table management

## Test Strategy

The tests in this directory are designed to:

1. **Verify Basic Functionality**: Test that S3 tables can be created and queried
2. **Test CTAS Operations**: Verify if CREATE TABLE AS SELECT works with S3 tables
3. **Test Incremental Models**: Check incremental materialization support
4. **Test Merge Strategy**: Verify merge incremental strategy works with S3 tables
5. **Test Partitioning**: Verify partitioned S3 tables work correctly
6. **Test Table Properties**: Check custom table properties support
7. **Test Integration**: Verify S3 tables work with other dbt features
8. **Test Error Handling**: Verify proper error handling and edge cases
9. **Test Backward Compatibility**: Ensure existing functionality remains intact
10. **Test Edge Cases**: Verify boundary conditions and special scenarios

## Environment Variables

The following environment variables are required for testing:

- `DBT_GLUE_ROLE_ARN`: IAM role for Glue Interactive Sessions
- `DBT_GLUE_REGION`: AWS region for testing
- `DBT_S3_LOCATION`: S3 location for test data
- `DBT_S3_TABLES_BUCKET`: S3 bucket name for S3 tables
- `DBT_TEST_USER_1`: Test schema name (auto-generated in CI)
- `DBT_GLUE_SESSION_ID`: Glue session ID (auto-generated in CI)

## Running Tests

### Locally
```bash
# Set required environment variables
export DBT_GLUE_ROLE_ARN="arn:aws:iam::123456789012:role/GlueInteractiveSessionRole"
export DBT_GLUE_REGION="us-east-1"
export DBT_S3_LOCATION="s3://my-test-bucket/dbt-test/"
export DBT_S3_TABLES_BUCKET="123456789012:s3tablescatalog/my-s3-tables-bucket"

# Run S3 tables tests
python -m pytest tests/functional/adapter/s3_tables/ -v
```

### Using tox
```bash
tox -e s3-tables
```

### In CI/CD
The tests run automatically via GitHub Actions when:
- Changes are made to S3 tables test files
- Changes are made to adapter code
- Pull requests are created

## Test Structure

- `conftest.py`: Test configuration and fixtures
- `dbt_project.yml`: dbt project configuration for tests
- `test_s3_tables.py`: Main test suite with comprehensive coverage
- `models/`: Sample models for testing including:
  - Basic S3 tables models
  - Merge strategy test models
  - Edge case validation models
  - Backward compatibility models
  - Comprehensive test scenarios
- `README.md`: This documentation
- `README_MERGE_STRATEGY.md`: Specific documentation for merge strategy tests

## Expected Outcomes

These tests will help us understand:

1. **What works out-of-the-box**: Which dbt features work with S3 tables without code changes
2. **What needs modification**: Which features require adapter changes
3. **Configuration requirements**: What specific configurations are needed for S3 tables
4. **Limitations**: What constraints exist when using S3 tables
5. **Merge strategy compatibility**: How S3 tables work with merge incremental strategy
6. **Error handling**: How the system handles edge cases and invalid configurations
7. **Backward compatibility**: That existing functionality continues to work
8. **Performance characteristics**: How S3 tables perform with various data scenarios

## Next Steps

Based on test results, we will:

1. **Document working features**: Update main README with S3 tables support
2. **Implement missing features**: Add minimal code changes for unsupported operations
3. **Add configuration options**: Extend adapter with S3 tables-specific configurations
4. **Update documentation**: Provide usage examples and best practices
