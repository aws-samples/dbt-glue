# dbt-glue: AWS Glue Adapter for dbt

## Overview

dbt-glue is an adapter that enables [dbt (data build tool)](https://www.getdbt.com/) to work with AWS Glue's Spark engine. This adapter allows data engineers and analysts to leverage dbt's transformation capabilities while using AWS Glue's serverless Spark environment for data processing.

The adapter implements the dbt adapter protocol for AWS Glue's Spark engine and supports running dbt against Spark through the Glue Interactive Sessions API.

## Development Tenets
- **Keep compatibility**: Do NOT break existing capabilities.

## Key Features

- **AWS Glue Integration**: Run dbt models using AWS Glue's serverless Spark engine
- **Data Lake Support**: Work with data stored in Amazon S3
- **Multiple File Formats**: Support for Parquet, CSV, JSON, and other Spark-compatible formats
- **ACID Transaction Support**: Integration with Apache Hudi, Apache Iceberg, and Delta Lake
- **Incremental Models**: Support for append, insert_overwrite, and merge strategies
- **AWS Lake Formation Integration**: Tag management and data cell filtering capabilities
- **Monitoring**: CloudWatch logs and metrics for monitoring Glue jobs
- **Auto Scaling**: Support for AWS Glue Auto Scaling to optimize resource usage
- **Cross-Account Access**: Ability to access Glue catalogs in other AWS accounts

## Installation

The package can be installed from PyPI:

```bash
pip install dbt-glue
```

## Configuration

### Required IAM Permissions

The adapter requires specific IAM permissions to function properly. These include permissions for:
- Glue database and table operations
- Lake Formation permissions management
- S3 bucket access
- DynamoDB access (for Iceberg locking)

### Profile Configuration

Example profile configuration:

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: glue
      query-comment: This is a glue dbt example
      role_arn: arn:aws:iam::1234567890:role/GlueInteractiveSessionRole
      region: us-east-1
      workers: 2
      worker_type: G.1X
      idle_timeout: 10
      schema: "dbt_demo"
      session_provisioning_timeout_in_seconds: 120
      location: "s3://dbt_demo_bucket/dbt_demo_data"
```

## Supported Features

### Materializations

- **Tables**: Create tables in various file formats
- **Views**: Create views in the Glue Data Catalog
- **Incremental Models**: Support for incremental loading strategies
- **Seeds**: Load CSV files as tables
- **Snapshots**: Track historical data changes (with some limitations)

### Incremental Strategies

1. **append**: Insert new records without updating existing data
2. **insert_overwrite**: Overwrite partitions or entire tables with new data
3. **merge** (Hudi/Iceberg/Delta only): Update existing records and insert new ones based on a unique key

### Data Lake Formats

- **Apache Hudi**: Support for merge operations and time travel
- **Apache Iceberg**: Support for ACID transactions and schema evolution
- **Delta Lake**: Support for ACID transactions and time travel

### Lake Formation Integration

- **Tag Management**: Associate Lake Formation tags with databases, tables, and columns
- **Data Cell Filtering**: Row-level and column-level security controls

## Advanced Features

### Monitoring

The adapter supports monitoring through:
- CloudWatch logs and metrics
- Spark UI for detailed job analysis

### Auto Scaling

Enable AWS Glue Auto Scaling to automatically adjust worker count based on workload:

```yaml
default_arguments: "--enable-auto-scaling=true"
```

### Cross-Account Access

Access Glue catalogs in other AWS accounts by configuring the catalog ID:

```yaml
conf: "--conf hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory --conf spark.hadoop.hive.metastore.glue.catalogid=<TARGET-AWS-ACCOUNT-ID>"
```

## Current Limitations

- Column-level descriptions are not yet supported
- Some dbt features available on core adapters may have limited support

## Version Information

- Current version: 1.10.0b2
- Compatible with dbt-core: 1.10.0b2
- Compatible with dbt-spark: 1.9.2
- Requires Python: 3.9 or higher

## Additional Resources

- [AWS Blog: Build your data pipeline using AWS Lake Formation, AWS Glue, and dbt Core](https://aws.amazon.com/blogs/big-data/build-your-data-pipeline-in-your-aws-modern-data-platform-using-aws-lake-formation-aws-glue-and-dbt-core/)
- [GitHub Repository](https://github.com/aws-samples/dbt-glue)
