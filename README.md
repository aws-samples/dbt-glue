<p align="center">
  <img src="/etc/dbt-logo-full.svg" alt="dbt logo" width="500"/>
</p>

**[dbt](https://www.getdbt.com/)** enables data analysts and engineers to transform their data using the same practices that software engineers use to build applications.
dbt is the T in ELT. Organize, cleanse, denormalize, filter, rename, and pre-aggregate the raw data in your warehouse so that it's ready for analysis.

# dbt-glue

The `dbt-glue` package implements the [dbt adapter](https://docs.getdbt.com/docs/contributing/building-a-new-adapter) protocol for AWS Glue's Spark engine. 
It supports running dbt against Spark, through the new Glue Interactive Sessions API.

To learn how to deploy a data pipeline in your modern data platform using the `dbt-glue` adapter, please read the following blog post: [Build your data pipeline in your AWS modern data platform using AWS Lake Formation, AWS Glue, and dbt Core](https://aws.amazon.com/blogs/big-data/build-your-data-pipeline-in-your-aws-modern-data-platform-using-aws-lake-formation-aws-glue-and-dbt-core/)

## Installation

The package can be installed from PyPI with:

```bash
$ pip3 install dbt-glue
```
For further (and more likely up-to-date) info, see the [README](https://github.com/aws-samples/dbt-glue#readme)


## Connection Methods


### Configuring your AWS profile for Glue Interactive Session
There are two IAM principals used with interactive sessions.
- Client principal: The princpal (either user or role) calling the AWS APIs (Glue, Lake Formation, Interactive Sessions)
from the local client. This is the principal configured in the AWS CLI and likely the same.
- Service role: The IAM role that AWS Glue uses to execute your session. This is the same as AWS Glue
ETL.

Read [this documentation](https://docs.aws.amazon.com/glue/latest/dg/glue-is-security.html) to configure these principals.

You will find bellow a least privileged policy to enjoy all features of **`dbt-glue`** adapter.

Please to update variables between **`<>`**, here are explanations of these arguments:

|Args	|Description	| 
|---|---|
|region|The region where your Glue database is stored |
|AWS Account|The AWS account where you run your pipeline|
|dbt output database|The database updated by dbt (this is the schema configured in the profile.yml of your dbt environment)|
|dbt source database|All databases used as source|
|dbt output bucket|The bucket name where the data will be generate dbt (the location configured in the profile.yml of your dbt environment)|
|dbt source bucket|The bucket name of source databases (if they are not managed by Lake Formation)|

```yaml
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "Read_and_write_databases",
            "Action": [
                "glue:SearchTables",
                "glue:BatchCreatePartition",
                "glue:CreatePartitionIndex",
                "glue:DeleteDatabase",
                "glue:GetTableVersions",
                "glue:GetPartitions",
                "glue:DeleteTableVersion",
                "glue:UpdateTable",
                "glue:DeleteTable",
                "glue:DeletePartitionIndex",
                "glue:GetTableVersion",
                "glue:UpdateColumnStatisticsForTable",
                "glue:CreatePartition",
                "glue:UpdateDatabase",
                "glue:CreateTable",
                "glue:GetTables",
                "glue:GetDatabases",
                "glue:GetTable",
                "glue:GetDatabase",
                "glue:GetPartition",
                "glue:UpdateColumnStatisticsForPartition",
                "glue:CreateDatabase",
                "glue:BatchDeleteTableVersion",
                "glue:BatchDeleteTable",
                "glue:DeletePartition",
                "glue:GetUserDefinedFunctions",
                "lakeformation:ListResources",
                "lakeformation:BatchGrantPermissions",
                "lakeformation:ListPermissions", 
                "lakeformation:GetDataAccess",
                "lakeformation:GrantPermissions",
                "lakeformation:RevokePermissions",
                "lakeformation:BatchRevokePermissions",
                "lakeformation:AddLFTagsToResource",
                "lakeformation:RemoveLFTagsFromResource",
                "lakeformation:GetResourceLFTags",
                "lakeformation:ListLFTags",
                "lakeformation:GetLFTag",
            ],
            "Resource": [
                "arn:aws:glue:<region>:<AWS Account>:catalog",
                "arn:aws:glue:<region>:<AWS Account>:table/<dbt output database>/*",
                "arn:aws:glue:<region>:<AWS Account>:database/<dbt output database>"
            ],
            "Effect": "Allow"
        },
        {
            "Sid": "Read_only_databases",
            "Action": [
                "glue:SearchTables",
                "glue:GetTableVersions",
                "glue:GetPartitions",
                "glue:GetTableVersion",
                "glue:GetTables",
                "glue:GetDatabases",
                "glue:GetTable",
                "glue:GetDatabase",
                "glue:GetPartition",
                "lakeformation:ListResources",
                "lakeformation:ListPermissions"
            ],
            "Resource": [
                "arn:aws:glue:<region>:<AWS Account>:table/<dbt source database>/*",
                "arn:aws:glue:<region>:<AWS Account>:database/<dbt source database>",
                "arn:aws:glue:<region>:<AWS Account>:database/default",
                "arn:aws:glue:<region>:<AWS Account>:database/global_temp"
            ],
            "Effect": "Allow"
        },
        {
            "Sid": "Storage_all_buckets",
            "Action": [
                "s3:GetBucketLocation",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::<dbt output bucket>",
                "arn:aws:s3:::<dbt source bucket>"
            ],
            "Effect": "Allow"
        },
        {
            "Sid": "Read_and_write_buckets",
            "Action": [
                "s3:PutObject",
                "s3:PutObjectAcl",
                "s3:GetObject",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::<dbt output bucket>"
            ],
            "Effect": "Allow"
        },
        {
            "Sid": "Read_only_buckets",
            "Action": [
                "s3:GetObject"
            ],
            "Resource": [
                "arn:aws:s3:::<dbt source bucket>"
            ],
            "Effect": "Allow"
        }
    ]
}
```

### Configuration of the local environment

Because **`dbt`** and **`dbt-glue`** adapter are compatible with Python versions 3.7, 3.8, and 3.9, check the version of Python:

```bash
$ python3 --version
```

Configure a Python virtual environment to isolate package version and code dependencies:

```bash
$ sudo yum install git
$ python3 -m venv dbt_venv
$ source dbt_venv/bin/activate
$ python3 -m pip install --upgrade pip
```

Configure the last version of AWS CLI

```bash
$ curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
$ unzip awscliv2.zip
$ sudo ./aws/install
```

Install boto3 package

```bash
$ sudo yum install gcc krb5-devel.x86_64 python3-devel.x86_64 -y
$ pip3 install —upgrade boto3
```

Install the package:

```bash
$ pip3 install dbt-glue
```

### Example config

```yml
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

The table below describes all the options.

| Option	                                 | Description	                                                                                                                                                                                                                                                                                      | Mandatory |
|-----------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------|
| project_name	                           | The dbt project name. This must be the same as the one configured in the dbt project.	                                                                                                                                                                                                            | yes       |
| type	                                   | The driver to use.	                                                                                                                                                                                                                                                                               | yes       |
| query-comment	                          | A string to inject as a comment in each query that dbt runs. 	                                                                                                                                                                                                                                    | no        |
| role_arn	                               | The ARN of the glue interactive session IAM role.	                                                                                                                                                                                                                                                | yes       |
| region	                                 | The AWS Region were you run the data pipeline.	                                                                                                                                                                                                                                                   | yes       |
| workers	                                | The number of workers of a defined workerType that are allocated when a job runs.	                                                                                                                                                                                                                | yes       |
| worker_type	                            | The type of predefined worker that is allocated when a job runs. Accepts a value of Standard, G.1X, or G.2X.	                                                                                                                                                                                     | yes       |
| schema	                                 | The schema used to organize data stored in Amazon S3.Additionally, is the database in AWS Lake Formation that stores metadata tables in the Data Catalog.	                                                                                                                                        | yes       |
| session_provisioning_timeout_in_seconds | The timeout in seconds for AWS Glue interactive session provisioning.	                                                                                                                                                                                                                            | yes       |
| location	                               | The Amazon S3 location of your target data.	                                                                                                                                                                                                                                                      | yes       |
| query_timeout_in_minutes	               | The timeout in minutes for a signle query. Default is 300                                                                                                                                                                                                                                         | no        |
| idle_timeout	                           | The AWS Glue session idle timeout in minutes. (The session stops after being idle for the specified amount of time)	                                                                                                                                                                              | no        |
| glue_version	                           | The version of AWS Glue for this session to use. Currently, the only valid options are 2.0 and 3.0. The default value is 3.0.	                                                                                                                                                                    | no        |
| security_configuration	                 | The security configuration to use with this session.	                                                                                                                                                                                                                                             | no        |
| connections	                            | A comma-separated list of connections to use in the session.	                                                                                                                                                                                                                                     | no        |
| conf	                                   | Specific configuration used at the startup of the Glue Interactive Session (arg --conf)	                                                                                                                                                                                                          | no        |
| extra_py_files	                         | Extra python Libs that can be used by the interactive session.                                                                                                                                                                                                                                    | no        |
| delta_athena_prefix	                    | A prefix used to create Athena compatible tables for Delta tables	(if not specified, then no Athena compatible table will be created)                                                                                                                                                             | no        |
| tags	                                   | The map of key value pairs (tags) belonging to the session. Ex: `KeyName1=Value1,KeyName2=Value2`                                                                                                                                                                                                 | no        |
| seed_format	                            | By default `parquet`, can be Spark format compatible like `csv` or `json`                                                                                                                                                                                                                         | no        |
| seed_mode	                              | By default `overwrite`, the seed data will be overwritten, you can set it to `append` if you just want to add new data in your dataset                                                                                                                                                            | no        |
| default_arguments	                      | The map of key value pairs parameters belonging to the session. More information on [Job parameters used by AWS Glue](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-glue-arguments.html). Ex: `--enable-continuous-cloudwatch-log=true,--enable-continuous-log-filter=true` | no        |
| glue_session_id                         | re-use a glue-session to run multiple dbt run commands. Will create a new glue-session using glue_session_id if it does not exists yet.                                                                                                                        | no        | 
| glue_session_reuse                      | re-use the glue-session to run multiple dbt run commands: If set to true, the glue session will not be closed for re-use. If set to false, the session will be closed. The glue session will close after idle_timeout time is expired after idle_timeout time                                                                                                                            | no        | 
| datalake_formats	                      | The ACID datalake format that you want to use if you are doing merge, can be `hudi`, `ìceberg` or `delta`                                                                                                                                                                                          |no|

## Configs

### Configuring tables

When materializing a model as `table`, you may include several optional configs that are specific to the dbt-spark plugin, in addition to the standard [model configs](model-configs).

| Option  | Description                                        | Required?               | Example                  |
|---------|----------------------------------------------------|-------------------------|--------------------------|
| file_format | The file format to use when creating tables (`parquet`, `csv`, `json`, `text`, `jdbc` or `orc`). | Optional | `parquet`|
| partition_by  | Partition the created table by the specified columns. A directory is created for each partition. | Optional                | `date_day`              |
| clustered_by  | Each partition in the created table will be split into a fixed number of buckets by the specified columns. | Optional               | `country_code`              |
| buckets  | The number of buckets to create while clustering | Required if `clustered_by` is specified                | `8`              |
| custom_location  | By default, the adapter will store your data in the following path: `location path`/`schema`/`table`. If you don't want to follow that default behaviour, you can use this parameter to set your own custom location on S3 | No | `s3://mycustombucket/mycustompath`              |
| hudi_options | When using file_format `hudi`, gives the ability to overwrite any of the default configuration options. | Optional | `{'hoodie.schema.on.read.enable': 'true'}` |
## Incremental models

dbt seeks to offer useful and intuitive modeling abstractions by means of its built-in configurations and materializations.

For that reason, the dbt-glue plugin leans heavily on the [`incremental_strategy` config](configuring-incremental-models#about-incremental_strategy). This config tells the incremental materialization how to build models in runs beyond their first. It can be set to one of three values:
 - **`append`** (default): Insert new records without updating or overwriting any existing data.
 - **`insert_overwrite`**: If `partition_by` is specified, overwrite partitions in the table with new data. If no `partition_by` is specified, overwrite the entire table with new data.
 - **`merge`** (Apache Hudi and Apache Iceberg only): Match records based on a `unique_key`; update old records, insert new ones. (If no `unique_key` is specified, all new data is inserted, similar to `append`.)
 
Each of these strategies has its pros and cons, which we'll discuss below. As with any model config, `incremental_strategy` may be specified in `dbt_project.yml` or within a model file's `config()` block.

**Notes:**
The default strategy is **`insert_overwrite`**

### The `append` strategy

Following the `append` strategy, dbt will perform an `insert into` statement with all new data. The appeal of this strategy is that it is straightforward and functional across all platforms, file types, connection methods, and Apache Spark versions. However, this strategy _cannot_ update, overwrite, or delete existing data, so it is likely to insert duplicate records for many data sources.

#### Source code
```sql
{{ config(
    materialized='incremental',
    incremental_strategy='append',
) }}

--  All rows returned by this query will be appended to the existing table

select * from {{ ref('events') }}
{% if is_incremental() %}
  where event_ts > (select max(event_ts) from {{ this }})
{% endif %}
```
#### Run Code
```sql
create temporary view spark_incremental__dbt_tmp as

    select * from analytics.events

    where event_ts >= (select max(event_ts) from {{ this }})

;

insert into table analytics.spark_incremental
    select `date_day`, `users` from spark_incremental__dbt_tmp
```

### The `insert_overwrite` strategy

This strategy is most effective when specified alongside a `partition_by` clause in your model config. dbt will run an [atomic `insert overwrite` statement](https://spark.apache.org/docs/latest/sql-ref-syntax-dml-insert-overwrite-table.html) that dynamically replaces all partitions included in your query. Be sure to re-select _all_ of the relevant data for a partition when using this incremental strategy.

If no `partition_by` is specified, then the `insert_overwrite` strategy will atomically replace all contents of the table, overriding all existing data with only the new records. The column schema of the table remains the same, however. This can be desirable in some limited circumstances, since it minimizes downtime while the table contents are overwritten. The operation is comparable to running `truncate` + `insert` on other databases. For atomic replacement of Delta-formatted tables, use the `table` materialization (which runs `create or replace`) instead.

#### Source Code
```sql
{{ config(
    materialized='incremental',
    partition_by=['date_day'],
    file_format='parquet'
) }}

/*
  Every partition returned by this query will be overwritten
  when this model runs
*/

with new_events as (

    select * from {{ ref('events') }}

    {% if is_incremental() %}
    where date_day >= date_add(current_date, -1)
    {% endif %}

)

select
    date_day,
    count(*) as users

from events
group by 1
```

#### Run Code

```sql
create temporary view spark_incremental__dbt_tmp as

    with new_events as (

        select * from analytics.events


        where date_day >= date_add(current_date, -1)


    )

    select
        date_day,
        count(*) as users

    from events
    group by 1

;

insert overwrite table analytics.spark_incremental
    partition (date_day)
    select `date_day`, `users` from spark_incremental__dbt_tmp
```

Specifying `insert_overwrite` as the incremental strategy is optional, since it's the default strategy used when none is specified.

### The `merge` strategy

**Compatibility:**
- Hudi : OK
- Delta Lake : OK
- Iceberg : OK
- Lake Formation Governed Tables : On going

NB: 

- For Glue 3: you have to setup a [Glue connectors](https://docs.aws.amazon.com/glue/latest/ug/connectors-chapter.html).

- For Glue 4: use the `datalake_formats` option in your profile.yml

When using a connector be sure that your IAM role has these policies:
```
{
    "Sid": "access_to_connections",
    "Action": [
        "glue:GetConnection",
        "glue:GetConnections"
    ],
    "Resource": [
        "arn:aws:glue:<region>:<AWS Account>:catalog",
        "arn:aws:glue:<region>:<AWS Account>:connection/*"
    ],
    "Effect": "Allow"
}
```
and that the managed policy `AmazonEC2ContainerRegistryReadOnly` is attached. 
Be sure that you follow the getting started instructions [here](https://docs.aws.amazon.com/glue/latest/ug/setting-up.html#getting-started-min-privs-connectors).


This [blog post](https://aws.amazon.com/blogs/big-data/part-1-integrate-apache-hudi-delta-lake-apache-iceberg-datasets-at-scale-aws-glue-studio-notebook/) also explain how to setup and works with Glue Connectors

#### Hudi

**Usage notes:** The `merge` with Hudi incremental strategy requires:
- To add `file_format: hudi` in your table configuration
- To add a datalake_formats in your profile : `datalake_formats: hudi`
  - Alternatively, to add a connections in your profile : `connections: name_of_your_hudi_connector`
- To add Kryo serializer in your Interactive Session Config (in your profile):  `conf: spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.hive.convertMetastoreParquet=false`

dbt will run an [atomic `merge` statement](https://hudi.apache.org/docs/writing_data#spark-datasource-writer) which looks nearly identical to the default merge behavior on Snowflake and BigQuery. If a `unique_key` is specified (recommended), dbt will update old records with values from new records that match on the key column. If a `unique_key` is not specified, dbt will forgo match criteria and simply insert all new records (similar to `append` strategy).

#### Profile config example
```yaml
test_project:
  target: dev
  outputs:
    dev:
      type: glue
      query-comment: my comment
      role_arn: arn:aws:iam::1234567890:role/GlueInteractiveSessionRole
      region: eu-west-1
      glue_version: "4.0"
      workers: 2
      worker_type: G.1X
      schema: "dbt_test_project"
      session_provisioning_timeout_in_seconds: 120
      location: "s3://aws-dbt-glue-datalake-1234567890-eu-west-1/"
      conf: spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.hive.convertMetastoreParquet=false
      datalake_formats: hudi
```

#### Source Code example
```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='user_id',
    file_format='hudi',
    hudi_options={
        'hoodie.datasource.write.precombine.field': 'eventtime',
    }
) }}

with new_events as (

    select * from {{ ref('events') }}

    {% if is_incremental() %}
    where date_day >= date_add(current_date, -1)
    {% endif %}

)

select
    user_id,
    max(date_day) as last_seen

from events
group by 1
```

#### Delta

You can also use Delta Lake to be able to use merge feature on tables.

**Usage notes:** The `merge` with Delta incremental strategy requires:
- To add `file_format: delta` in your table configuration
- To add a datalake_formats in your profile : `datalake_formats: delta`
  - Alternatively, to add a connections in your profile : `connections: name_of_your_delta_connector`
- To add the following config in your Interactive Session Config (in your profile):  `conf: "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog`

**Athena:** Athena is not compatible by default with delta tables, but you can configure the adapter to create Athena tables on top of your delta table. To do so, you need to configure the two following options in your profile:
- `delta_athena_prefix: "the_prefix_of_your_choice"`
- If your table is partitioned, then the add of new partition is not automatic, you need to perform an `MSCK REPAIR TABLE your_delta_table` after each new partition adding

#### Profile config example
```yaml
test_project:
  target: dev
  outputs:
    dev:
      type: glue
      query-comment: my comment
      role_arn: arn:aws:iam::1234567890:role/GlueInteractiveSessionRole
      region: eu-west-1
      glue_version: "4.0"
      workers: 2
      worker_type: G.1X
      schema: "dbt_test_project"
      session_provisioning_timeout_in_seconds: 120
      location: "s3://aws-dbt-glue-datalake-1234567890-eu-west-1/"
      datalake_formats: delta
      conf: "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
      delta_athena_prefix: "delta"
```

#### Source Code example
```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='user_id',
    partition_by=['dt'],
    file_format='delta'
) }}

with new_events as (

    select * from {{ ref('events') }}

    {% if is_incremental() %}
    where date_day >= date_add(current_date, -1)
    {% endif %}

)

select
    user_id,
    max(date_day) as last_seen,
    current_date() as dt

from events
group by 1
```

#### Iceberg

**Usage notes:** The `merge` with Iceberg incremental strategy requires:
- To add `file_format: Iceberg` in your table configuration
- To add a datalake_formats in your profile : `datalake_formats: iceberg`
  - Alternatively, if you use Glue 3.0, to add a connections in your profile : `connections: name_of_your_iceberg_connector` (
    - For Athena version 3: 
      - The adapter is compatible with the Iceberg Connector from AWS Marketplace with Glue 3.0 as Fulfillment option and 0.14.0 (Oct 11, 2022) as Software version)
      - the latest connector for iceberg in AWS marketplace uses Ver 0.14.0 for Glue 3.0, and Ver 1.2.1 for Glue 4.0 where Kryo serialization fails when writing iceberg, use "org.apache.spark.serializer.JavaSerializer" for spark.serializer instead, more info [here](https://github.com/apache/iceberg/pull/546) 
    - For Athena version 2: The adapter is compatible with the Iceberg Connector from AWS Marketplace with Glue 3.0 as Fulfillment option and 0.12.0-2 (Feb 14, 2022) as Software version)
- For Glue 4.0, to add the following configurations in dbt-profile:  
```
    --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog
    --conf spark.sql.catalog.glue_catalog.warehouse=s3://<PATH_TO_YOUR_WAREHOUSE>
    --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog 
    --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO 
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions  
```
- For Glue 3.0, you need to set up more configurations : 
```
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer
    --conf spark.sql.warehouse=s3://<your-bucket-name>
    --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog 
    --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog 
    --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO 
    --conf spark.sql.catalog.glue_catalog.lock-impl=org.apache.iceberg.aws.glue.DynamoDbLockManager
    --conf spark.sql.catalog.glue_catalog.lock.table=myGlueLockTable  
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
```

- Also note that for Glue 4.0, you can choose between Glue Optimistic Locking (enabled by default) and DynamoDB Lock Manager for concurrent update to a table.
    - If you want to activate DynamoDB Lock Manager set the below config in your profiles. A DynamoDB would be created on your behalf (if it does not exist). 
```
    --conf spark.sql.catalog.glue_catalog.lock-impl=org.apache.iceberg.aws.dynamodb.DynamoDbLockManager
    --conf spark.sql.catalog.glue_catalog.lock.table=<DYNAMODB_TABLE_NAME>
```
    You'll also need to grant the dbt-glue execution role with the appropriate permissions on DynamoDB
```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "CommitLockTable",
            "Effect": "Allow",
            "Action": [
                "dynamodb:CreateTable",
                "dynamodb:BatchGetItem",
                "dynamodb:BatchWriteItem",
                "dynamodb:ConditionCheckItem",
                "dynamodb:PutItem",
                "dynamodb:DescribeTable",
                "dynamodb:DeleteItem",
                "dynamodb:GetItem",
                "dynamodb:Scan",
                "dynamodb:Query",
                "dynamodb:UpdateItem"
            ],
            "Resource": "arn:aws:dynamodb:<AWS_REGION>:<AWS_ACCOUNT_ID>:table/<DYNAMODB_TABLE_NAME>"
        }
    ]
}
```
- Note that if you use Glue 3.0 DynamoDB Lock Manager is the only option available and you need to set `org.apache.iceberg.aws.glue.DynamoLockManager` instead : 
```
    --conf spark.sql.catalog.glue_catalog.lock-impl=org.apache.iceberg.aws.glue.DynamoDbLockManager
    --conf spark.sql.catalog.glue_catalog.lock.table=myGlueLockTable  
```

dbt will run an [atomic `merge` statement](https://iceberg.apache.org/docs/latest/spark-writes/) which looks nearly identical to the default merge behavior on Snowflake and BigQuery. You need to provide a `unique_key` to perform merge operation otherwise it will fail. This key is to provide in a Python list format and can contains multiple column name to create a composite unique_key. 

##### Notes
- When using a custom_location in Iceberg, avoid to use final trailing slash. Adding a final trailing slash lead to an un-proper handling of the location, and issues when reading the data from query engines like Trino. The issue should be fixed for Iceberg version > 0.13. Related Github issue can be find [here](https://github.com/apache/iceberg/issues/4582).
- Iceberg also supports `insert_overwrite` and `append` strategies. 
- The `warehouse` conf must be provided, but it's overwritten by the adapter `location` in your profile or `custom_location` in model configuration.
- By default, this materialization has `iceberg_expire_snapshots` set to 'True', if you need to have historical auditable changes, set: `iceberg_expire_snapshots='False'`.
- Currently, due to some dbt internal, the iceberg catalog used internally when running glue interactive sessions with dbt-glue has a hardcoded name `glue_catalog`. This name is an alias pointing to the AWS Glue Catalog but is specific to each session. If you want to interact with your data in another session without using dbt-glue (from a Glue Studio notebook, for example), you can configure another alias (ie. another name for the Iceberg Catalog). To illustrate this concept, you can set in your configuration file : 
```
--conf spark.sql.catalog.RandomCatalogName=org.apache.iceberg.spark.SparkCatalog
```
And then run in an AWS Glue Studio Notebook a session with the following config: 
```
--conf spark.sql.catalog.AnotherRandomCatalogName=org.apache.iceberg.spark.SparkCatalog
```
In both cases, the underlying catalog would be the AWS Glue Catalog, unique in your AWS Account and Region, and you would be able to work with the exact same data. Also make sure that if you change the name of the Glue Catalog Alias, you change it in all the other `--conf` where it's used: 
```
 --conf spark.sql.catalog.RandomCatalogName=org.apache.iceberg.spark.SparkCatalog 
 --conf spark.sql.catalog.RandomCatalogName.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog 
 ...
 --conf spark.sql.catalog.RandomCatalogName.lock-impl=org.apache.iceberg.aws.glue.DynamoLockManager
```
- A full reference to `table_properties` can be found [here](https://iceberg.apache.org/docs/latest/configuration/).
- Iceberg Tables are natively supported by Athena. Therefore, you can query tables created and operated with dbt-glue adapter from Athena.
- Incremental Materialization with Iceberg file format supports dbt snapshot. You are able to run a dbt snapshot command that queries an Iceberg Table and create a dbt fashioned snapshot of it. 

#### Profile config example
```yaml
test_project:
  target: dev
  outputs:
    dev:
      type: glue
      query-comment: my comment
      role_arn: arn:aws:iam::1234567890:role/GlueInteractiveSessionRole
      region: eu-west-1
      glue_version: "4.0"
      workers: 2
      worker_type: G.1X
      schema: "dbt_test_project"
      session_provisioning_timeout_in_seconds: 120
      location: "s3://aws-dbt-glue-datalake-1234567890-eu-west-1/"
      datalake_formats: iceberg
      conf: --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.warehouse=s3://aws-dbt-glue-datalake-1234567890-eu-west-1/dbt_test_project --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.catalog.glue_catalog.lock-impl=org.apache.iceberg.aws.dynamodb.DynamoDbLockManager --conf spark.sql.catalog.glue_catalog.lock.table=myGlueLockTable  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions 
```

#### Source Code example
```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['user_id'],
    file_format='iceberg',
    iceberg_expire_snapshots='False', 
    partition_by=['status']
    table_properties={'write.target-file-size-bytes': '268435456'}
) }}

with new_events as (

    select * from {{ ref('events') }}

    {% if is_incremental() %}
    where date_day >= date_add(current_date, -1)
    {% endif %}

)

select
    user_id,
    max(date_day) as last_seen

from events
group by 1
```
#### Iceberg Snapshot source code example
```sql

{% snapshot demosnapshot %}

{{
    config(
        strategy='timestamp',
        target_schema='jaffle_db',
        updated_at='dt',
        file_format='iceberg'
) }}

select * from {{ ref('customers') }}

{% endsnapshot %}

```

## Monitoring your Glue Interactive Session

Monitoring is an important part of maintaining the reliability, availability,
and performance of AWS Glue and your other AWS solutions. AWS provides monitoring
tools that you can use to watch AWS Glue, identify the required number of workers
required for your Glue Interactive Session, report when something is wrong and
take action automatically when appropriate. AWS Glue provides Spark UI,
and CloudWatch logs and metrics for monitoring your AWS Glue jobs.
More information on: [Monitoring AWS Glue Spark jobs](https://docs.aws.amazon.com/glue/latest/dg/monitor-spark.html)

**Usage notes:** Monitoring requires:
- To add the following IAM policy to your IAM role:
```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "CloudwatchMetrics",
            "Effect": "Allow",
            "Action": "cloudwatch:PutMetricData",
            "Resource": "*",
            "Condition": {
                "StringEquals": {
                    "cloudwatch:namespace": "Glue"
                }
            }
        },
        {
            "Sid": "CloudwatchLogs",
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "logs:CreateLogStream",
                "logs:CreateLogGroup",
                "logs:PutLogEvents"
            ],
            "Resource": [
                "arn:aws:logs:*:*:/aws-glue/*",
                "arn:aws:s3:::bucket-to-write-sparkui-logs/*"
            ]
        }
    ]
}
```

- To add monitoring parameters in your Interactive Session Config (in your profile).
More information on [Job parameters used by AWS Glue](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-glue-arguments.html)

#### Profile config example
```yaml
test_project:
  target: dev
  outputs:
    dev:
      type: glue
      query-comment: my comment
      role_arn: arn:aws:iam::1234567890:role/GlueInteractiveSessionRole
      region: eu-west-1
      glue_version: "4.0"
      workers: 2
      worker_type: G.1X
      schema: "dbt_test_project"
      session_provisioning_timeout_in_seconds: 120
      location: "s3://aws-dbt-glue-datalake-1234567890-eu-west-1/"
      default_arguments: "--enable-metrics=true, --enable-continuous-cloudwatch-log=true, --enable-continuous-log-filter=true, --enable-spark-ui=true, --spark-event-logs-path=s3://bucket-to-write-sparkui-logs/dbt/"
```

If you want to use the Spark UI, you can launch the Spark history server using a
AWS CloudFormation template that hosts the server on an EC2 instance,
or launch locally using Docker. More information on [Launching the Spark history server](https://docs.aws.amazon.com/glue/latest/dg/monitor-spark-ui-history.html#monitor-spark-ui-history-local)

## Enabling AWS Glue Auto Scaling
Auto Scaling is available since AWS Glue version 3.0 or later. More information
on the following AWS blog post: ["Introducing AWS Glue Auto Scaling: Automatically resize serverless computing resources for lower cost with optimized Apache Spark"](https://aws.amazon.com/blogs/big-data/introducing-aws-glue-auto-scaling-automatically-resize-serverless-computing-resources-for-lower-cost-with-optimized-apache-spark/)

With Auto Scaling enabled, you will get the following benefits:

* AWS Glue automatically adds and removes workers from the cluster depending on the parallelism at each stage or microbatch of the job run.

* It removes the need for you to experiment and decide on the number of workers to assign for your AWS Glue Interactive sessions.

* Once you choose the maximum number of workers, AWS Glue will choose the right size resources for the workload.

* You can see how the size of the cluster changes during the Glue Interactive sessions run by looking at CloudWatch metrics.
More information on [Monitoring your Glue Interactive Session](#Monitoring-your-Glue-Interactive-Session).

**Usage notes:** AWS Glue Auto Scaling requires:
- To set your AWS Glue version 3.0 or later.
- To set the maximum number of workers (if Auto Scaling is enabled, the `workers`
parameter sets the maximum number of workers)
- To set the `--enable-auto-scaling=true` parameter on your Glue Interactive Session Config (in your profile).
More information on [Job parameters used by AWS Glue](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-glue-arguments.html)

#### Profile config example
```yaml
test_project:
  target: dev
  outputs:
    dev:
      type: glue
      query-comment: my comment
      role_arn: arn:aws:iam::1234567890:role/GlueInteractiveSessionRole
      region: eu-west-1
      glue_version: "3.0"
      workers: 2
      worker_type: G.1X
      schema: "dbt_test_project"
      session_provisioning_timeout_in_seconds: 120
      location: "s3://aws-dbt-glue-datalake-1234567890-eu-west-1/"
      default_arguments: "--enable-auto-scaling=true"
```

## Access Glue catalog in another AWS account
In many cases, you may need to run you dbt jobs to read from another AWS account.

Review the following link https://repost.aws/knowledge-center/glue-tables-cross-accounts to set up access policies in source and target accounts

Add the following "spark.hadoop.hive.metastore.glue.catalogid=<AWS-ACCOUNT-ID>" to your conf in the DBT profile, as such, you can have multiple outputs for each of the accounts that you have access to.

Note: The access cross-accounts need to be within the same AWS Region
#### Profile config example
```yaml
test_project:
  target: dev
  outputsAccountB:
    dev:
      type: glue
      query-comment: my comment
      role_arn: arn:aws:iam::1234567890:role/GlueInteractiveSessionRole
      region: eu-west-1
      glue_version: "3.0"
      workers: 2
      worker_type: G.1X
      schema: "dbt_test_project"
      session_provisioning_timeout_in_seconds: 120
      location: "s3://aws-dbt-glue-datalake-1234567890-eu-west-1/"
      conf: "--conf hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory 
             --conf spark.hadoop.hive.metastore.glue.catalogid=<TARGET-AWS-ACCOUNT-ID-B>"
```

## Persisting model descriptions

Relation-level docs persistence is supported since dbt v0.17.0. For more
information on configuring docs persistence, see [the docs](resource-configs/persist_docs).

When the `persist_docs` option is configured appropriately, you'll be able to
see model descriptions in the `Comment` field of `describe [table] extended`
or `show table extended in [database] like '*'`.

## Always `schema`, never `database`

Apache Spark uses the terms "schema" and "database" interchangeably. dbt understands
`database` to exist at a higher level than `schema`. As such, you should _never_
use or set `database` as a node config or in the target profile when running dbt-glue.

If you want to control the schema/database in which dbt will materialize models,
use the `schema` config and `generate_schema_name` macro _only_.
For more information, check the dbt documentation about [custom schemas](https://docs.getdbt.com/docs/build/custom-schemas).

## AWS Lakeformation integration
The adapter supports AWS Lake Formation tags management enabling you to associate existing tags defined out of dbt-glue to database objects built by dbt-glue (database, table, view, snapshot, incremental models, seeds).

- You can enable or disable lf-tags management via config, at model and dbt-project level (disabled by default)
- If enabled, lf-tags will be updated on every dbt run. There are table level lf-tags configs and column-level lf-tags configs. 
- You can specify that you want to drop existing database, table column Lake Formation tags by setting the drop_existing config field to True (False by default, meaning existing tags are kept)
- Please note that if the tag you want to associate with the table does not exist, the dbt-glue execution will throw an error

The adapter also supports AWS Lakeformation data cell filtering. 
- You can enable or disable data-cell filtering via config, at model and dbt-project level (disabled by default)
- If enabled, data_cell_filters will be updated on every dbt run.
- You can specify that you want to drop existing table data-cell filters by setting the drop_existing config field to True (False by default, meaning existing filters are kept)
- You can leverage excluded_columns_names **OR** columns config fields to perform Column level security as well. **Please note that you can use one or the other but not both**.
- By default, if you don't specify any column or excluded_columns, dbt-glue does not perform Column level filtering and let the principal access all the columns.

The below configuration let the specified principal (lf-data-scientist IAM user) access rows that have a customer_lifetime_value > 15 and all the columns specified ('customer_id', 'first_order', 'most_recent_order', 'number_of_orders')

```sql
lf_grants={
        'data_cell_filters': {
            'enabled': True,
            'drop_existing' : True,
            'filters': {
                'the_name_of_my_filter': {
                    'row_filter': 'customer_lifetime_value>15',
                    'principals': ['arn:aws:iam::123456789:user/lf-data-scientist'], 
                    'column_names': ['customer_id', 'first_order', 'most_recent_order', 'number_of_orders']
                }
            }, 
        }
    }
```
The below configuration let the specified principal (lf-data-scientist IAM user) access rows that have a customer_lifetime_value > 15 and all the columns *except* the one specified ('first_name')

```sql
lf_grants={
        'data_cell_filters': {
            'enabled': True,
            'drop_existing' : True,
            'filters': {
                'the_name_of_my_filter': {
                    'row_filter': 'customer_lifetime_value>15',
                    'principals': ['arn:aws:iam::123456789:user/lf-data-scientist'], 
                    'excluded_column_names': ['first_name']
                }
            }, 
        }
    }
```

See below some examples of how you can integrate LF Tags management and data cell filtering to your configurations : 

#### At model level
This way of defining your Lakeformation rules is appropriate if you want to handle the tagging and filtering policy at object level. Remember that it overrides any configuration defined at dbt-project level. 

```sql
{{ config(
    materialized='incremental',
    unique_key="customer_id",
    incremental_strategy='append',
    lf_tags_config={
          'enabled': true,
          'drop_existing' : False,
          'tags_database': 
          {
            'name_of_my_db_tag': 'value_of_my_db_tag'          
            }, 
          'tags_table': 
          {
            'name_of_my_table_tag': 'value_of_my_table_tag'          
            }, 
          'tags_columns': {
            'name_of_my_lf_tag': {
              'value_of_my_tag': ['customer_id', 'customer_lifetime_value', 'dt']
            }}},
    lf_grants={
        'data_cell_filters': {
            'enabled': True,
            'drop_existing' : True,
            'filters': {
                'the_name_of_my_filter': {
                    'row_filter': 'customer_lifetime_value>15',
                    'principals': ['arn:aws:iam::123456789:user/lf-data-scientist'], 
                    'excluded_column_names': ['first_name']
                }
            }, 
        }
    }
) }}

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order,
        customer_orders.most_recent_order,
        customer_orders.number_of_orders,
        customer_payments.total_amount as customer_lifetime_value,
        current_date() as dt
        
    from customers

    left join customer_orders using (customer_id)

    left join customer_payments using (customer_id)

```

#### At dbt-project level
This way you can specify tags and data filtering policy for a particular path in your dbt project (eg. models, seeds, models/model_group1, etc.)
This is especially useful for seeds, for which you can't define configuration in the file directly.

```yml
seeds:
  +lf_tags_config:
    enabled: true
    tags_table: 
      name_of_my_table_tag: 'value_of_my_table_tag'  
    tags_database: 
      name_of_my_database_tag: 'value_of_my_database_tag'
models:
  +lf_tags_config:
    enabled: true
    drop_existing: True
    tags_database: 
      name_of_my_database_tag: 'value_of_my_database_tag'
    tags_table: 
      name_of_my_table_tag: 'value_of_my_table_tag'
```

## Tests

To perform a functional test:
1. Install dev requirements:
```bash
$ pip3 install -r dev-requirements.txt
```

2. Install dev locally
```bash
$ python3 setup.py build && python3 setup.py install_lib
```

3. Export variables
```bash
$ export DBT_AWS_ACCOUNT=123456789101
$ export DBT_GLUE_REGION=us-east-1
$ export DBT_S3_LOCATION=s3://mybucket/myprefix
$ export DBT_GLUE_ROLE_ARN=arn:aws:iam::1234567890:role/GlueInteractiveSessionRole
```
Caution: Be careful not to set S3 path containing important files. 
dbt-glue's test suite automatically deletes all the existing files under the S3 path specified in `DBT_S3_LOCATION`.

4. Run the test
```bash
$ python3 -m pytest tests/functional
```
or
```bash
$ python3 -m pytest -s 
```
For more information, check the dbt documentation about [testing a new adapter](https://docs.getdbt.com/docs/contributing/testing-a-new-adapter).

## Caveats

### Supported Functionality

Most dbt Core functionality is supported, but some features are only available with Apache Hudi.

Apache Hudi-only features:
1. Incremental model updates by `unique_key` instead of `partition_by` (see [`merge` strategy](glue-configs#the-merge-strategy))


Some dbt features, available on the core adapters, are not yet supported on Glue:
1. [Persisting](persist_docs) column-level descriptions as database comments
2. [Snapshots](snapshots)

---
For more information on dbt:
- Read the [introduction to dbt](https://docs.getdbt.com/docs/introduction).
- Read the [dbt viewpoint](https://docs.getdbt.com/docs/about/viewpoint).
- Join the [dbt community](http://community.getdbt.com/).
---

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.
