<p align="center">
  <img src="/etc/dbt-logo-full.svg" alt="dbt logo" width="500"/>
</p>

**[dbt](https://www.getdbt.com/)** enables data analysts and engineers to transform their data using the same practices that software engineers use to build applications.
dbt is the T in ELT. Organize, cleanse, denormalize, filter, rename, and pre-aggregate the raw data in your warehouse so that it's ready for analysis.

# dbt-glue

The `dbt-glue` package implements the [dbt adapter](https://docs.getdbt.com/docs/contributing/building-a-new-adapter) protocol for AWS Glue's Spark engine. 
It supports running dbt against Spark, through the new Glue Interactive Sessions API.



## Installation

The package can be installed from PyPI with:

```bash
$ pip install dbt-glue
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
|region|The region where you're Glue database is stored |
|AWS Account|The AWS account where you run your pipeline|
|dbt output database|The database updated by dbt (this is the database configured in the profile.yml of your dbt environment)|
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
                "lakeformation:ListPermissions"
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
database: "dbt_demo"
session_provisioning_timeout_in_seconds: 120
location: "s3://dbt_demo_bucket/dbt_demo_data"
```

The table below describes all the options.

|Option	| Description	                                                                                                                           | Mandatory |
|---|----------------------------------------------------------------------------------------------------------------------------------------|---|
|project_name	| The dbt project name. This must be the same as the one configured in the dbt project.	                                                 |yes|
|type	| The driver to use.	                                                                                                                    |yes|
|query-comment	| A string to inject as a comment in each query that dbt runs. 	                                                                         |no|
|role_arn	| The ARN of the glue interactive session IAM role.	                                               |yes|
|region	| The AWS Region were you run the data pipeline.	                                                                                        |yes|
|workers	| The number of workers of a defined workerType that are allocated when a job runs.	                                                     |yes|
|worker_type	| The type of predefined worker that is allocated when a job runs. Accepts a value of Standard, G.1X, or G.2X.	                          |yes|
|schema	| The schema used to organize data stored in Amazon S3.	                                                                                 |yes|
|database	| The database in Lake Formation. The database stores metadata tables in the Data Catalog.	                                              |yes|
|session_provisioning_timeout_in_seconds | The timeout in seconds for AWS Glue interactive session provisioning.	                                                                |yes|
|location	| The Amazon S3 location of your target data.	                                                                                           |yes|
|query_timeout_in_seconds	| The timeout in seconds for a signle query. Default is 300                                                                              |no|
|idle_timeout	| The AWS Glue session idle timeout in minutes. (The session stops after being idle for the specified amount of time)	                  |no|
|glue_version	| The version of AWS Glue for this session to use. Currently, the only valid options are 2.0 and 3.0. The default value is 3.0.	         |no|
|security_configuration	| The security configuration to use with this session.	                                                                                  |no|
|connections	| A comma-separated list of connections to use in the session.	                                                                          |no|
|conf	| Specific configuration used at the startup of the Glue Interactive Session (arg --conf)	                                               |no|
|extra_py_files	| Extra python Libs that can be used by the interactive session.                                                                         |no|
|delta_athena_prefix	| A prefix used to create Athena compatible tables for Delta tables	(if not specified, then no Athena compatible table will be created)  |no|
|tags	| The map of key value pairs (tags) belonging to the session. Ex: KeyName1=Value1,KeyName2=Value2  |no|

## Configs

### Configuring tables

When materializing a model as `table`, you may include several optional configs that are specific to the dbt-spark plugin, in addition to the standard [model configs](model-configs).

| Option  | Description                                        | Required?               | Example                  |
|---------|----------------------------------------------------|-------------------------|--------------------------|
| file_format | The file format to use when creating tables (`parquet`, `csv`, `json`, `text`, `jdbc` or `orc`). | Optional | `parquet`|
| partition_by  | Partition the created table by the specified columns. A directory is created for each partition. | Optional                | `date_day`              |
| clustered_by  | Each partition in the created table will be split into a fixed number of buckets by the specified columns. | Optional               | `country_code`              |
| buckets  | The number of buckets to create while clustering | Required if `clustered_by` is specified                | `8`              |
| custom_location  | By default, the adapter will store your data in the following path: `location path`/`database`/`table`. If you don't want to follow that default behaviour, you can use this parameter to set your own custom location on S3 | No | `s3://mycustombucket/mycustompath`              |

## Incremental models

dbt seeks to offer useful and intuitive modeling abstractions by means of its built-in configurations and materializations.

For that reason, the dbt-glue plugin leans heavily on the [`incremental_strategy` config](configuring-incremental-models#about-incremental_strategy). This config tells the incremental materialization how to build models in runs beyond their first. It can be set to one of three values:
 - **`append`** (default): Insert new records without updating or overwriting any existing data.
 - **`insert_overwrite`**: If `partition_by` is specified, overwrite partitions in the table with new data. If no `partition_by` is specified, overwrite the entire table with new data.
 - **`merge`** (Apache Hudi only): Match records based on a `unique_key`; update old records, insert new ones. (If no `unique_key` is specified, all new data is inserted, similar to `append`.)
 
Each of these strategies has its pros and cons, which we'll discuss below. As with any model config, `incremental_strategy` may be specified in `dbt_project.yml` or within a model file's `config()` block.

**Notes:**
The default strategie is **`insert_overwrite`**

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
- Iceberg : On going
- Lake Formation Governed Tables : On going

The simpliest way to work with theses advanced features is to install theses using [Glue connectors](https://docs.aws.amazon.com/glue/latest/ug/connectors-chapter.html).

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
- To add a connections in your profile : `connections: name_of_your_hudi_connector`
- To add Kryo serializer in your Interactive Session Config (in your profile):  `conf: "spark.serializer=org.apache.spark.serializer.KryoSerializer"`

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
      glue_version: "3.0"
      workers: 2
      worker_type: G.1X
      schema: "dbt_test_project"
      database: "dbt_test_project"
      session_provisionning_timeout_in_seconds: 120
      location: "s3://aws-dbt-glue-datalake-1234567890-eu-west-1/"
      connections: name_of_your_hudi_connector
      conf: "spark.serializer=org.apache.spark.serializer.KryoSerializer"
```

#### Source Code example
```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='user_id',
    file_format='hudi'
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
- To add a connections in your profile : `connections: name_of_your_delta_connector`
- To add the following config in your Interactive Session Config (in your profile):  `conf: "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog`

**Athena:** Athena is not compatible by default with delta tables, but you can configure the adapter to create Athena tables on top of your delta table. To do so, you need to configure the two following options in your profile:
- `extra_py_files: "/tmp/delta-core_2.12-1.0.0.jar"`
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
      glue_version: "3.0"
      workers: 2
      worker_type: G.1X
      schema: "dbt_test_project"
      database: "dbt_test_project"
      session_provisionning_timeout_in_seconds: 120
      location: "s3://aws-dbt-glue-datalake-1234567890-eu-west-1/"
      connections: name_of_your_delta_connector
      conf: "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
      extra_py_files: "/tmp/delta-core_2.12-1.0.0.jar"
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
$ export DBT_S3_LOCATION=s3://mybucket/myprefix
$ export DBT_ROLE_ARN=arn:aws:iam::1234567890:role/GlueInteractiveSessionRole
```

4. Run the test
```bash
$ python3 -m pytest tests/functional
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
