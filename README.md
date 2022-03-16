<p align="center">
  <img src="/etc/dbt-logo-full.svg" alt="dbt logo" width="500"/>
</p>

**[dbt](https://www.getdbt.com/)** enables data analysts and engineers to transform their data using the same practices that software engineers use to build applications.
dbt is the T in ELT. Organize, cleanse, denormalize, filter, rename, and pre-aggregate the raw data in your warehouse so that it's ready for analysis.

# dbt-glue

The `dbt-glue` package implements the [dbt adapter](https://docs.getdbt.com/docs/contributing/building-a-new-adapter) protocol for AWS Glue's Spark engine. 
It supports running dbt against Spark, through the new Glue Interactive Sessions API.



## Installation

### Configuring your AWS profile for Glue Interactive Session
There are two IAM principals used with interactive sessions.
- Client principal: The princpal (either user or role) calling the AWS APIs (Glue, Lake Formation, Interactive Sessions)
from the local client. This is the principal configured in the AWS CLI and likely the same.
- Service role: The IAM role that AWS Glue uses to execute your session. This is the same as AWS Glue
ETL.

Read [this documentation](https://docs.aws.amazon.com/glue/latest/dg/glue-is-security.html) to configure these principals.

To enjoy all features of the dbt/Glue driver, you will need to attach to the Service role the 3 AWS managed policies below:

| Service  | managed policy required  |
|---|---|
| Amazon S3 | AmazonS3FullAccess |
| AWS Glue | AWSGlueConsoleFullAccess |
| AWS Lake formation | AWSLakeFormationDataAdmin |

### Configuration of the local environment

dbt and aws-glue-dbt-adapter are compatible with Python versions 3.7, 3.8, and 3.9

Configure a Python virtual environment to isolate package version and code dependencies:

```bash
sudo yum install git
python3 -m pip install --upgrade pip
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
```

Configure the last version of AWS CLI

```bash
$ curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
$ unzip awscliv2.zip
$ sudo ./aws/install
```

Configure the aws-glue-session package

```bash
$ sudo yum install gcc krb5-devel.x86_64 python3-devel.x86_64 -y
$ pip3 install —upgrade boto3
$ pip3 install —upgrade aws-glue-sessions
$ export is_package_dir=$(pip3 show aws-glue-sessions | grep Location | awk '{print $2}')
$ aws configure add-model --service-model file://$is_package_dir/aws_glue_interactive_sessions_kernel/service-2.json —service-name glue
```


Getting started with dbt

Install dbt and dbt CLI a command-line interface for running dbt projects. The dbt CLI is free to use and available as an open source project (https://github.com/dbt-labs/dbt-core).

```bash
$ pip3 install dbt
```

More information about dbt is available in the documentation below:

- [Install dbt](https://docs.getdbt.com/docs/installation)
- Read the [introduction](https://docs.getdbt.com/docs/introduction/) and [viewpoint](https://docs.getdbt.com/docs/about/viewpoint/)

### dbt Glue
Install the package
```bash
$ python3 -m pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ aws-glue-dbt-adapter
```

### Example config
Add the config as bellow in your profiles file: 
```bash
~/.dbt/profiles.yml
```

```yaml
<project_name>:
  target: dev
  outputs:
    dev:
        type: glue
        query-comment: null
        role_arn: <Role ARN>
        region: <AWSREGION>
        workers: <Number of Worker>
        worker_type: <Worker Type>
        idle_timeout: <Minutes>
        schema: <Schema>
        database: <Database>
        session_provisionning_timeout_in_seconds: <Timeout>
        location: <S3 Location>
```
The table below describes all the options.

|Option	|Description	|
|---|---|
|project_name |	The dbt project name, it has to be the same as the one configured in the dbt project	|
|type	|The driver to use.	|
|query-comment	|A string to inject as a comment in each query that dbt runs. 	|
|role_arn	|The ARN of the Interactive Session role created in prerequisite.	|
|region	|The AWS region were to run the data pipeline	|
|workers	|The number of workers of a defined workerType that are allocated when a job runs.	|
|worker_type	|The type of predefined worker that is allocated when a job runs. Accepts a value of Standard, G.1X, or G.2X.	|
|idle_timeout	|Glue Session Idle timeout in minutes (Session will terminate after being idle for specified minutes)	|
|schema	|The schema is used to organize data stored in S3.	|
|database	|The database in AWS Lake Formation. A databases store metadata tables in the data catalog.	|
|session_provisionning_timeout_in_seconds	|The timeout in seconds for the Glue Interactive Session provisionning.	|
|location	|The S3 location of your target data.|	

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
