<p align="center">
  <img src="/etc/dbt-logo-full.svg" alt="dbt logo" width="500"/>
</p>

**[dbt](https://www.getdbt.com/)** enables data analysts and engineers to transform their data using the same practices that software engineers use to build applications.
dbt is the T in ELT. Organize, cleanse, denormalize, filter, rename, and pre-aggregate the raw data in your warehouse so that it's ready for analysis.

# dbt-glue

This plugin ports [dbt](https://getdbt.com) functionality to AWS Glue. 
It supports running dbt against Glue interactive sessions that are hosted via AWS.

The `dbt-glue` package contains all of the code enabling dbt to work with AWS Glue. 
It supports running dbt against Glue interactive sessions that are hosted via AWS.


## Installation

### Configuring your AWS profile for Glue Interactive Session
There are two IAM principals used with interactive sessions.
- Client principal: The princpal (either user or role) calling the AWS APIs (Glue, Lake Formation, Interactive Sessions)
from the local client. This is the principal configured in the AWS CLI and likely the same.
- Service role: The IAM role that AWS Glue uses to execute your session. This is the same as AWS Glue
ETL.

In order to let the Client principal interact with Glue Interactive Session, the following policy have to be attached to
the user or the role:
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowStatementInASessionToAUser",
            "Action": [
                "glue:ListSessions",
                "glue:GetSession",
                "glue:ListStatements",
                "glue:GetStatement",
                "glue:RunStatement",
                "glue:CancelStatement",
                "glue:DeleteSession"
            ],
            "Effect": "Allow",
            "Resource": [
                "arn:aws:glue:<Region>:<AccountID>:session/${aws:userid}*"
            ]
        },
        {
            "Action": [
                "glue:CreateSession"
            ],
            "Effect": "Allow",
            "Resource": [
                "*"
            ]
        },
        {
            "Action": [
                "iam:PassRole"
            ],
            "Effect": "Allow",
            "Resource": "arn:aws:iam::*:role/AWSGlueServiceRole*",
            "Condition": {
                "StringLike": {
                    "iam:PassedToService": [
                        "glue.amazonaws.com"
                    ]
                }
            }
        },
        {
            "Action": [
                "iam:PassRole"
            ],
            "Effect": "Allow",
            "Resource": [
                "arn:aws:iam::*:role/service-role/AWSGlueServiceRole*"
            ],
            "Condition": {
                "StringLike": {
                    "iam:PassedToService": [
                        "glue.amazonaws.com"
                    ]
                }
            }
        }
    ]
}
```

To enjoy all features of the dbt/Glue driver, you will need to attach to the Service role the 3 AWS managed policies below:

| Service  | managed policy required  |
|---|---|
| Amazon S3 | AmazonS3FullAccess |
| AWS Glue | AWSGlueConsoleFullAccess |
| AWS Lake formation | AWSLakeFormationDataAdmin |

Now install the Glue Interactive Session package
```bash
$ pip3 install aws-glue-sessions
```

### Getting started with dbt

- [Install dbt](https://docs.getdbt.com/docs/installation)
- Read the [introduction](https://docs.getdbt.com/docs/introduction/) and [viewpoint](https://docs.getdbt.com/docs/about/viewpoint/)

### dbt Glue
Build & install the package
```bash
$ pip3 install -U setuptools
$ python3 setup.py bdist_wheel
$ pip3 install ~/dist/dbt_glue-1.0.0-py3-none-any.whl
```

### Example config
Add the config as bellow in your profiles file: 
```bash
~/.dbt/profiles.yml
```

```yaml
<your_project_name>:
  target:
    type: glue
    query-comment: null
    role_arn: arn:aws:iam::<ACCOUNTID>:role/<GLUEROLENAME>
    region: <AWSREGION>
    workers: 5
    worker_type: G.1X
    schema: "dbt_test_{{ var('_dbt_random_suffix') }}"
    database: dbt
    session_provisionning_timeout_in_seconds: 40
    location: "s3://<BUCKET>/dbt_test_{{ var('_dbt_random_suffix') }}/"
```

## Testing 
Install test tools
```bash
$ pip install -r dev-requirements.txt
```
Run the test
```bash
$ pytest -s  test/integration/awsglue.dbtspec
```
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