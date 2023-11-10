import os
import boto3
from urllib.parse import urlparse
from dbt.config.project import PartialProject


DEFAULT_REGION = "eu-west-1"


class Obj:
    which = "blah"
    single_threaded = False


def profile_from_dict(profile, profile_name, cli_vars="{}"):
    from dbt.config import Profile
    from dbt.config.renderer import ProfileRenderer
    from dbt.config.utils import parse_cli_vars

    if not isinstance(cli_vars, dict):
        cli_vars = parse_cli_vars(cli_vars)

    renderer = ProfileRenderer(cli_vars)

    # in order to call dbt's internal profile rendering, we need to set the
    # flags global. This is a bit of a hack, but it's the best way to do it.
    from dbt.flags import set_from_args
    from argparse import Namespace

    set_from_args(Namespace(), None)
    return Profile.from_raw_profile_info(
        profile,
        profile_name,
        renderer,
    )


def project_from_dict(project, profile, packages=None, selectors=None, cli_vars="{}"):
    from dbt.config.renderer import DbtProjectYamlRenderer
    from dbt.config.utils import parse_cli_vars

    if not isinstance(cli_vars, dict):
        cli_vars = parse_cli_vars(cli_vars)

    renderer = DbtProjectYamlRenderer(profile, cli_vars)

    project_root = project.pop("project-root", os.getcwd())

    partial = PartialProject.from_dicts(
        project_root=project_root,
        project_dict=project,
        packages_dict=packages,
        selectors_dict=selectors,
    )
    return partial.render(renderer)


def config_from_parts_or_dicts(project, profile, packages=None, selectors=None, cli_vars="{}"):
    from dbt.config import Project, Profile, RuntimeConfig
    from dbt.config.utils import parse_cli_vars
    from copy import deepcopy

    if not isinstance(cli_vars, dict):
        cli_vars = parse_cli_vars(cli_vars)

    if isinstance(project, Project):
        profile_name = project.profile_name
    else:
        profile_name = project.get("profile")

    if not isinstance(profile, Profile):
        profile = profile_from_dict(
            deepcopy(profile),
            profile_name,
            cli_vars,
        )

    if not isinstance(project, Project):
        project = project_from_dict(
            deepcopy(project),
            profile,
            packages,
            selectors,
            cli_vars,
        )

    args = Obj()
    args.vars = cli_vars
    args.profile_dir = "/dev/null"
    return RuntimeConfig.from_parts(project=project, profile=profile, args=args)


def get_account_id():
    if "DBT_AWS_ACCOUNT" in os.environ:
        return os.environ.get("DBT_AWS_ACCOUNT")
    else:
        raise ValueError("DBT_AWS_ACCOUNT must be configured")


def get_region():
    r = os.environ.get("DBT_GLUE_REGION", DEFAULT_REGION)
    return r


def get_s3_location():
    if "DBT_S3_LOCATION" in os.environ:
        return os.environ.get("DBT_S3_LOCATION")
    else:
        raise ValueError("DBT_S3_LOCATION must be configured")


def get_role_arn():
    return os.environ.get("DBT_GLUE_ROLE_ARN", f"arn:aws:iam::{get_account_id()}:role/GlueInteractiveSessionRole")


def cleanup_s3_location(path, region):
    client = boto3.client("s3", region_name=region)
    S3Url(path).delete_all_keys_v2(client)


class S3Url(object):
    def __init__(self, url):
        self._parsed = urlparse(url, allow_fragments=False)

    @property
    def bucket(self):
        return self._parsed.netloc

    @property
    def key(self):
        if self._parsed.query:
            return self._parsed.path.lstrip("/") + "?" + self._parsed.query
        else:
            return self._parsed.path.lstrip("/")

    @property
    def url(self):
        return self._parsed.geturl()

    def delete_all_keys_v2(self, client):
        bucket = self.bucket
        prefix = self.key

        for response in client.get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' not in response:
                continue
            for content in response['Contents']:
                print("Deleting: s3://" + bucket + "/" + content['Key'])
                client.delete_object(Bucket=bucket, Key=content['Key'])
