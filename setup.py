#!/usr/bin/env python
from setuptools import find_packages
from setuptools import find_namespace_packages
from setuptools import setup
package_name = "dbt-glue"
package_version = "1.0.0"
description = """Aws Glue adapter for dbt (data build tool)"""
setup(
    name=package_name,
    version=package_version,
    description="dbt adapter for AWS Glue",
    long_description="Easily use Glue Spark as a target runtime for dbt",
    author="moshir mikael,benjamin menuet, amine el mallem",
    author_email="moshirm@amazon.fr, menuetb@amazon.fr, mamallem@amazon.fr",
    url="https://aws.amazon.com/fr/glue/",
    packages=find_namespace_packages(include=['dbt', 'dbt.*']),
    #packages=find_packages(where="src"),
    package_data={
        'dbt': [
            'include/glue/macros/*.sql',
            'include/glue/macros/*/*.sql',
            'include/glue/macros/*/*/*.sql',
            'include/glue/dbt_project.yml',
            'include/glue/sample_profiles.yml',
            'adapters/glue/*.py',
            'adapters/gluedbapi/*.py',
        ]
    },
    install_requires=[
        "dbt-core",
        "waiter",
        "boto3"
    ]
)
