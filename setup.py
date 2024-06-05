#!/usr/bin/env python
import os
import sys

# require python 3.7 or newer
if sys.version_info < (3, 8):
    print('Error: dbt does not support this version of Python.')
    print('Please upgrade to Python 3.8 or higher.')
    sys.exit(1)

# require version of setuptools that supports find_namespace_packages
from setuptools import setup

try:
    from setuptools import find_namespace_packages
except ImportError:
    # the user has a downlevel version of setuptools.
    print('Error: dbt requires setuptools v40.1.0 or higher.')
    print('Please upgrade setuptools with "pip install --upgrade setuptools" '
          'and try again')
    sys.exit(1)


def read(rel_path):
    this_directory = os.path.abspath(os.path.dirname(__file__))
    with open(os.path.join(this_directory, rel_path), 'r') as f:
        return f.read()


def get_version(rel_path):
    for line in read(rel_path).splitlines():
        if line.startswith('version'):
            delim = '"' if '"' in line else "'"
            return line.split(delim)[1]
    else:
        raise RuntimeError("Unable to find version string.")


package_name = "dbt-glue"
package_version = get_version("dbt/adapters/glue/__version__.py")
dbt_version = "1.8.1"
dbt_spark_version = "1.8.0"
description = """dbt (data build tool) adapter for Aws Glue"""
long_description = read('README.md')
setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=long_description,
    long_description_content_type='text/markdown',
    author="moshirm,menuetb,mamallem,segnina",
    author_email="moshirm@amazon.fr, menuetb@amazon.fr, mamallem@amazon.fr, segnina@amazon.fr ",
    url='https://github.com/aws-samples/dbt-glue',
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    package_data={
        'dbt': [
            'include/glue/macros/*.sql',
            'include/glue/macros/*/*.sql',
            'include/glue/macros/*/*/*.sql',
            'include/glue/dbt_project.yml',
            'include/glue/sample_profiles.yml',
            'include/glue/tests/*/*.sql',
            'adapters/glue/*.py',
            'adapters/gluedbapi/*.py',
        ]
    },
    install_requires=[
        "dbt-core~={}".format(dbt_version),
        "dbt-spark~={}".format(dbt_spark_version),
        "dbt-common>=1.0.4,<2.0",
        "dbt-adapters>=1.1.1,<2.0",
        "waiter",
        "pyarrow",
        "boto3 >= 1.28.16"
    ],
    zip_safe=False,
    classifiers=[
        "Development Status :: 4 - Beta",

        'License :: OSI Approved :: Apache Software License',

        'Operating System :: Microsoft :: Windows',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX :: Linux',

        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],
    python_requires=">=3.8",
)
