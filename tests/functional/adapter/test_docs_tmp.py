import pytest
import os

from dbt.tests.adapter.basic.test_docs_generate import (
    BaseDocsGenerate,
    models__second_model_sql,
    models__readme_md,
    models__model_sql,
    run_and_generate,
    verify_metadata
)
from dbt.tests.util import get_artifact

schema_name = "dbt_functional_test_01"

models__schema_yml = """
version: 2

models:
  - name: model
    description: "The test model"
    docs:
      show: false
    columns:
      - name: id
        description: The user ID number
        tests:
          - unique
          - not_null
      - name: first_name
        description: The user's first name
      - name: email
        description: The user's email
      - name: ip_address
        description: The user's IP address
      - name: updated_at
        description: The last time this user's email was updated
    tests:
      - test.nothing

  - name: second_model
    description: "The second test model"
    docs:
      show: false
    columns:
      - name: id
        description: The user ID number
      - name: first_name
        description: The user's first name
      - name: email
        description: The user's email
      - name: ip_address
        description: The user's IP address
      - name: updated_at
        description: The last time this user's email was updated


sources:
  - name: my_source
    description: "My source"
    loader: a_loader
    database: "{{ var('test_schema') }}"
    schema: "{{ var('test_schema') }}"
    tables:
      - name: my_table
        description: "My table"
        identifier: seed
        columns:
          - name: id
            description: "An ID field"


exposures:
  - name: simple_exposure
    type: dashboard
    depends_on:
      - ref('model')
      - source('my_source', 'my_table')
    owner:
      email: something@example.com
  - name: notebook_exposure
    type: notebook
    depends_on:
      - ref('model')
      - ref('second_model')
    owner:
      email: something@example.com
      name: Some name
    description: >
      A description of the complex exposure
    maturity: medium
    meta:
      tool: 'my_tool'
      languages:
        - python
    tags: ['my_department']
    url: http://example.com/notebook/1
"""


def custom_verify_catalog_glue(project, expected_catalog, start_time):
    # get the catalog.json
    catalog_path = os.path.join(project.project_root, "target", "catalog.json")
    print(f"catalog_path: {catalog_path}")
    assert os.path.exists(catalog_path)
    catalog = get_artifact(catalog_path)

    # verify the catalog
    assert set(catalog) == {"errors", "nodes", "sources", "metadata"}
    verify_metadata(
        catalog["metadata"],
        "https://schemas.getdbt.com/dbt/catalog/v1.json",
        start_time,
    )
    assert not catalog["errors"]

    print(f"catalog: {catalog}")
    for key in "nodes", "sources":
        for unique_id, expected_node in expected_catalog[key].items():
            print(f"key: {key}, unique_id: {unique_id}, expected_node: {expected_node}")
            found_node = catalog[key][unique_id]
            for node_key in expected_node:
                assert node_key in found_node
                # the value of found_node[node_key] is not exactly expected_node[node_key]


class TestDocsGenerate(BaseDocsGenerate):
    @pytest.fixture(scope="class")
    def unique_schema(request, prefix) -> str:
        return schema_name

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": models__schema_yml,
            "second_model.sql": models__second_model_sql,
            "readme.md": models__readme_md,
            "model.sql": models__model_sql,
        }

    def test_run_and_generate_no_compile(self, project, expected_catalog):
        start_time = run_and_generate(project, ["--no-compile"])
        assert not os.path.exists(os.path.join(project.project_root, "target", "manifest.json"))
        custom_verify_catalog_glue(project, expected_catalog, start_time)

    def test_run_and_generate(self, project, expected_catalog):
        start_time = run_and_generate(project)
        custom_verify_catalog_glue(project, expected_catalog, start_time)

        # Check that assets have been copied to the target directory for use in the docs html page
        assert os.path.exists(os.path.join(".", "target", "assets"))
        assert os.path.exists(os.path.join(".", "target", "assets", "lorem-ipsum.txt"))
        assert not os.path.exists(os.path.join(".", "target", "non-existent-assets"))

    pass
