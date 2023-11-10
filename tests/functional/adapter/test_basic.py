import os

import pytest
from dbt.tests.adapter.basic.files import (base_ephemeral_sql, base_table_sql,
                                           base_view_sql, ephemeral_table_sql,
                                           ephemeral_view_sql)
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_docs_generate import (BaseDocsGenerate,
                                                        BaseDocsGenReferences)
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import \
    BaseSingularTestsEphemeral
from dbt.tests.adapter.basic.test_snapshot_check_cols import \
    BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import \
    BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_table_materialization import \
    BaseTableMaterialization
from dbt.tests.adapter.basic.test_validate_connection import \
    BaseValidateConnection
from dbt.tests.util import (check_relations_equal, check_result_nodes_by_name,
                            get_manifest, relation_from_name, run_dbt)
from tests.util import cleanup_s3_location, get_region, get_s3_location

s3bucket = get_s3_location()
region = get_region()
schema_name = "dbt_functional_test_01"

# override schema_base_yml to set missing database
schema_base_yml = """
version: 2
sources:
  - name: raw
    schema: "{{ target.schema }}"
    database: "{{ target.schema }}"
    tables:
      - name: seed
        identifier: "{{ var('seed_name', 'base') }}"
"""

# override base_materialized_var_sql to set strategy=insert_overwrite
config_materialized_var = """
  {{ config(materialized=var("materialized_var", "table")) }}
"""
config_incremental_strategy = """
  {{ config(incremental_strategy='insert_overwrite') }}
"""
model_base = """
  select * from {{ source('raw', 'seed') }}
"""
base_materialized_var_sql = config_materialized_var + config_incremental_strategy + model_base

class TestBaseCachingGlue(BaseAdapterMethod):
    pass

class TestSimpleMaterializationsGlue(BaseSimpleMaterializations):
    # all tests within this test has the same schema
    @pytest.fixture(scope="class")
    def unique_schema(request, prefix) -> str:
        return schema_name

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "base",
            "models": {
                "+incremental_strategy": "append",
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "view_model.sql": base_view_sql,
            "table_model.sql": base_table_sql,
            "swappable.sql": base_materialized_var_sql,
            "schema.yml": schema_base_yml,
        }

    @pytest.fixture(scope='class', autouse=True)
    def cleanup(self):
        cleanup_s3_location(s3bucket + schema_name, region)
        yield

    pass


class TestSingularTestsGlue(BaseSingularTests):
    @pytest.fixture(scope="class")
    def unique_schema(request, prefix) -> str:
        return schema_name

    pass


class TestEmptyGlue(BaseEmpty):
    @pytest.fixture(scope="class")
    def unique_schema(request, prefix) -> str:
        return schema_name

    pass


class TestEphemeralGlue(BaseEphemeral):
    # all tests within this test has the same schema
    @pytest.fixture(scope="class")
    def unique_schema(request, prefix) -> str:
        return schema_name

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "ephemeral.sql": base_ephemeral_sql,
            "view_model.sql": ephemeral_view_sql,
            "table_model.sql": ephemeral_table_sql,
            "schema.yml": schema_base_yml,
        }

    @pytest.fixture(scope='class', autouse=True)
    def cleanup(self):
        cleanup_s3_location(s3bucket + schema_name, region)
        yield

    # test_ephemeral with refresh table
    def test_ephemeral(self, project):
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 1
        relation = relation_from_name(project.adapter, "base")
        # run refresh table to disable the previous parquet file paths
        project.run_sql(f"refresh table {relation}")
        check_result_nodes_by_name(results, ["base"])

        # run command
        results = run_dbt(["run"])
        assert len(results) == 2
        relation_table_model = relation_from_name(project.adapter, "table_model")
        project.run_sql(f"refresh table {relation_table_model}")
        check_result_nodes_by_name(results, ["view_model", "table_model"])

        # base table rowcount
        # run refresh table to disable the previous parquet file paths
        project.run_sql(f"refresh table {relation}")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 10

        # relations equal
        check_relations_equal(project.adapter, ["base", "view_model", "table_model"])

        # catalog node count
        catalog = run_dbt(["docs", "generate"])
        catalog_path = os.path.join(project.project_root, "target", "catalog.json")
        assert os.path.exists(catalog_path)
        assert len(catalog.nodes) == 3
        assert len(catalog.sources) == 1

        # manifest (not in original)
        manifest = get_manifest(project.project_root)
        assert len(manifest.nodes) == 4
        assert len(manifest.sources) == 1

    pass

class TestSingularTestsEphemeralGlue(BaseSingularTestsEphemeral):
    @pytest.fixture(scope="class")
    def unique_schema(request, prefix) -> str:
        return schema_name

    pass

class TestIncrementalGlue(BaseIncremental):
    @pytest.fixture(scope='class', autouse=True)
    def cleanup(self):
        cleanup_s3_location(s3bucket + schema_name, region)
        yield

    @pytest.fixture(scope="class")
    def models(self):
        model_incremental = """
           select * from {{ source('raw', 'seed') }}
           """.strip()

        return {"incremental.sql": model_incremental, "schema.yml": schema_base_yml}

    @pytest.fixture(scope="class")
    def unique_schema(request, prefix) -> str:
        return schema_name

    # test_incremental with refresh table
    def test_incremental(self, project):
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 2

        # base table rowcount
        relation = relation_from_name(project.adapter, "base")
        # run refresh table to disable the previous parquet file paths
        project.run_sql(f"refresh table {relation}")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 10

        # added table rowcount
        relation = relation_from_name(project.adapter, "added")
        # run refresh table to disable the previous parquet file paths
        project.run_sql(f"refresh table {relation}")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 20

        # run command
        # the "seed_name" var changes the seed identifier in the schema file
        results = run_dbt(["run", "--vars", "seed_name: base"])
        assert len(results) == 1

        # check relations equal
        check_relations_equal(project.adapter, ["base", "incremental"])

        # change seed_name var
        # the "seed_name" var changes the seed identifier in the schema file
        results = run_dbt(["run", "--vars", "seed_name: added"])
        assert len(results) == 1

        # check relations equal
        check_relations_equal(project.adapter, ["added", "incremental"])

        # get catalog from docs generate
        catalog = run_dbt(["docs", "generate"])
        assert len(catalog.nodes) == 3
        assert len(catalog.sources) == 1

    pass


class TestGenericTestsGlue(BaseGenericTests):
    @pytest.fixture(scope="class")
    def unique_schema(request, prefix) -> str:
        return schema_name

    @pytest.fixture(scope='class', autouse=True)
    def cleanup(self):
        cleanup_s3_location(s3bucket + schema_name, region)
        yield

    def test_generic_tests(self, project):
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 1

        relation = relation_from_name(project.adapter, "base")
        # run refresh table to disable the previous parquet file paths
        project.run_sql(f"refresh table {relation}")

        # test command selecting base model
        results = run_dbt(["test", "-m", "base"])
        assert len(results) == 1

        # run command
        results = run_dbt(["run"])
        assert len(results) == 2

        # test command, all tests
        results = run_dbt(["test"])
        assert len(results) == 3

    pass

class TestDocsGenerateGlue(BaseDocsGenerate):
   pass


class TestDocsGenReferencesGlue(BaseDocsGenReferences):
   pass


# To Dev
#class TestSnapshotCheckColsGlue(BaseSnapshotCheckCols):
#    pass


#class TestSnapshotTimestampGlue(BaseSnapshotTimestamp):
#    pass

class TestTableMatGlue(BaseTableMaterialization):
   pass

class TestValidateConnectionGlue(BaseValidateConnection):
    pass