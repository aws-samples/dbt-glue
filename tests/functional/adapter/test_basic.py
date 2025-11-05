import os
import boto3
import pytest
from botocore.config import Config
from dbt.tests.adapter.basic.files import (base_ephemeral_sql, base_table_sql,
                                           base_view_sql, ephemeral_table_sql,
                                           ephemeral_view_sql)
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import BaseSingularTestsEphemeral
from dbt.tests.adapter.basic.test_table_materialization import BaseTableMaterialization
from dbt.tests.adapter.basic.test_validate_connection import BaseValidateConnection
from dbt.tests.util import (check_relations_equal, check_result_nodes_by_name,
                            get_manifest, relation_from_name, run_dbt)
from tests.util import get_s3_location

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
config_custom_group_id = """
  {{ config(meta={ "group_session_id": "test_group_id"}) }}
"""
config_custom_group_id2 = """
  {{ config(meta={ "group_session_id": "test_group_id2"}) }}
"""
config_materialized_with_custom_meta = """
  {{ config(materialized="table", meta={"workers": 3, "idle_timeout": 2}) }}
"""
config_materialized_with_custom_meta_shared = """
  {{ config(materialized="table", meta={ "group_session_id": "test_group_id"}) }}
"""
model_base = """
  select * from {{ source('raw', 'seed') }}
"""
base_materialized_var_sql = config_materialized_var + config_incremental_strategy + model_base

table_with_custom_meta = config_materialized_with_custom_meta + model_base

table_with_custom_meta_shared = config_custom_group_id + table_with_custom_meta

view_with_custom_meta_shared = config_custom_group_id + base_view_sql

@pytest.mark.skip(
    reason="Fails because the test tries to fetch the table metadata during the compile step, "
    "before the models are actually run. Not sure how this test is intended to work."
)
class TestBaseCachingGlue(BaseAdapterMethod):
    pass

class TestSimpleMaterializationsGlue(BaseSimpleMaterializations):
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
            "table_model.sql": table_with_custom_meta,
            "swappable.sql": base_materialized_var_sql,
            "schema.yml": schema_base_yml,
        }

    pass

class TestSimpleMaterializationsWithCustomMetaShared(TestSimpleMaterializationsGlue):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "view_model.sql": view_with_custom_meta_shared,
            "table_model.sql": table_with_custom_meta_shared,
            "swappable.sql": config_custom_group_id2 + base_materialized_var_sql,
            "schema.yml": schema_base_yml,
        }

    @pytest.fixture(scope="class")
    def custom_session_id(self):
        return 'dbt-glue__test_group_id'
    
    @pytest.fixture(scope="class")
    def custom_session_id2(self):
        return 'dbt-glue__test_group_id2'

    # remove session between tests to clear cached tables.
    # this isn't a problem in actual dbt runs since the same table isn't updated multiple times per job.
    @pytest.fixture(autouse=True)
    def cleanup_custom_session(self, dbt_profile_target, custom_session_id, custom_session_id2):
        yield
        boto_session = boto3.session.Session()
        glue_client = boto_session.client("glue", region_name=dbt_profile_target['region'])
        for id in (custom_session_id, custom_session_id2):
            glue_client.stop_session(Id=id)
            glue_client.delete_session(Id=id)

    def test_create_session(self, dbt_profile_target, custom_session_id, custom_session_id2):
        boto_session = boto3.session.Session()
        glue_client = boto_session.client("glue", region_name=dbt_profile_target['region'])
        run_dbt(["seed"])
        glue_session = glue_client.get_session(Id='dbt-glue')
        assert glue_session['Session']['Status'] == 'READY'

        # Make sure group session id doesn't exist.
        for id in (custom_session_id, custom_session_id2):
            try:
                glue_session = glue_client.get_session(Id=id)
                assert False, f"Session {id} should not exist but does"
            except glue_client.exceptions.EntityNotFoundException as e:
                pass
        run_dbt()

        # Ensure group session is created and still in ready state
        for id in (custom_session_id, custom_session_id2):
            glue_session = glue_client.get_session(Id=id)
            assert glue_session['Session']['Status'] == 'READY'
            assert glue_session['Session']['NumberOfWorkers'] == (3 if id == custom_session_id else 2)

class TestSimpleMaterializationsWithCustomMeta(TestSimpleMaterializationsGlue):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "view_model.sql": base_view_sql,
            "table_model.sql": table_with_custom_meta,
            "swappable.sql": base_materialized_var_sql,
            "schema.yml": schema_base_yml,
        }

    @pytest.fixture(scope="class")
    def custom_session_id(self):
        return 'dbt-glue__model_base_table_model'

    # remove session between tests to clear cached tables.
    # this isn't a problem in actual dbt runs since the same table isn't updated multiple times per job.
    @pytest.fixture(autouse=True)
    def cleanup_custom_session(self, dbt_profile_target, custom_session_id):
        yield
        boto_session = boto3.session.Session()
        glue_client = boto_session.client("glue", region_name=dbt_profile_target['region'])
        glue_client.stop_session(Id=custom_session_id)
        glue_client.delete_session(Id=custom_session_id)

    def test_create_session(self, dbt_profile_target, custom_session_id):
        run_dbt(["seed"])
        run_dbt()

        boto_session = boto3.session.Session()
        glue_client = boto_session.client("glue", region_name=dbt_profile_target['region'])
        glue_session = glue_client.get_session(Id=custom_session_id)
        assert glue_session['Session']['IdleTimeout'] == 2
        assert glue_session['Session']['NumberOfWorkers'] == 3


class TestSimpleMaterializationsWithUnrelatedMeta(TestSimpleMaterializationsGlue):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "view_model.sql": base_view_sql,
            "table_model.sql": '\n{{ config(materialized="table", meta={"unrelated_field": "base", "other_unrelated_field": 2}) }}\n' + model_base,
            "swappable.sql": base_materialized_var_sql,
            "schema.yml": schema_base_yml,
        }

    @pytest.fixture(scope="class")
    def custom_session_id(self):
        return 'dbt-glue__model_base_table_model'

    @pytest.fixture(autouse=True)
    def cleanup_custom_session(self, dbt_profile_target, custom_session_id):
        yield
        boto_session = boto3.session.Session()
        glue_client = boto_session.client("glue", region_name=dbt_profile_target['region'])
        glue_client.stop_session(Id=custom_session_id)
        glue_client.delete_session(Id=custom_session_id)

    def test_create_session(self, dbt_profile_target, custom_session_id):
        run_dbt(["seed"])
        run_dbt()

        boto_session = boto3.session.Session()
        glue_client = boto_session.client("glue", region_name=dbt_profile_target['region'])
        try:
            glue_session = glue_client.get_session(Id=custom_session_id)
            assert False, f"Session {custom_session_id} should not exist but does"
        except glue_client.exceptions.EntityNotFoundException as e:
            pass


class TestSingularTestsGlue(BaseSingularTests):
    pass


class TestEmptyGlue(BaseEmpty):
    pass


class TestEphemeralGlue(BaseEphemeral):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "ephemeral.sql": base_ephemeral_sql,
            "view_model.sql": ephemeral_view_sql,
            "table_model.sql": ephemeral_table_sql,
            "schema.yml": schema_base_yml,
        }

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
    pass


class TestIncrementalGlue(BaseIncremental):
    @pytest.fixture(scope="class")
    def models(self):
        incremental_sql = """
            {{ config(materialized="incremental",incremental_strategy="append") }}
            select * from {{ source('raw', 'seed') }}
            {% if is_incremental() %}
            where id > (select max(id) from {{ this }})
            {% endif %}
        """.strip()
        return {"incremental.sql": incremental_sql, "schema.yml": schema_base_yml}

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


class TestIncrementalGlueWithCustomLocation(TestIncrementalGlue):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        default_location=get_s3_location()
        custom_prefix = "{{target.schema}}/custom/incremental"
        custom_location = default_location+custom_prefix
        return {
            "name": "incremental",
            "models": {
                "+custom_location": custom_location
            }
        }


class TestGenericTestsGlue(BaseGenericTests):
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


class TestTableMatGlue(BaseTableMaterialization):
    pass


class TestValidateConnectionGlue(BaseValidateConnection):
    pass

