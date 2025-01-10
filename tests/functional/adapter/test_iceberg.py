from typing import List
import pytest
from dbt.tests.adapter.basic.files import (base_table_sql, base_view_sql,)
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import BaseSingularTestsEphemeral
from dbt.tests.adapter.basic.test_table_materialization import BaseTableMaterialization
from dbt.tests.adapter.basic.test_validate_connection import BaseValidateConnection
from dbt.tests.util import (check_result_nodes_by_name,check_relation_types, check_relations_equal_with_relations, TestProcessingException,
                            run_dbt, check_relations_equal, relation_from_name)
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
  {{ config(materialized=var("materialized_var", "table"), file_format="iceberg") }}
"""
config_incremental_strategy = """
  {{ config(incremental_strategy='insert_overwrite') }}
"""
model_base = """
  select * from {{ source('raw', 'seed') }}
"""
base_materialized_var_sql = config_materialized_var + config_incremental_strategy + model_base


def get_relation(adapter, name: str):
    """reverse-engineer a relation from a given name and
    the adapter. The relation name is split by the '.' character.
    """

    # Different adapters have different Relation classes
    cls = adapter
    credentials = adapter.config.credentials

    # Make sure we have database/schema/identifier parts, even if
    # only identifier was supplied.
    relation_parts = name.split(".")
    if len(relation_parts) == 1:
        relation_parts.insert(0, credentials.schema)
    if len(relation_parts) == 2:
        relation_parts.insert(0, credentials.database)
    relation = cls.get_relation(relation_parts[0], relation_parts[1], relation_parts[2])

    return relation


def check_relations_equal(adapter, relation_names: List, compare_snapshot_cols=False):
    if len(relation_names) < 2:
        raise TestProcessingException(
            "Not enough relations to compare",
        )
    relations = [get_relation(adapter, name) for name in relation_names]
    return check_relations_equal_with_relations(
        adapter, relations, compare_snapshot_cols=compare_snapshot_cols
    )


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
            "table_model.sql": base_table_sql,
            "swappable.sql": base_materialized_var_sql,
            "schema.yml": schema_base_yml,
        }

    def test_base(self, project):
        # seed command
        results = run_dbt(["seed"])
        # seed result length
        assert len(results) == 1

        # run command
        results = run_dbt()
        # run result length
        assert len(results) == 3

        # names exist in result nodes
        check_result_nodes_by_name(results, ["view_model", "table_model", "swappable"])

        # check relation types
        expected = {
            "base": "table",
            "view_model": "view",
            "table_model": "table",
            "swappable": "table",
        }
        check_relation_types(project.adapter, expected)

        # base table rowcount
        relation = relation_from_name(project.adapter, "base")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 10

        # relations_equal
        check_relations_equal(project.adapter, ["base", "view_model", "table_model", "swappable"])

        # check relations in catalog
        catalog = run_dbt(["docs", "generate"])
        assert len(catalog.nodes) == 4
        assert len(catalog.sources) == 1

        # run_dbt changing materialized_var to view
        if project.test_config.get("require_full_refresh", False):  # required for BigQuery
            results = run_dbt(
                ["run", "--full-refresh", "-m", "swappable", "--vars", "materialized_var: view"]
            )
        else:
            results = run_dbt(["run", "-m", "swappable", "--vars", "materialized_var: view"])
        assert len(results) == 1

        # check relation types, swappable is view
        expected = {
            "base": "table",
            "view_model": "view",
            "table_model": "table",
            "swappable": "view",
        }
        check_relation_types(project.adapter, expected)

        # run_dbt changing materialized_var to incremental
        results = run_dbt(["run", "-m", "swappable", "--vars", "materialized_var: incremental"])
        assert len(results) == 1

        # check relation types, swappable is table
        expected = {
            "base": "table",
            "view_model": "view",
            "table_model": "table",
            "swappable": "table",
        }
        check_relation_types(project.adapter, expected)


class TestSingularTestsGlue(BaseSingularTests):
    pass


class TestEmptyGlue(BaseEmpty):
    pass


class TestSingularTestsEphemeralGlue(BaseSingularTestsEphemeral):
    pass


class TestIncrementalGlue(BaseIncremental):
    @pytest.fixture(scope="class")
    def models(self):
        incremental_sql = """
            {{ config(materialized="incremental",incremental_strategy="append", file_format="iceberg") }}
            select * from {{ source('raw', 'seed') }}
            {% if is_incremental() %}
            where id > (select max(id) from glue_catalog.{{ this }})
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
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 10

        # added table rowcount
        relation = relation_from_name(project.adapter, "added")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 20

        # run command
        # the "seed_name" var changes the seed identifier in the schema file
        results = run_dbt(["run", "--vars", "seed_name: base"])
        assert len(results) == 1

        # check relations equal
        incremental_relation = relation_from_name(project.adapter, "incremental")
        project.run_sql(f"refresh table {incremental_relation}")
        check_relations_equal(project.adapter, ["base", "incremental"])

        # change seed_name var
        # the "seed_name" var changes the seed identifier in the schema file
        results = run_dbt(["run", "--vars", "seed_name: added"])
        assert len(results) == 1

        # check relations equal
        project.run_sql(f"refresh table {incremental_relation}")
        check_relations_equal(project.adapter, ["added", "incremental"])

        # get catalog from docs generate
        catalog = run_dbt(["docs", "generate"])
        assert len(catalog.nodes) == 3
        assert len(catalog.sources) == 1

    pass


class TestIncrementalGlueWithCustomLocation(TestIncrementalGlue):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        default_location = get_s3_location()
        custom_prefix = "{{target.schema}}/custom/incremental"
        custom_location = default_location + custom_prefix
        return {
            "name": "incremental",
            "models": {
                "+file_format": "iceberg",
                "+add_iceberg_timestamp": True,
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


class TestIcebergTimestamp(BaseSimpleMaterializations):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        """Project level configuration"""
        return {
            "name": "iceberg_timestamp_test",
            "models": {
                "+file_format": "iceberg"
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        """Model definitions for timestamp testing"""
        # Model with timestamp enabled (default behavior)
        timestamp_enabled_sql = """
            {{ config(
                materialized="table",
                file_format="iceberg",
                add_iceberg_timestamp=true
            ) }}
            select * from {{ source('raw', 'seed') }}
        """

        # Model with timestamp explicitly disabled
        timestamp_disabled_sql = """
            {{ config(
                materialized="table",
                file_format="iceberg",
                add_iceberg_timestamp=false
            ) }}
            select * from {{ source('raw', 'seed') }}
        """

        # Schema definition with source configuration
        schema_yml = """
version: 2
sources:
  - name: raw
    schema: "{{ target.schema }}"
    database: "{{ target.schema }}"
    tables:
      - name: seed
        identifier: "base"
"""

        return {
            "enabled_timestamp.sql": timestamp_enabled_sql,
            "disabled_timestamp.sql": timestamp_disabled_sql,
            "schema.yml": schema_yml,
        }

    def test_base(self, project):
        """Test basic table creation and model count"""
        # Run initial seed
        results = run_dbt(["seed"])
        assert len(results) == 1

        # Run models
        results = run_dbt(["run"])
        assert len(results) == 2  # Two models: enabled and disabled

        # Get relations
        relation_enabled = relation_from_name(project.adapter, "enabled_timestamp")
        relation_disabled = relation_from_name(project.adapter, "disabled_timestamp")

        # Refresh tables to ensure metadata is up to date
        project.run_sql(f"refresh table {relation_enabled}")
        project.run_sql(f"refresh table {relation_disabled}")

        # Verify basic table functionality
        for relation in [relation_enabled, relation_disabled]:
            result = project.run_sql(f"select count(*) from {relation}", fetch="one")
            assert result[0] > 0, f"Table {relation} should contain rows"

    def test_iceberg_timestamp(self, project):
        """Test timestamp column behavior in Iceberg tables"""
        # Run initial setup
        results = run_dbt(["seed"])
        assert len(results) == 1

        results = run_dbt(["run"])
        assert len(results) == 2

        # Get relations
        relation_enabled = relation_from_name(project.adapter, "enabled_timestamp")
        relation_disabled = relation_from_name(project.adapter, "disabled_timestamp")

        # Refresh tables to ensure metadata is up to date
        project.run_sql(f"refresh table {relation_enabled}")
        project.run_sql(f"refresh table {relation_disabled}")

        # Verify timestamp column exists in enabled table
        try:
            result = project.run_sql(f"select update_iceberg_ts from {relation_enabled} limit 1", fetch="one")
            assert result is not None, "update_iceberg_ts should contain valid timestamps"
            has_timestamp = True
        except Exception:
            has_timestamp = False
        assert has_timestamp, "update_iceberg_ts column should exist in enabled table"

        # Verify timestamp column does not exist in disabled table
        try:
            project.run_sql(f"select update_iceberg_ts from {relation_disabled} limit 1", fetch="one")
            has_timestamp = True
        except Exception:
            has_timestamp = False
        assert not has_timestamp, "update_iceberg_ts column should not exist in disabled table"

        # Verify data consistency
        base_relation = relation_from_name(project.adapter, "base")
        enabled_base_sql = f"""
        select count(*) from (
            select id, name, some_date from {relation_enabled}
            except
            select * from {base_relation}
        )
        """
        disabled_base_sql = f"""
        select count(*) from (
            select * from {relation_disabled}
            except
            select * from {base_relation}
        )
        """
        enabled_diff = project.run_sql(enabled_base_sql, fetch="one")[0]
        disabled_diff = project.run_sql(disabled_base_sql, fetch="one")[0]

        assert enabled_diff == 0, "Data in enabled model should match base (excluding timestamp)"
        assert disabled_diff == 0, "Data in disabled model should match base"