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
        
        project.run_sql(f"refresh table {relation}")
        # run refresh table to disable the previous parquet file paths
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 10

        # added table rowcount
        relation = relation_from_name(project.adapter, "added")
        project.run_sql(f"refresh table {relation}")
        # run refresh table to disable the previous parquet file paths
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


class TestIcebergTimestamp(BaseSimpleMaterializations):
    @pytest.fixture(scope="class")
    def models(self):
        # Define the base model SQL with timestamp enabled (default)
        timestamp_enabled_sql = """
            {{ config(
                materialized="table",
                file_format="iceberg",
                add_iceberg_timestamp=true
            ) }}
            select * from {{ source('raw', 'seed') }}
        """.strip()

        # Define the model SQL with timestamp disabled
        timestamp_disabled_sql = """
            {{ config(
                materialized="table",
                file_format="iceberg",
                add_iceberg_timestamp=false
            ) }}
            select * from {{ source('raw', 'seed') }}
        """.strip()

        # Models configuration with both test cases
        return {
            "enabled_timestamp.sql": timestamp_enabled_sql,
            "disabled_timestamp.sql": timestamp_disabled_sql,
            "schema.yml": schema_base_yml,
        }

    def test_iceberg_timestamp(self, project):
        # Run initial seed
        results = run_dbt(["seed"])
        assert len(results) == 1

        # Run the models
        results = run_dbt(["run"])
        assert len(results) == 2

        # Check that the timestamp column exists in the enabled model
        relation_enabled = relation_from_name(project.adapter, "enabled_timestamp")
        result = project.run_sql(
            f"describe table {relation_enabled}",
            fetch="all"
        )
        column_names = [row[0].lower() for row in result]
        assert "update_iceberg_ts" in column_names

        # Check that the timestamp column does not exist in the disabled model
        relation_disabled = relation_from_name(project.adapter, "disabled_timestamp")
        result = project.run_sql(
            f"describe table {relation_disabled}",
            fetch="all"
        )
        column_names = [row[0].lower() for row in result]
        assert "update_iceberg_ts" not in column_names

        # Verify that the base data is correctly copied
        relation_base = relation_from_name(project.adapter, "base")

        # For enabled timestamp model, exclude the timestamp column when comparing
        sql_enabled = f"""
        select * except(update_iceberg_ts)
        from {relation_enabled}
        order by id
        """
        result_enabled = project.run_sql(sql_enabled, fetch="all")

        # For disabled timestamp model
        sql_disabled = f"""
        select *
        from {relation_disabled}
        order by id
        """
        result_disabled = project.run_sql(sql_disabled, fetch="all")

        # For base table
        sql_base = f"""
        select *
        from {relation_base}
        order by id
        """
        result_base = project.run_sql(sql_base, fetch="all")

        # Compare results
        assert result_base == result_disabled
        assert result_base == result_enabled

        # Check that timestamp value is recent
        sql_timestamp = f"""
        select update_iceberg_ts
        from {relation_enabled}
        limit 1
        """
        result_timestamp = project.run_sql(sql_timestamp, fetch="one")
        timestamp = result_timestamp[0]

        # Verify that the timestamp is within 5 minutes
        from datetime import datetime, timezone
        current_time = datetime.now(timezone.utc)
        timestamp_diff = (current_time - timestamp).total_seconds()
        assert timestamp_diff < 300  # Timestamp should be less than 5 minute old
