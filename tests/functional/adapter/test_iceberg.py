from time import sleep
from typing import List
import pytest
from dbt.tests.adapter.basic.files import (base_table_sql, base_view_sql,)
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
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


class TestSchemaChangeGlue:
    """Test schema change handling in Glue adapter with Iceberg tables"""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        """Configure project to use Iceberg format for tables"""
        return {
            "name": "schema_change_test",
            "models": {
                "+file_format": "iceberg",  # Set default file format to Iceberg
                "+on_schema_change": "sync_all_columns"  # Set default schema change handling
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        """Define test models to demonstrate schema changes"""
        return {
            # Initial model with base schema
            "base_model.sql": """
select 
    1 as id, 
    'John' as first_name, 
    'Doe' as last_name, 
    'john.doe@example.com' as email
union all
select 
    2 as id, 
    'Jane' as first_name, 
    'Smith' as last_name, 
    'jane.smith@example.com' as email
            """,

            # Incremental model with hard-coded configs
            "incremental_model.sql": """
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        on_schema_change='sync_all_columns'
    )
}}

{{ log("DEBUG - Model config - file_format: " ~ config.get('file_format'), info=true) }}
{{ log("DEBUG - Model config - on_schema_change: " ~ config.get('on_schema_change'), info=true) }}

select * from {{ ref('base_model') }}
            """
        }

    def test_schema_change_detection(self, project):
        """Test that schema changes are properly detected and handled"""
        # Initial run with original models - use full-refresh
        results = run_dbt(["run", "--select", "base_model incremental_model", "--full-refresh"])
        assert len(results) == 2, "Initial run should succeed with both models"

        # Check that incremental_model has the expected schema
        relation = relation_from_name(project.adapter, "incremental_model")
        initial_columns = [c.name for c in project.adapter.get_columns_in_relation(relation)]
        print(f"Initial columns: {initial_columns}")
        assert "email" in initial_columns, "Initial schema should contain email column"

        # Update the base model to add a new column
        with open(f"{project.project_root}/models/base_model.sql", "w") as f:
            f.write("""
    select
        1 as id,
        'John' as first_name,
        'Doe' as last_name,
        'john.doe@example.com' as email,
        '555-1234' as phone
    union all
    select
        2 as id,
        'Jane' as first_name,
        'Smith' as last_name,
        'jane.smith@example.com' as email,
        '555-5678' as phone
            """)

        results = run_dbt(["run", "--select", "base_model incremental_model", "--full-refresh"])
        assert len(results) == 2, "Full refresh with schema changes should succeed"

        # Verify the incremental model has the new column after schema change
        relation = relation_from_name(project.adapter, "incremental_model")
        updated_columns = [c.name for c in project.adapter.get_columns_in_relation(relation)]
        print(f"Updated columns: {updated_columns}")
        assert "phone" in updated_columns, "Schema change should add the phone column"


class TestIcebergTimestampColumnEnabled:
    """Test adding update_iceberg_ts column to Iceberg tables when enabled"""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        """Configure project to use Iceberg format with timestamp column enabled"""
        return {
            "name": "iceberg_timestamp_test_enabled",
            "models": {
                "+file_format": "iceberg",  # Set default file format to Iceberg
                "+add_iceberg_timestamp": True  # Enable timestamp column addition
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        """Define test models"""
        return {
            # Base model
            "base_model.sql": """
select 
    1 as id, 
    'John' as first_name, 
    'Doe' as last_name
union all
select 
    2 as id, 
    'Jane' as first_name, 
    'Smith' as last_name
            """,

            # Incremental model that references base_model
            "incremental_model.sql": """
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg'
    )
}}

select * from {{ ref('base_model') }}
            """
        }

    def test_iceberg_timestamp_column_added(self, project):
        """Test that update_iceberg_ts column is automatically added when enabled"""
        # Run the base model
        results = run_dbt(["run", "--select", "base_model"])
        assert len(results) == 1, "Should build base model successfully"

        # Check base model columns
        base_relation = relation_from_name(project.adapter, "base_model")
        base_columns = [c.name.lower() for c in project.adapter.get_columns_in_relation(base_relation)]
        assert "update_iceberg_ts" in base_columns, "Base model should have update_iceberg_ts column when enabled"

        # Run the incremental model
        results = run_dbt(["run", "--select", "incremental_model"])
        assert len(results) == 1, "Should build incremental model successfully"

        # Check incremental model columns
        incremental_relation = relation_from_name(project.adapter, "incremental_model")
        incremental_columns = [c.name.lower() for c in project.adapter.get_columns_in_relation(incremental_relation)]
        assert "update_iceberg_ts" in incremental_columns, "Incremental model should have update_iceberg_ts column when enabled"
        assert len([c for c in incremental_columns if c == "update_iceberg_ts"]) == 1, "Should not have duplicate update_iceberg_ts columns"


class TestIcebergTimestampColumnDisabled:
    """Test behavior when update_iceberg_ts column is disabled for Iceberg tables"""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        """Configure project to use Iceberg format with timestamp column disabled"""
        return {
            "name": "iceberg_timestamp_test_disabled",
            "models": {
                "+file_format": "iceberg",  # Set default file format to Iceberg
                "+add_iceberg_timestamp": False  # Disable timestamp column addition
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        """Define test models"""
        return {
            # Base model
            "base_model.sql": """
select 
    1 as id, 
    'John' as first_name, 
    'Doe' as last_name
union all
select 
    2 as id, 
    'Jane' as first_name, 
    'Smith' as last_name
            """,

            # Incremental model that references base_model
            "incremental_model.sql": """
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg'
    )
}}

select * from {{ ref('base_model') }}
            """
        }

    def test_iceberg_timestamp_column_not_added(self, project):
        """Test that update_iceberg_ts column is not added when disabled"""
        # Run the base model
        results = run_dbt(["run", "--select", "base_model"])
        assert len(results) == 1, "Should build base model successfully"

        # Check base model columns
        base_relation = relation_from_name(project.adapter, "base_model")
        base_columns = [c.name.lower() for c in project.adapter.get_columns_in_relation(base_relation)]
        assert "update_iceberg_ts" not in base_columns, "Base model should NOT have update_iceberg_ts column when disabled"

        # Run the incremental model
        results = run_dbt(["run", "--select", "incremental_model"])
        assert len(results) == 1, "Should build incremental model successfully"

        # Check incremental model columns
        incremental_relation = relation_from_name(project.adapter, "incremental_model")
        incremental_columns = [c.name.lower() for c in project.adapter.get_columns_in_relation(incremental_relation)]
        assert "update_iceberg_ts" not in incremental_columns, "Incremental model should NOT have update_iceberg_ts column when disabled"
