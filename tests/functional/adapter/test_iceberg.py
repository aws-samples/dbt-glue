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
                            run_dbt, check_relations_equal, relation_from_name, write_file)
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

        # check relation types, swappable is view (or table if using Iceberg)
        expected = {
            "base": "table",
            "view_model": "view",
            "table_model": "table",
            "swappable": "table",  # For Iceberg, views are actually tables
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
                "+file_format": "iceberg",
                "+on_schema_change": "sync_all_columns"
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
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
        incremental_strategy='append',
        file_format='iceberg',
        on_schema_change='sync_all_columns'
    )
}}

{{ log("DEBUG - Model config - file_format: " ~ config.get('file_format'), info=true) }}
{{ log("DEBUG - Model config - on_schema_change: " ~ config.get('on_schema_change'), info=true) }}

select * from glue_catalog.{{ ref('base_model') }}
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

select * from glue_catalog.{{ ref('base_model') }}
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

select * from glue_catalog.{{ ref('base_model') }}
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


class TestIcebergMultipleRuns:
    """Test running dbt multiple times on Iceberg tables to reproduce customer bug"""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": """id,first_name,last_name,email,gender,ip_address
1,Jack,Hunter,jhunter0@pbs.org,Male,59.80.20.168
2,Kathryn,Walker,kwalker1@ezinearticles.com,Female,194.121.179.35
3,Gerald,Ryan,gryan2@com.com,Male,11.3.212.243
4,Bonnie,Spencer,bspencer3@ameblo.jp,Female,216.32.196.175
5,Harold,Taylor,htaylor4@people.com.cn,Male,253.10.246.136
6,Jacqueline,Griffin,jgriffin5@t.co,Female,16.13.192.220
7,Wanda,Arnold,warnold6@google.nl,Female,232.116.150.64
8,Craig,Ortiz,cortiz7@sciencedaily.com,Male,199.126.106.13
9,Gary,Day,gday8@nih.gov,Male,35.81.68.186
10,Rose,Wright,rwright9@yahoo.co.jp,Female,236.82.178.100
""",
            "added.csv": """id,first_name,last_name,email,gender,ip_address
11,Nicole,Cunningham,ncunninghama@google.com.hk,Female,93.5.163.58
12,Carlos,Russell,crussellb@narod.ru,Male,91.33.25.80
13,Victor,Hernandez,vhernandezc@yahoo.com,Male,193.232.11.125
14,Kathryn,Gilbert,kgilbertd@sophos.com,Female,112.79.164.28
15,Christine,Payne,cpaynee@bbc.co.uk,Female,94.78.93.129
16,Jacqueline,Garza,jgarzaf@sciencedaily.com,Female,161.38.219.178
17,Benjamin,Cooper,bcooperg@wired.com,Male,76.194.170.108
18,Gregory,Hamilton,ghamiltonh@canalblog.com,Male,194.151.170.21
19,Stephanie,Owens,sowensi@dot.gov,Female,131.134.82.96
20,Kimberly,Johnson,kjohnsonj@sun.com,Female,67.198.236.255
21,Jacqueline,Owens,jowensk@buzzfeed.com,Female,189.53.175.172
22,Sarah,Hanson,shansonl@livejournal.com,Female,76.82.36.106
23,Thomas,Tucker,ttuckerm@usatoday.com,Male,109.251.164.84
24,Willie,Gonzales,wgonzalesn@cpanel.net,Male,223.80.168.239
25,Dennis,Carpenter,dcarpenero@ow.ly,Male,105.177.74.76
26,Phillip,Welch,pwelchp@usgs.gov,Male,156.81.69.181
27,Edward,Reynolds,ereynoldsq@angelfire.com,Male,56.82.194.196
28,Mark,Sullivan,msullivanr@state.tx.us,Male,166.220.5.88
29,Dennis,Garza,dgarzas@webnode.com,Male,153.40.18.228
30,Kathleen,Nichols,knicholst@mozilla.com,Female,161.88.82.200
"""
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        """Configure project to use Iceberg format for tables"""
        return {
            "name": "iceberg_multiple_runs_test",
            "models": {
                "+file_format": "iceberg",
                "+on_schema_change": "append_new_columns"
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        incremental_sql = """
            {{ config(materialized="incremental", incremental_strategy="append", file_format="iceberg", on_schema_change="append_new_columns") }}
            select * from {{ source('raw', 'seed') }}
            {% if is_incremental() %}
            where id > (select max(id) from glue_catalog.{{ this }})
            {% endif %}
        """.strip()
        return {"incremental.sql": incremental_sql, "schema.yml": schema_base_yml}

    def test_multiple_runs(self, project):
        """Test that running an Iceberg incremental model multiple times works correctly"""
        # seed command
        results = run_dbt(["seed"])
        assert len(results) > 0, "Should create seed tables"

        # First run of the incremental model with base data
        results = run_dbt(["run", "--vars", "seed_name: base"])
        assert len(results) == 1, "First run should succeed"

        # Second run of the incremental model with added data
        results = run_dbt(["run", "--vars", "seed_name: added"])
        assert len(results) == 1, "Second run with new data should also succeed"

        # Verify the incremental model has the expected data (base + added)
        relation = relation_from_name(project.adapter, "incremental")
        project.run_sql(f"refresh table glue_catalog.{relation.schema}.{relation.identifier}")
        result = project.run_sql(f"select count(*) as num_rows from glue_catalog.{relation.schema}.{relation.identifier}", fetch="one")

        # Assuming no duplicates between base and added
        assert result[0] == 30, "Incremental model should have combined records from both runs (10 + 20 = 30)"


class TestIcebergMergeWithSchemaChange:
    """Test merge strategy with schema changes for Iceberg tables"""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "iceberg_merge_schema_test",
            "models": {
                "+file_format": "iceberg"
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_merge_model.sql": """
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='id',
        file_format='iceberg',
        on_schema_change='append_new_columns'
    )
}}

select 1 as id, 'John' as name, 100 as value
union all
select 2 as id, 'Jane' as name, 200 as value
{% if is_incremental() %}
union all
select 3 as id, 'Bob' as name, 300 as value
{% endif %}
            """
        }

    def test_merge_with_schema_change(self, project):
        """Test that merge strategy works correctly with schema changes"""
        # First run
        results = run_dbt(["run"])
        assert len(results) == 1

        # Verify initial data
        schema_name = project.test_schema
        result = project.run_sql(f"select count(*) from glue_catalog.{schema_name}.incremental_merge_model", fetch="one")
        assert result[0] == 2

        # Update model to add new column and trigger schema change
        updated_model = """
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='id',
        file_format='iceberg',
        on_schema_change='append_new_columns'
    )
}}

select 1 as id, 'John' as name, 100 as value, 'A' as category
union all
select 2 as id, 'Jane' as name, 200 as value, 'B' as category
{% if is_incremental() %}
union all
select 3 as id, 'Bob' as name, 300 as value, 'C' as category
{% endif %}
        """
        
        write_file(updated_model, project.project_root, "models", "incremental_merge_model.sql")
        
        # Second run with schema change and new data
        results = run_dbt(["run"])
        assert len(results) == 1

        # Verify merge worked correctly - should have 3 rows total
        result = project.run_sql(f"select count(*) from glue_catalog.{schema_name}.incremental_merge_model", fetch="one")
        assert result[0] == 3

        # Verify new column exists
        columns = project.run_sql(f"describe glue_catalog.{schema_name}.incremental_merge_model", fetch="all")
        column_names = [col[0] for col in columns]
        assert 'category' in column_names


class TestIcebergTableRefresh:
    """Test case to reproduce the issue with refreshing Iceberg tables."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        """Configure project to use Iceberg format for tables"""
        return {
            "name": "iceberg_refresh_test",
            "models": {
                "+file_format": "iceberg"
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "simple_model.sql": """
{{ config(
    materialized='table',
    file_format='iceberg'
) }}

select 1 as id, 'test' as name
"""
        }

    def test_iceberg_refresh(self, project):
        """Test that we can run an Iceberg table model twice without errors.
        This test reproduces the issue reported in GitHub issue #537.
        """
        # First run - should succeed
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        # Second run - should also succeed
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"
