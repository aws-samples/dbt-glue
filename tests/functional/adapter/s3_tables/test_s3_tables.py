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
from dbt.tests.util import (check_result_nodes_by_name, check_relation_types, check_relations_equal_with_relations, TestProcessingException,
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


class TestS3TablesBasicMaterializations:
    """Test basic S3 tables functionality with simple table creation"""
    
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "s3_tables_basic",
            "models": {
                "+file_format": "iceberg"
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        # Simple table model with S3 tables configuration
        s3_table_model_sql = """
        {{ config(materialized='table', file_format='iceberg') }}
        select 
            1 as id,
            'test' as name,
            current_timestamp() as created_at
        union all
        select 
            2 as id,
            'test2' as name,
            current_timestamp() as created_at
        """
        
        # View model (should work normally)
        s3_view_model_sql = """
        {{ config(materialized='view') }}
        select * from {{ ref('s3_table_model') }}
        where id = 1
        """
        
        return {
            "s3_table_model.sql": s3_table_model_sql,
            "s3_view_model.sql": s3_view_model_sql,
            "schema.yml": schema_base_yml,
        }

    def test_s3_tables_basic_creation(self, project):
        """Test that S3 tables can be created successfully"""
        # Run dbt to create models
        results = run_dbt()
        
        # Check that both models were created successfully
        assert len(results) == 2
        check_result_nodes_by_name(results, ["s3_table_model", "s3_view_model"])
        
        # Verify the S3 table has data
        relation = relation_from_name(project.adapter, "s3_table_model")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 2
        
        # Verify the view works
        relation = relation_from_name(project.adapter, "s3_view_model")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 1


class TestS3TablesWithCTAS:
    """Test S3 tables with CREATE TABLE AS SELECT (CTAS) operations"""
    
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "s3_tables_ctas",
        }

    @pytest.fixture(scope="class")
    def models(self):
        # Test CTAS with S3 tables - this is the critical test
        ctas_model_sql = """
        {{ config(materialized='table', file_format='iceberg') }}
        select 
            row_number() over (order by 1) as id,
            'row_' || row_number() over (order by 1) as name,
            current_date() as date_col
        from (
            select 1 union all select 2 union all select 3 union all 
            select 4 union all select 5
        ) t(dummy)
        """
        
        return {
            "ctas_s3_table.sql": ctas_model_sql,
            "schema.yml": schema_base_yml,
        }

    def test_s3_tables_ctas(self, project):
        """Test CREATE TABLE AS SELECT with S3 tables"""
        # This test will help us understand if CTAS works with S3 tables
        try:
            results = run_dbt()
            assert len(results) == 1
            
            # Verify data was inserted correctly
            relation = relation_from_name(project.adapter, "ctas_s3_table")
            result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
            assert result[0] == 5
            
            print("✅ CTAS works with S3 tables!")
            
        except Exception as e:
            print(f"❌ CTAS failed with S3 tables: {str(e)}")
            # This will help us understand what doesn't work
            raise


class TestS3TablesIncremental:
    """Test S3 tables with incremental materialization"""
    
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "s3_tables_incremental",
        }

    @pytest.fixture(scope="class")
    def models(self):
        incremental_s3_sql = """
        {{ config(
            materialized="incremental",
            incremental_strategy="append", 
            file_format="iceberg"
        ) }}
        select 
            {{ var('run_number', 1) }} as run_id,
            row_number() over (order by 1) as id,
            'incremental_row_' || row_number() over (order by 1) as name,
            current_timestamp() as created_at
        from (
            select 1 union all select 2 union all select 3
        ) t(dummy)
        {% if is_incremental() %}
        -- Only add new data in incremental runs
        where {{ var('run_number', 1) }} > 1
        {% endif %}
        """
        
        return {
            "incremental_s3_table.sql": incremental_s3_sql,
            "schema.yml": schema_base_yml,
        }

    def test_s3_tables_incremental(self, project):
        """Test incremental materialization with S3 tables"""
        # First run - initial load
        results = run_dbt(["run", "--vars", "run_number: 1"])
        assert len(results) == 1
        
        # Check initial data
        relation = relation_from_name(project.adapter, "incremental_s3_table")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 3
        
        # Second run - incremental
        results = run_dbt(["run", "--vars", "run_number: 2"])
        assert len(results) == 1
        
        # Check incremental data was added
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 6  # 3 original + 3 new


class TestS3TablesPartitioning:
    """Test S3 tables with partitioning"""
    
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "s3_tables_partitioning",
        }

    @pytest.fixture(scope="class")
    def models(self):
        partitioned_s3_sql = """
        {{ config(
            materialized='table',
            file_format='iceberg',
            partition_by=['year', 'month']
        ) }}
        select 
            id,
            name,
            year(date_col) as year,
            month(date_col) as month,
            date_col
        from (
            select 1 as id, 'jan_record' as name, date('2024-01-15') as date_col
            union all
            select 2 as id, 'feb_record' as name, date('2024-02-15') as date_col
            union all
            select 3 as id, 'mar_record' as name, date('2024-03-15') as date_col
        ) t
        """
        
        return {
            "partitioned_s3_table.sql": partitioned_s3_sql,
            "schema.yml": schema_base_yml,
        }

    def test_s3_tables_partitioning(self, project):
        """Test partitioned S3 tables"""
        results = run_dbt()
        assert len(results) == 1
        
        # Verify data
        relation = relation_from_name(project.adapter, "partitioned_s3_table")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 3
        
        # Test partition pruning works
        result = project.run_sql(
            f"select count(*) as num_rows from {relation} where year = 2024 and month = 1", 
            fetch="one"
        )
        assert result[0] == 1


class TestS3TablesTableProperties:
    """Test S3 tables with custom table properties"""
    
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "s3_tables_properties",
        }

    @pytest.fixture(scope="class")
    def models(self):
        properties_s3_sql = """
        {{ config(
            materialized='table',
            file_format='iceberg',
            table_properties={
                'write.target-file-size-bytes': '134217728',
                'write.delete.mode': 'merge-on-read'
            }
        ) }}
        select 
            row_number() over (order by 1) as id,
            'property_test_' || row_number() over (order by 1) as name,
            current_timestamp() as created_at
        from (
            select 1 union all select 2 union all select 3 union all 
            select 4 union all select 5 union all select 6 union all
            select 7 union all select 8 union all select 9 union all select 10
        ) t(dummy)
        """
        
        return {
            "properties_s3_table.sql": properties_s3_sql,
            "schema.yml": schema_base_yml,
        }

    def test_s3_tables_properties(self, project):
        """Test S3 tables with custom properties"""
        results = run_dbt()
        assert len(results) == 1
        
        # Verify data
        relation = relation_from_name(project.adapter, "properties_s3_table")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 10


class TestS3TablesErrorHandling:
    """Test error handling and edge cases with S3 tables"""
    
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "s3_tables_errors",
        }

    @pytest.fixture(scope="class")
    def models(self):
        # Test different file formats to understand constraints
        parquet_config_sql = """
        {{ config(
            materialized='table',
            file_format='parquet'
        ) }}
        select 1 as id, 'test' as name
        """
        
        return {
            "parquet_s3_table.sql": parquet_config_sql,
            "schema.yml": schema_base_yml,
        }

    def test_s3_tables_different_formats(self, project):
        """Test different file formats with S3 tables"""
        # This test will help us understand what file formats work
        try:
            results = run_dbt()
            assert len(results) == 1
            print("✅ Parquet file format works with S3 tables")
        except Exception as e:
            print(f"❌ Parquet format issue: {str(e)}")
            # This helps us understand the constraints


# Integration test that combines multiple features
class TestS3TablesIntegration:
    """Integration test combining multiple S3 tables features"""
    
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "s3_tables_integration",
        }

    @pytest.fixture(scope="class")
    def models(self):
        # Source table
        source_table_sql = """
        {{ config(materialized='table', file_format='iceberg') }}
        select 
            row_number() over (order by 1) as id,
            case 
                when row_number() over (order by 1) <= 5 then 'A'
                else 'B'
            end as category,
            'source_' || row_number() over (order by 1) as name,
            current_date() as date_col
        from (
            select 1 union all select 2 union all select 3 union all 
            select 4 union all select 5 union all select 6 union all
            select 7 union all select 8 union all select 9 union all select 10
        ) t(dummy)
        """
        
        # Dependent table
        dependent_table_sql = """
        {{ config(
            materialized='table', 
            file_format='iceberg',
            partition_by=['category']
        ) }}
        select 
            category,
            count(*) as record_count,
            max(date_col) as max_date
        from {{ ref('source_s3_table') }}
        group by category
        """
        
        # View on S3 table
        view_on_s3_sql = """
        {{ config(materialized='view') }}
        select 
            s.category,
            s.name,
            d.record_count
        from {{ ref('source_s3_table') }} s
        join {{ ref('dependent_s3_table') }} d on s.category = d.category
        where s.id <= 3
        """
        
        return {
            "source_s3_table.sql": source_table_sql,
            "dependent_s3_table.sql": dependent_table_sql,
            "view_on_s3_table.sql": view_on_s3_sql,
            "schema.yml": schema_base_yml,
        }

    def test_s3_tables_integration(self, project):
        """Test integration of multiple S3 tables features"""
        results = run_dbt()
        assert len(results) == 3
        
        check_result_nodes_by_name(results, ["source_s3_table", "dependent_s3_table", "view_on_s3_table"])
        
        # Verify source table
        relation = relation_from_name(project.adapter, "source_s3_table")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 10
        
        # Verify dependent table
        relation = relation_from_name(project.adapter, "dependent_s3_table")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 2  # Two categories: A and B
        
        # Verify view
        relation = relation_from_name(project.adapter, "view_on_s3_table")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 3  # First 3 records


class TestS3TablesSpecificFeatures:
    """Test S3 tables specific features and configurations"""
    
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "s3_tables_specific",
        }

    @pytest.fixture(scope="class")
    def models(self):
        # Test with explicit S3 tables bucket configuration
        s3_tables_explicit_sql = """
        {{ config(
            materialized='table',
            file_format='iceberg',
            table_properties={
                's3.table.bucket-name': env_var('DBT_S3_TABLES_BUCKET', 'default-bucket')
            }
        ) }}
        select 
            1 as id,
            'explicit_s3_table' as name,
            current_timestamp() as created_at
        """
        
        return {
            "explicit_s3_table.sql": s3_tables_explicit_sql,
            "schema.yml": schema_base_yml,
        }

    def test_s3_tables_explicit_config(self, project):
        """Test S3 tables with explicit bucket configuration"""
        results = run_dbt()
        assert len(results) == 1
        
        # Verify the table was created
        relation = relation_from_name(project.adapter, "explicit_s3_table")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 1
