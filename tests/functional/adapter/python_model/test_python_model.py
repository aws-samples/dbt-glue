import pytest
from dbt.tests.util import run_dbt

# Basic Python model test - simplified for debugging
basic_python_model = """
def model(dbt, spark):
    print("DEBUG: Starting basic Python model")
    dbt.config(materialized='python_model', file_format='iceberg')
    
    # Create a simple DataFrame
    data = [
        (1, 'Alice', 100),
        (2, 'Bob', 200),
        (3, 'Charlie', 300),
        (4, 'David', 400)
    ]
    
    # Define the schema
    columns = ['id', 'name', 'value']
    
    # Create and return a Spark DataFrame
    print("DEBUG: Creating Spark DataFrame")
    df = spark.createDataFrame(data, columns)
    print(f"DEBUG: DataFrame created with {df.count()} rows")
    return df
"""

# Incremental Python model test - simplified
incremental_python_model = """
def model(dbt, spark):
    print("DEBUG: Starting incremental Python model")
    dbt.config(
        materialized='incremental',
        file_format='iceberg',
        incremental_strategy='merge',
        unique_key='id'
    )
    
    # Create test data
    if dbt.is_incremental():
        print("DEBUG: This is an incremental run")
        # Add new data for incremental run
        data = [
            (5, 'Eve', 500),
            (6, 'Frank', 600),
            (7, 'Grace', 700),
            (8, 'Henry', 800)
        ]
    else:
        print("DEBUG: This is a full refresh run")
        # Initial data
        data = [
            (1, 'Alice', 100),
            (2, 'Bob', 200),
            (3, 'Charlie', 300),
            (4, 'David', 400)
        ]
    
    columns = ['id', 'name', 'value']
    df = spark.createDataFrame(data, columns)
    print(f"DEBUG: DataFrame created with {df.count()} rows")
    return df
"""

# Simple packages test - no actual packages to avoid complexity
python_model_with_packages = """
def model(dbt, spark):
    print("DEBUG: Starting packages test model")
    dbt.config(materialized='python_model', file_format='iceberg')
    
    # Create test data (without actually using packages to avoid installation overhead)
    data = [
        (1, 'Package_Test_1', 100),
        (2, 'Package_Test_2', 200)
    ]
    
    columns = ['id', 'name', 'value']
    df = spark.createDataFrame(data, columns)
    print(f"DEBUG: DataFrame created with {df.count()} rows")
    return df
"""

@pytest.fixture(scope="class")
def unique_schema(request, prefix):
    test_file = request.module.__name__.split('.')[-1]
    return f"{prefix}_{test_file}_{request.cls.__name__}".lower()

class TestPythonModelConsolidated:
    """Consolidated Python model tests to reduce execution time"""
    
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "basic_python_model.py": basic_python_model,
            "incremental_python_model.py": incremental_python_model,
            "python_model_with_packages.py": python_model_with_packages,
        }
    
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+file_format": "iceberg"
            }
        }
    
    def test_python_models_comprehensive(self, project):
        """Test all Python model functionality in a single test to reduce execution time"""
        
        # Run all models in one go
        print("DEBUG: Running all Python models")
        results = run_dbt(["run"])
        assert len(results) == 3
        print("DEBUG: All models ran successfully")
        
        # Test basic model with more robust error handling
        try:
            relation = project.adapter.Relation.create(
                schema=project.test_schema,
                identifier="basic_python_model"
            )
            
            # Try different query approaches to handle catalog issues
            try:
                # First try with catalog prefix (Iceberg standard)
                catalog_table_name = f"glue_catalog.{relation.schema}.{relation.identifier}"
                print(f"DEBUG: Querying with catalog prefix: {catalog_table_name}")
                result = project.run_sql(f"SELECT * FROM {catalog_table_name} ORDER BY id", fetch="all")
            except Exception as e:
                print(f"DEBUG: Catalog prefix failed: {e}")
                # Fallback to direct table name
                table_name = f"{relation.schema}.{relation.identifier}"
                print(f"DEBUG: Querying without catalog prefix: {table_name}")
                result = project.run_sql(f"SELECT * FROM {table_name} ORDER BY id", fetch="all")
            
            assert len(result) == 4
            assert result[0][0] == 1  # First row, id column
            print("DEBUG: Basic model test passed")
            
        except Exception as e:
            print(f"DEBUG: Basic model test failed: {e}")
            # Print available tables for debugging
            try:
                tables = project.run_sql("SHOW TABLES", fetch="all")
                print("DEBUG: Available tables:", tables)
            except:
                pass
            raise e

        # Test incremental model - first run
        try:
            relation = project.adapter.Relation.create(
                schema=project.test_schema,
                identifier="incremental_python_model"
            )
            
            # Use same query approach as basic model
            try:
                catalog_table_name = f"glue_catalog.{relation.schema}.{relation.identifier}"
                result = project.run_sql(f"SELECT * FROM {catalog_table_name} ORDER BY id", fetch="all")
            except:
                table_name = f"{relation.schema}.{relation.identifier}"
                result = project.run_sql(f"SELECT * FROM {table_name} ORDER BY id", fetch="all")
            
            assert len(result) == 4
            print("DEBUG: Incremental model first run test passed")

            # Test incremental merge logic - run incremental model again
            print("DEBUG: Running incremental model second time")
            results = run_dbt(["run", "--models", "incremental_python_model"])
            
            # Query again to check merge results
            try:
                result = project.run_sql(f"SELECT * FROM {catalog_table_name} ORDER BY id", fetch="all")
            except:
                result = project.run_sql(f"SELECT * FROM {table_name} ORDER BY id", fetch="all")
            
            assert len(result) == 8  # Should have 4 original + 4 new rows
            print("DEBUG: Incremental model merge test passed")
            
        except Exception as e:
            print(f"DEBUG: Incremental model test failed: {e}")
            raise e
        
        # Test packages model (verify it runs without error)
        try:
            relation = project.adapter.Relation.create(
                schema=project.test_schema,
                identifier="python_model_with_packages"
            )
            
            try:
                catalog_table_name = f"glue_catalog.{relation.schema}.{relation.identifier}"
                result = project.run_sql(f"SELECT * FROM {catalog_table_name} ORDER BY id", fetch="all")
            except:
                table_name = f"{relation.schema}.{relation.identifier}"
                result = project.run_sql(f"SELECT * FROM {table_name} ORDER BY id", fetch="all")
            
            assert len(result) == 2
            print("DEBUG: Packages model test passed")
            
        except Exception as e:
            print(f"DEBUG: Packages model test failed: {e}")
            raise e

# Keep a minimal error test separate since it's expected to fail
class TestPythonModelErrors:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "invalid_python_model.py": """
def model(dbt, spark):
    dbt.config(materialized='python_model', file_format='iceberg')
    # This should raise an error
    raise ValueError("Test error for validation")
"""
        }
    
    def test_python_model_error(self, project):
        """Test that invalid Python models fail appropriately"""
        print("DEBUG: Testing error handling")
        results = run_dbt(["run"], expect_pass=False)
        # Should fail but not crash the test suite
        assert len(results) == 1
        assert not results[0].success
        print("DEBUG: Error handling test passed")
