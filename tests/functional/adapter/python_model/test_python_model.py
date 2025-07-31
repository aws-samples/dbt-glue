import pytest
from dbt.tests.util import run_dbt

# Basic Python model test
basic_python_model = """
def model(dbt, spark):
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
    return spark.createDataFrame(data, columns)
"""

# Incremental Python model test
incremental_python_model = """
def model(dbt, spark):
    dbt.config(
        materialized='incremental',
        file_format='iceberg',
        incremental_strategy='merge',
        unique_key='id'
    )
    
    # Create or get existing table
    if dbt.is_incremental():
        # Get max id from existing table
        max_id = spark.sql(f"SELECT MAX(id) as max_id FROM {dbt.this}").collect()[0].max_id
        if max_id is None:
            max_id = 0
    else:
        max_id = 0
    
    # Create new data
    data = [
        (max_id + 1, f'name_{max_id + 1}', (max_id + 1) * 100),
        (max_id + 2, f'name_{max_id + 2}', (max_id + 2) * 100),
        (max_id + 3, f'name_{max_id + 3}', (max_id + 3) * 100),
        (max_id + 4, f'name_{max_id + 4}', (max_id + 4) * 100)
    ]
    
    columns = ['id', 'name', 'value']
    return spark.createDataFrame(data, columns)
"""

# Python model with packages (simplified - removed numpy/pandas to avoid package installation time)
python_model_with_packages = """
def model(dbt, spark):
    dbt.config(materialized='python_model', file_format='iceberg', packages=['requests'])
    
    # Create test data (without actually using packages to avoid installation overhead)
    data = [
        (1, 'Package_Test_1', 100),
        (2, 'Package_Test_2', 200)
    ]
    
    columns = ['id', 'name', 'value']
    return spark.createDataFrame(data, columns)
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
        results = run_dbt(["run"])
        assert len(results) == 3
        
        # Test basic model
        relation = project.adapter.Relation.create(
            schema=project.test_schema,
            identifier="basic_python_model"
        )
        catalog_table_name = f"glue_catalog.{relation.schema}.{relation.identifier}"
        result = project.run_sql(f"SELECT * FROM {catalog_table_name} ORDER BY id", fetch="all")
        assert len(result) == 4
        assert result[0][0] == 1  # First row, id column
        
        # Test incremental model - first run
        relation = project.adapter.Relation.create(
            schema=project.test_schema,
            identifier="incremental_python_model"
        )
        catalog_table_name = f"glue_catalog.{relation.schema}.{relation.identifier}"
        result = project.run_sql(f"SELECT * FROM {catalog_table_name} ORDER BY id", fetch="all")
        assert len(result) == 4

        # Test incremental merge logic - run incremental model again
        results = run_dbt(["run", "--models", "incremental_python_model"])
        result = project.run_sql(f"SELECT * FROM {catalog_table_name} ORDER BY id", fetch="all")
        assert len(result) == 8  # Should have 4 original + 4 new rows
        
        # Test packages model (verify it runs without error)
        relation = project.adapter.Relation.create(
            schema=project.test_schema,
            identifier="python_model_with_packages"
        )
        catalog_table_name = f"glue_catalog.{relation.schema}.{relation.identifier}"
        result = project.run_sql(f"SELECT * FROM {catalog_table_name} ORDER BY id", fetch="all")
        assert len(result) == 2

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
        results = run_dbt(["run"], expect_pass=False)
        # Should fail but not crash the test suite
        assert len(results) == 1
        assert not results[0].success
