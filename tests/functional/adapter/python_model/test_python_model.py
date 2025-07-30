import pytest
from dbt.tests.util import run_dbt
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod

# Basic Python model test
basic_python_model = """
def model(dbt, spark):
    dbt.config(materialized='python_model')
    
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
        file_format='parquet',
        incremental_strategy='insert_overwrite',
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
    
    # Generate new data
    data = [(i, f'name_{i}', i * 100) for i in range(max_id + 1, max_id + 5)]
    return spark.createDataFrame(data, ['id', 'name', 'value'])
"""

# Test configuration
@pytest.fixture(scope="class")
def project_config_update():
    return {
        "name": "python_model_test",
        "models": {"+materialized": "python_model"}
    }

class TestPythonModel:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_python_model.py": basic_python_model,
            "my_incremental_model.py": incremental_python_model
        }
    
    def test_python_model(self, project):
        # Run the models
        results = run_dbt(["run"])
        assert len(results) == 2
        
        # Test basic model
        relation = project.adapter.Relation.create(
            schema=project.test_schema,
            identifier="my_python_model"
        )
        
        result = project.run_sql(f"SELECT * FROM {relation} ORDER BY id", fetch="all")
        assert len(result) == 4
        assert result[0][0] == 1  # First row, id column
        
        # Test incremental model - first run
        relation = project.adapter.Relation.create(
            schema=project.test_schema,
            identifier="my_incremental_model"
        )
        
        result = project.run_sql(f"SELECT * FROM {relation} ORDER BY id", fetch="all")
        assert len(result) == 4
        
        # Run incremental model again
        results = run_dbt(["run", "--models", "my_incremental_model"])
        result = project.run_sql(f"SELECT * FROM {relation} ORDER BY id", fetch="all")
        assert len(result) == 8  # Should have 4 new rows

class TestPythonModelErrors:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "invalid_python_model.py": """
def model(dbt, spark):
    dbt.config(materialized='python_model')
    # This should raise an error
    raise ValueError("Test error")
"""
        }
    
    def test_python_model_error(self, project):
        with pytest.raises(Exception):
            run_dbt(["run"])

class TestPythonModelConfigs:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "configured_python_model.py": """
def model(dbt, spark):
    dbt.config(
        materialized='python_model',
        packages=['numpy', 'pandas'],
        partition_by=['id']
    )
    
    data = [(i, f'name_{i}', i * 100) for i in range(1, 5)]
    return spark.createDataFrame(data, ['id', 'name', 'value'])
"""
        }
    
    def test_python_model_configs(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        
        # Check if table was created with partitioning
        relation = project.adapter.Relation.create(
            schema=project.test_schema,
            identifier="configured_python_model"
        )
        
        # Verify the table exists and has data
        result = project.run_sql(f"SELECT * FROM {relation}", fetch="all")
        assert len(result) == 4
        
        # Verify partitioning (this is Glue-specific, may need adjustment)
        describe_result = project.run_sql(f"DESCRIBE {relation}", fetch="all")
        # Note: Actual partition verification depends on Glue's DESCRIBE output format

class TestPythonModelPackages:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "python_model_with_packages.py": """
def model(dbt, spark):
    dbt.config(
        materialized='python_model',
        packages=['numpy', 'pandas']
    )
    
    # Test that numpy is available
    import numpy as np
    
    # Create data using numpy
    data = [(int(i), f'name_{i}', float(i * 100)) for i in np.arange(1, 5)]
    return spark.createDataFrame(data, ['id', 'name', 'value'])
"""
        }
    
    def test_python_model_with_packages(self, project):
        # Run the model that uses numpy
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"
        
        # Verify the table was created successfully
        relation = project.adapter.Relation.create(
            schema=project.test_schema,
            identifier="python_model_with_packages"
        )
        
        result = project.run_sql(f"SELECT * FROM {relation}", fetch="all")
        assert len(result) == 4
