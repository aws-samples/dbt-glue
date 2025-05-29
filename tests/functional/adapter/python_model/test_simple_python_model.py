import pytest
from dbt.tests.util import run_dbt, check_relations_equal, relation_from_name

# Define the test class
class TestSimplePythonModel:
    
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "simple_python_model.py": """
def model(dbt, spark):
    # Configure the model
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
    df = spark.createDataFrame(data, columns)
    print("DEBUG: Created DataFrame with schema:", df.schema)
    
    # Print the DataFrame for debugging
    print("DEBUG: DataFrame content:")
    df.show()
    
    return df
"""
        }
    
    def test_simple_python_model(self, project):
        # Run the model
        results = run_dbt(["run"])
        
        # Check that the model ran successfully
        assert len(results) == 1
        assert results[0].status == "success"
        
        # Get the relation for the model
        relation = relation_from_name(project.adapter, "simple_python_model")
        
        # Query the model and check the results
        result = project.run_sql(f"SELECT * FROM {relation} ORDER BY id", fetch="all")
        
        # Print the results for debugging
        print("Query results:")
        for row in result:
            print(row)
        
        # Check that we have the expected number of rows
        assert len(result) == 4
        
        # Check the values in the first row
        assert result[0][0] == 1  # id
        assert result[0][1] == "Alice"  # name
        assert result[0][2] == 100  # value
