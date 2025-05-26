def model(dbt, spark):
    """
    A simple Python model that creates a DataFrame with sample data.
    
    Returns:
        DataFrame: A Spark DataFrame with id, name, and value columns
    """
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
