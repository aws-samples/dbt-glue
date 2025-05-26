def model(dbt, spark):
    """
    An incremental Python model that adds new data on each run.
    
    Returns:
        DataFrame: A Spark DataFrame with new records
    """
    dbt.config(
        materialized='incremental',
        unique_key='id'
    )
    
    # Get current max id if table exists
    if dbt.is_incremental():
        max_id = spark.sql(f"SELECT MAX(id) as max_id FROM {dbt.this}").collect()[0].max_id
        if max_id is None:
            max_id = 0
    else:
        max_id = 0
    
    # Generate new data starting from max_id + 1
    data = [
        (i, f'name_{i}', i * 100) 
        for i in range(max_id + 1, max_id + 5)
    ]
    
    # Define the schema
    columns = ['id', 'name', 'value']
    
    # Create and return a Spark DataFrame
    return spark.createDataFrame(data, columns)
