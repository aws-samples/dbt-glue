{% macro glue__py_script_comment() %}
# This code is executed on AWS Glue
# You can access Glue context via the 'glueContext' variable
# and SparkSession via the 'spark' variable

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# Initialize Glue context
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)

def load_df(table_name):
    """Load a table as a Spark DataFrame"""
    return spark.table(table_name)

# Make dbt object available with proper loading function
dbt = dbtObj(load_df)
{% endmacro %}
