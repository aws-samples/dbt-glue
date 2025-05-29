import time
import uuid
from typing import Any, Dict

from dbt.adapters.base import PythonJobHelper
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.glue import GlueCredentials

class GluePythonJobHelper(PythonJobHelper):
    def __init__(self, parsed_model: Dict, credentials: GlueCredentials) -> None:
        self.credentials = credentials
        self.identifier = parsed_model["alias"]
        self.schema = parsed_model["schema"]
        self.parsed_model = parsed_model
        self.timeout = self.parsed_model["config"].get("timeout", 60 * 60)  # Default 1 hour
        self.polling_interval = 10
        
    def submit(self, compiled_code: str) -> None:
        """Submit Python code to AWS Glue for execution"""
        import boto3
        
        # Create a Glue session client
        glue_client = boto3.client(
            'glue',
            region_name=self.credentials.region
        )
        
        # Create a session
        session_id = f"dbt-python-{uuid.uuid4()}"
        
        # Use existing configuration from credentials
        workers = self.credentials.workers
        worker_type = self.credentials.worker_type
        
        # Start the session
        response = glue_client.create_session(
            Id=session_id,
            Role=self.credentials.role_arn,
            Command={
                'Name': 'glueetl',
                'PythonVersion': '3'
            },
            DefaultArguments={
                '--enable-glue-datacatalog': 'true'
            },
            Timeout=self.timeout,
            NumberOfWorkers=workers,
            WorkerType=worker_type
        )
        
        try:
            # Wait for session to be ready
            self._wait_for_session_ready(glue_client, session_id)
            
            # Add debugging code to check databases and tables
            debug_code = """
print("DEBUG: Running diagnostic queries...")
print("DEBUG: Available databases:")
databases_df = spark.sql("SHOW DATABASES")
databases_df.show(truncate=False)

print(f"DEBUG: Current database: {spark.catalog.currentDatabase()}")

print(f"DEBUG: Setting database to {schema}")
spark.sql(f"USE {schema}")

print("DEBUG: Tables in current database:")
tables_df = spark.sql("SHOW TABLES")
tables_df.show(truncate=False)
"""
            
            # Insert schema information into debug code
            debug_code = debug_code.replace("{schema}", self.schema)
            
            # Run the debug code first
            debug_statement_id = self._run_statement(glue_client, session_id, debug_code)
            self._wait_for_statement_completion(glue_client, session_id, debug_statement_id)
            
            # Run the actual Python code
            statement_id = self._run_statement(glue_client, session_id, compiled_code)
            
            # Wait for completion
            self._wait_for_statement_completion(glue_client, session_id, statement_id)
            
            # Run another debug query to see if the table was created
            post_debug_code = f"""
print("DEBUG: After model execution - Tables in {self.schema}:")
tables_df = spark.sql("SHOW TABLES")
tables_df.show(truncate=False)

print("DEBUG: Attempting to describe the created table:")
try:
    desc_df = spark.sql("DESCRIBE {self.schema}.{self.identifier}")
    desc_df.show(truncate=False)
except Exception as e:
    print(f"DEBUG: Error describing table: {{e}}")
    
# Try to refresh the table
try:
    print("DEBUG: Attempting to refresh the table...")
    spark.sql("REFRESH TABLE {self.schema}.{self.identifier}")
    print("DEBUG: Table refreshed successfully")
except Exception as e:
    print(f"DEBUG: Error refreshing table: {{e}}")
"""
            
            post_debug_statement_id = self._run_statement(glue_client, session_id, post_debug_code)
            self._wait_for_statement_completion(glue_client, session_id, post_debug_statement_id)
            
        finally:
            # Clean up the session
            try:
                glue_client.delete_session(Id=session_id)
            except Exception:
                pass
    
    def _wait_for_session_ready(self, glue_client, session_id):
        """Wait for the Glue session to be ready"""
        start_time = time.time()
        while time.time() - start_time < self.timeout:
            response = glue_client.get_session(Id=session_id)
            status = response['Session']['Status']
            
            if status == 'READY':
                return
            elif status in ('FAILED', 'TIMEOUT', 'STOPPING', 'STOPPED'):
                raise DbtRuntimeError(f"Glue session failed with status: {status}")
                
            time.sleep(self.polling_interval)
            
        raise DbtRuntimeError("Timed out waiting for Glue session to be ready")
    
    def _run_statement(self, glue_client, session_id, code):
        """Run a Python statement in the Glue session"""
        response = glue_client.run_statement(
            SessionId=session_id,
            Code=code
        )
        return response['Id']
    
    def _wait_for_statement_completion(self, glue_client, session_id, statement_id):
        """Wait for a statement to complete execution"""
        start_time = time.time()
        while time.time() - start_time < self.timeout:
            response = glue_client.get_statement(
                SessionId=session_id,
                Id=statement_id
            )
            state = response['Statement']['State']
            
            if state == 'AVAILABLE':
                # Check for errors
                output = response['Statement'].get('Output', {})
                if output.get('Status') == 'ERROR':
                    error_message = output.get('ErrorName', '')
                    error_value = output.get('ErrorValue', '')
                    traceback = output.get('Traceback', '')
                    
                    # Print the full output for debugging
                    print(f"DEBUG: Statement output: {output}")
                    
                    raise DbtRuntimeError(
                        f"Python model failed with error: {error_message}\n{error_value}\n{traceback}"
                    )
                
                # Print the output for debugging
                print(f"DEBUG: Statement completed successfully. Output: {output}")
                return
            elif state in ('CANCELLED', 'ERROR'):
                raise DbtRuntimeError(f"Statement execution failed with state: {state}")
                
            time.sleep(self.polling_interval)
            
        raise DbtRuntimeError("Timed out waiting for statement to complete")
