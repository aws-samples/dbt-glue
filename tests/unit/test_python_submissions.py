import pytest
from unittest import mock

from dbt.adapters.glue.python_submissions import GluePythonJobHelper
from dbt.adapters.glue.credentials import GlueCredentials

@pytest.fixture
def mock_credentials():
    return GlueCredentials(
        role_arn="arn:aws:iam::123456789012:role/GlueServiceRole",
        region="us-east-1",
        workers=2,
        worker_type="G.1X",
        schema="test_schema",
        session_provisioning_timeout_in_seconds=120,
        location="s3://test-bucket/test-location"
    )

@pytest.fixture
def mock_parsed_model():
    return {
        "alias": "test_model",
        "schema": "test_schema",
        "config": {
            "timeout": 3600
        }
    }

def test_glue_python_job_helper_init(mock_credentials, mock_parsed_model):
    helper = GluePythonJobHelper(mock_parsed_model, mock_credentials)
    
    assert helper.credentials == mock_credentials
    assert helper.identifier == "test_model"
    assert helper.schema == "test_schema"
    assert helper.timeout == 3600
    assert helper.polling_interval == 10

@mock.patch('dbt.adapters.glue.gluedbapi.connection.GlueConnection')
def test_glue_python_job_helper_submit(mock_glue_connection_class, mock_credentials, mock_parsed_model):
    # Setup mock GlueConnection instance
    mock_connection = mock.MagicMock()
    mock_glue_connection_class.return_value = mock_connection
    
    # Setup mock cursor
    mock_cursor = mock.MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    
    # Setup mock client and session_id
    mock_glue_client = mock.MagicMock()
    mock_connection.client = mock_glue_client
    mock_connection.session_id = 'test-session-id'
    
    # Mock statement execution
    mock_glue_client.run_statement.return_value = {
        'Id': 'test-statement-id'
    }
    
    # Mock statement completion
    mock_glue_client.get_statement.return_value = {
        'Statement': {
            'State': 'AVAILABLE',
            'Output': {
                'Status': 'OK'
            }
        }
    }
    
    # Create helper and submit code
    helper = GluePythonJobHelper(mock_parsed_model, mock_credentials)
    helper.submit("print('Hello, World!')")
    
    # Verify GlueConnection was created with correct credentials
    mock_glue_connection_class.assert_called_once_with(mock_credentials)
    
    # Verify cursor was called to establish connection
    mock_connection.cursor.assert_called_once()
    
    # Verify statement was executed
    mock_glue_client.run_statement.assert_called_once()
    run_statement_args = mock_glue_client.run_statement.call_args[1]
    assert run_statement_args['SessionId'] == 'test-session-id'
    assert "print('Hello, World!')" in run_statement_args['Code']

@mock.patch('dbt.adapters.glue.gluedbapi.connection.GlueConnection')
def test_glue_python_job_helper_with_packages(mock_glue_connection_class, mock_credentials):
    """Test that packages parameter is properly handled"""
    # Setup mock GlueConnection instance
    mock_connection = mock.MagicMock()
    mock_glue_connection_class.return_value = mock_connection
    
    # Setup mock cursor
    mock_cursor = mock.MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    
    # Setup mock client and session_id
    mock_glue_client = mock.MagicMock()
    mock_connection.client = mock_glue_client
    mock_connection.session_id = 'test-session-id'
    
    # Mock statement execution
    mock_glue_client.run_statement.return_value = {
        'Id': 'test-statement-id'
    }
    
    # Mock statement completion
    mock_glue_client.get_statement.return_value = {
        'Statement': {
            'State': 'AVAILABLE',
            'Output': {
                'Status': 'OK'
            }
        }
    }
    
    # Create parsed model with packages
    parsed_model_with_packages = {
        "alias": "test_model",
        "schema": "test_schema",
        "config": {
            "timeout": 3600,
            "packages": ["numpy", "pandas", "scikit-learn"]
        }
    }
    
    # Create helper and submit code
    helper = GluePythonJobHelper(parsed_model_with_packages, mock_credentials)
    helper.submit("import numpy as np; print('Hello with packages!')")
    
    # Verify GlueConnection was created with correct credentials
    mock_glue_connection_class.assert_called_once_with(mock_credentials)
    
    # Verify cursor was called to establish connection
    mock_connection.cursor.assert_called_once()
    
    # Verify statement was executed
    mock_glue_client.run_statement.assert_called_once()
    run_statement_args = mock_glue_client.run_statement.call_args[1]
    assert run_statement_args['SessionId'] == 'test-session-id'
    assert "import numpy as np; print('Hello with packages!')" in run_statement_args['Code']
    
    # Verify packages were extracted correctly
    assert helper.packages == ["numpy", "pandas", "scikit-learn"]

def test_glue_python_job_helper_packages_extraction(mock_credentials):
    """Test that packages are properly extracted from model config"""
    # Test with packages
    parsed_model_with_packages = {
        "alias": "test_model",
        "schema": "test_schema",
        "config": {
            "packages": ["numpy", "pandas"]
        }
    }
    
    helper = GluePythonJobHelper(parsed_model_with_packages, mock_credentials)
    assert helper.packages == ["numpy", "pandas"]
    
    # Test without packages
    parsed_model_without_packages = {
        "alias": "test_model",
        "schema": "test_schema",
        "config": {}
    }
    
    helper = GluePythonJobHelper(parsed_model_without_packages, mock_credentials)
    assert helper.packages == []
