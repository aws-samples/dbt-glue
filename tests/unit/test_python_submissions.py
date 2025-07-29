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

@mock.patch('boto3.client')
def test_glue_python_job_helper_submit(mock_boto3_client, mock_credentials, mock_parsed_model):
    # Setup mock responses
    mock_glue_client = mock.MagicMock()
    mock_boto3_client.return_value = mock_glue_client
    
    # Mock session creation
    mock_glue_client.create_session.return_value = {
        'SessionId': 'test-session-id'
    }
    
    # Mock session status
    mock_glue_client.get_session.return_value = {
        'Session': {
            'Status': 'READY'
        }
    }
    
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
    
    # Verify boto3 client was created with correct parameters
    mock_boto3_client.assert_called_once_with('glue', region_name='us-east-1')
    
    # Verify session was created
    mock_glue_client.create_session.assert_called_once()
    create_session_args = mock_glue_client.create_session.call_args[1]
    assert create_session_args['Role'] == "arn:aws:iam::123456789012:role/GlueServiceRole"
    assert create_session_args['NumberOfWorkers'] == 2
    assert create_session_args['WorkerType'] == "G.1X"
    
    # Verify statement was executed (now called 3 times: debug, actual code, post-debug)
    assert mock_glue_client.run_statement.call_count == 3
    
    # Verify the actual model code is in the second call
    run_statement_calls = mock_glue_client.run_statement.call_args_list
    actual_code_call = run_statement_calls[1]  # Second call contains the actual model code
    actual_code_args = actual_code_call[1]  # Get keyword arguments
    assert "print('Hello, World!')" in actual_code_args['Code']
    
    # Verify session was deleted
    mock_glue_client.delete_session.assert_called_once()

@mock.patch('boto3.client')
def test_glue_python_job_helper_with_packages(mock_boto3_client, mock_credentials):
    """Test that packages parameter is properly handled"""
    # Setup mock responses
    mock_glue_client = mock.MagicMock()
    mock_boto3_client.return_value = mock_glue_client
    
    # Mock session creation
    mock_glue_client.create_session.return_value = {
        'SessionId': 'test-session-id'
    }
    
    # Mock session status
    mock_glue_client.get_session.return_value = {
        'Session': {
            'Status': 'READY'
        }
    }
    
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
    
    # Verify session was created with packages
    mock_glue_client.create_session.assert_called_once()
    create_session_args = mock_glue_client.create_session.call_args[1]
    
    # Check that packages were added to DefaultArguments
    default_args = create_session_args['DefaultArguments']
    assert '--additional-python-modules' in default_args
    assert default_args['--additional-python-modules'] == 'numpy,pandas,scikit-learn'
    
    # Verify session was deleted
    mock_glue_client.delete_session.assert_called_once()

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
