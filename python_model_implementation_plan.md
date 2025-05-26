# Implementation Plan for Python Models in dbt-glue

## Overview

This document outlines the plan to add Python model support to the dbt-glue adapter. Python models allow users to define transformations using Python code instead of SQL, which can be useful for complex transformations that are difficult to express in SQL.

## Background

dbt-glue uses AWS Glue Interactive Sessions for all operations. To support Python models, we'll extend the existing Interactive Session functionality to handle Python code execution while maintaining the current session management and execution flow.

## Implementation Components

### 1. Adapter Implementation Updates

Update `dbt/adapters/glue/impl.py` to add Python model support:

- Add `execute_python` method to handle Python code execution
- Add DataFrame type handling and conversion utilities
- Add Python-specific configuration validation

```python
def execute_python(self, code: str, **kwargs) -> Any:
    """Execute Python code in Glue Interactive Session.
    
    Args:
        code: The Python code to execute
        **kwargs: Additional execution options
        
    Returns:
        The result of code execution
    """
    session = self.connections.get_thread_connection().handle
    return session.cursor().execute(code)
```

### 2. SQL Macros for Python Models

Add the following macros to `dbt/include/glue/macros/adapters.sql`:

- `glue__py_write_table`: Handles writing Python DataFrame output to a table
- `py_get_writer_options`: Configures writer options for the DataFrame
- `create_python_intermediate_table`: For incremental models

### 3. Configuration Options

Update `dbt/adapters/glue/credentials.py` to add Python-specific configuration options:

```python
@dataclass
class GlueCredentials(Credentials):
    # Existing fields...
    
    # Python model specific fields
    packages: Optional[list] = None
    additional_libs: Optional[list] = None
```

### 4. Testing

Create functional tests in `tests/functional/adapter/python_model/` to verify:
- Basic Python model execution
- Different DataFrame types (pandas, PySpark)
- Incremental Python models
- Package installation

## Action Items

1. **Update Adapter Implementation**
   - Add Python code execution support to GlueAdapter
   - Implement DataFrame conversion and table writing logic

2. **Add SQL Macros**
   - Create macros for Python model materialization
   - Support different DataFrame types and writer options

3. **Update Configuration**
   - Add Python-specific configuration options to credentials
   - Document new configuration options

4. **Testing**
   - Create functional tests for Python models
   - Test with different DataFrame types and configurations

5. **Documentation**
   - Update README with Python model usage examples
   - Document configuration options and limitations

## Implementation Details

### Python Model Execution Process

1. User defines a Python model with a `model()` function
2. dbt compiles the model and adds boilerplate code
3. The adapter executes the code in the existing Glue Interactive Session
4. The code executes and returns a DataFrame
5. The DataFrame is written to the target table

### DataFrame Type Support

Support the following DataFrame types:
- PySpark DataFrame (primary/default)
- pandas DataFrame (converted to PySpark)
- pandas-on-Spark DataFrame (converted to PySpark)

### Package Management

Allow users to specify Python packages to be installed in the Glue environment:

```python
def model(dbt, spark):
    dbt.config(
        materialized='table',
        packages=['numpy', 'scikit-learn']
    )
    # Model code...
```

## Limitations and Considerations

1. **Performance**: Python models may be slower than SQL models for large datasets
2. **Package Installation**: Package installation may add overhead to job startup
3. **Memory Constraints**: Complex Python transformations may require more memory
4. **Session Management**: Need to handle session timeouts and failures gracefully

## Timeline

1. Initial implementation: 1-2 weeks
2. Testing and refinement: 1 week
3. Documentation and examples: 1 week
