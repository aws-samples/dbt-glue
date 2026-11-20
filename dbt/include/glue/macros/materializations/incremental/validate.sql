{% macro dbt_glue_validate_get_file_format(raw_file_format) %}
  {#-- Validate the file format #}

  {% set accepted_formats = ['text', 'csv', 'json', 'jdbc', 'parquet', 'orc', 'hive', 'delta', 'iceberg', 'libsvm', 'hudi', 's3tables'] %}

  {% set invalid_file_format_msg -%}
    Invalid file format provided: {{ raw_file_format }}
    Expected one of: {{ accepted_formats | join(', ') }}
  {%- endset %}

  {% if raw_file_format not in accepted_formats %}
    {% do exceptions.raise_compiler_error(invalid_file_format_msg) %}
  {% endif %}

  {% do return(raw_file_format) %}
{% endmacro %}


{% macro dbt_glue_validate_get_incremental_strategy(raw_strategy, file_format) %}
  {#-- Validate the incremental strategy #}

  {% set invalid_strategy_msg -%}
    Invalid incremental strategy provided: {{ raw_strategy }}
    Expected one of: 'append', 'merge', 'insert_overwrite'
  {%- endset %}

  {% set invalid_merge_msg -%}
    Invalid incremental strategy provided: {{ raw_strategy }}
    You can only choose this strategy when file_format is set to 'delta' or 'iceberg' or 'hudi' or 's3tables'
  {%- endset %}

  {% set invalid_insert_overwrite_endpoint_msg -%}
    Invalid incremental strategy provided: {{ raw_strategy }}
    You cannot use this strategy when connecting via endpoint
    Use the 'append' or 'merge' strategy instead
  {%- endset %}

  {% if raw_strategy not in ['append', 'merge', 'insert_overwrite'] %}
    {% do exceptions.raise_compiler_error(invalid_strategy_msg) %}
  {%-else %}
    {% if raw_strategy == 'merge' and file_format not in ['delta', 'iceberg', 'hudi', 's3tables'] %}
      {% do exceptions.raise_compiler_error(invalid_merge_msg) %}
    {% endif %}
    {% if raw_strategy == 'insert_overwrite' and target.endpoint %}
      {% do exceptions.raise_compiler_error(invalid_insert_overwrite_endpoint_msg) %}
    {% endif %}
  {% endif %}

  {% do return(raw_strategy) %}
{% endmacro %}


{% macro dbt_glue_validate_contract_with_schema_change(on_schema_change) %}
  {#-- Validate that contract enforcement is compatible with on_schema_change setting #}

  {% set contract_config = config.get('contract') %}
  {% set contract_enforced = contract_config.enforced if contract_config else false %}

  {% if contract_enforced and on_schema_change == 'ignore' %}
    {% set invalid_contract_schema_change_msg -%}
      Invalid value for on_schema_change: {{ on_schema_change }}.
      Models materialized as incremental with contracts enabled must set on_schema_change to 'append_new_columns' or 'fail'.
    {%- endset %}
    {% do exceptions.raise_compiler_error(invalid_contract_schema_change_msg) %}
  {% endif %}
{% endmacro %}
