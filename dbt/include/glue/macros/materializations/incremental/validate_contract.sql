{% macro validate_incremental_contract(tmp_relation, target_relation) %}
  {#-- Validate that SQL columns match the contract during incremental runs.
       This checks both column names AND data types, consistent with how
       dbt-core validates contracts for table/view materializations via
       get_assert_columns_equivalent(). --#}

  {% set contract_config = config.get('contract') %}
  {% if not contract_config or not contract_config.enforced %}
    {{ return('') }}
  {% endif %}

  {# Use the relation without schema if it's a view (temp views are session-scoped) #}
  {% set tmp_relation_ref = tmp_relation.include(schema=(tmp_relation.type != 'view')) %}

  {# Delegate to dbt-core's contract validation which checks both names and data types.
     It compares the column schema from the SQL query against the YAML contract definition,
     using the adapter's data_type_code_to_name for type normalization. #}
  {% set tmp_sql %}
    SELECT * FROM {{ tmp_relation_ref }}
  {% endset %}
  {{ get_assert_columns_equivalent(tmp_sql) }}

{% endmacro %}
