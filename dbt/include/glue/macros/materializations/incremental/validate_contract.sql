{% macro validate_incremental_contract(tmp_relation, target_relation) %}
  {#-- Validate that SQL columns match the contract during incremental runs --#}

  {% set contract_config = config.get('contract') %}
  {% if not contract_config or not contract_config.enforced %}
    {# Contract not enforced, skip validation #}
    {{ return('') }}
  {% endif %}

  {# Get columns from the SQL by querying the temp relation #}
  {# Use the relation without schema if it's a view (temp views are session-scoped) #}
  {% set tmp_relation_ref = tmp_relation.include(schema=(tmp_relation.type != 'view')) %}

  {% set tmp_columns_query %}
    SELECT * FROM {{ tmp_relation_ref }} WHERE 1=0
  {% endset %}

  {%- set tmp_columns_result = run_query(tmp_columns_query) -%}
  {% set tmp_columns = tmp_columns_result.columns %}

  {# Get contract columns from model columns definition #}
  {% set contract_columns = model.columns %}

  {# Build maps for comparison #}
  {% set tmp_column_names = [] %}
  {% for col in tmp_columns %}
    {# Handle both agate columns (from run_query) and GlueColumn objects #}
    {% set col_name = col.name if col.name is defined else col %}
    {% do tmp_column_names.append(col_name | lower) %}
  {% endfor %}

  {% set contract_column_names = [] %}
  {% if contract_columns %}
    {% for col_name in contract_columns.keys() %}
      {% do contract_column_names.append(col_name | lower) %}
    {% endfor %}
  {% endif %}

  {# Find columns in SQL but not in contract #}
  {% set extra_columns = [] %}
  {% for col_name in tmp_column_names %}
    {% if col_name not in contract_column_names %}
      {% do extra_columns.append(col_name) %}
    {% endif %}
  {% endfor %}

  {# Find columns in contract but not in SQL #}
  {% set missing_columns = [] %}
  {% for col_name in contract_column_names %}
    {% if col_name not in tmp_column_names %}
      {% do missing_columns.append(col_name) %}
    {% endif %}
  {% endfor %}

  {# Raise error if there are mismatches - use same format as dbt core validation #}
  {% if extra_columns | length > 0 or missing_columns | length > 0 %}

    {# Build a table of mismatches like dbt core does #}
    {% set mismatch_rows = [] %}

    {# Add rows for extra columns (in SQL but not in contract) #}
    {% for col_name in extra_columns %}
      {# Find the actual column type from tmp_columns #}
      {% set col_type = namespace(value='') %}
      {% for col in tmp_columns %}
        {% set this_col_name = col.name if col.name is defined else col %}
        {% if this_col_name | lower == col_name %}
          {# Get type name - extract from string representation #}
          {% if col.data_type is defined %}
            {% if col.data_type is string %}
              {% set col_type.value = col.data_type | upper %}
            {% else %}
              {# Convert to string and extract type name between angle brackets #}
              {% set type_str = col.data_type | string %}
              {% if 'Integer' in type_str %}
                {% set col_type.value = 'INTEGER' %}
              {% elif 'Text' in type_str or 'String' in type_str %}
                {% set col_type.value = 'STRING' %}
              {% elif 'Boolean' in type_str %}
                {% set col_type.value = 'BOOLEAN' %}
              {% elif 'Date' in type_str %}
                {% set col_type.value = 'DATE' %}
              {% elif 'DateTime' in type_str or 'Timestamp' in type_str %}
                {% set col_type.value = 'TIMESTAMP' %}
              {% elif 'Number' in type_str or 'Decimal' in type_str %}
                {% set col_type.value = 'DECIMAL' %}
              {% else %}
                {% set col_type.value = 'UNKNOWN' %}
              {% endif %}
            {% endif %}
          {% else %}
            {% set col_type.value = 'UNKNOWN' %}
          {% endif %}
        {% endif %}
      {% endfor %}
      {% do mismatch_rows.append({'column_name': col_name, 'definition_type': col_type.value, 'contract_type': '', 'mismatch_reason': 'missing in contract'}) %}
    {% endfor %}

    {# Add rows for missing columns (in contract but not in SQL) #}
    {% for col_name in missing_columns %}
      {# Find the contract type #}
      {% set contract_type = contract_columns[col_name].data_type if contract_columns and col_name in contract_columns else '' %}
      {% do mismatch_rows.append({'column_name': col_name, 'definition_type': '', 'contract_type': contract_type, 'mismatch_reason': 'missing in definition'}) %}
    {% endfor %}

    {# Calculate column widths based on content using namespace for scoping #}
    {% set widths = namespace(
      col_name = 11,
      def_type = 15,
      contract_type = 13,
      reason = 19
    ) %}

    {% for row in mismatch_rows %}
      {% if row.column_name | length > widths.col_name %}
        {% set widths.col_name = row.column_name | length %}
      {% endif %}
      {% if row.definition_type | length > widths.def_type %}
        {% set widths.def_type = row.definition_type | length %}
      {% endif %}
      {% if row.contract_type | length > widths.contract_type %}
        {% set widths.contract_type = row.contract_type | length %}
      {% endif %}
      {% if row.mismatch_reason | length > widths.reason %}
        {% set widths.reason = row.mismatch_reason | length %}
      {% endif %}
    {% endfor %}

    {% set error_msg %}
This model has an enforced contract that failed.
Please ensure the name, data_type, and number of columns in your contract match the columns in your model's definition.

| {{ "%-{0}s".format(widths.col_name) | format("column_name") }} | {{ "%-{0}s".format(widths.def_type) | format("definition_type") }} | {{ "%-{0}s".format(widths.contract_type) | format("contract_type") }} | {{ "%-{0}s".format(widths.reason) | format("mismatch_reason") }} |
| {{ "-" * widths.col_name }} | {{ "-" * widths.def_type }} | {{ "-" * widths.contract_type }} | {{ "-" * widths.reason }} |
{%- for row in mismatch_rows %}
| {{ "%-{0}s".format(widths.col_name) | format(row.column_name) }} | {{ "%-{0}s".format(widths.def_type) | format(row.definition_type) }} | {{ "%-{0}s".format(widths.contract_type) | format(row.contract_type) }} | {{ "%-{0}s".format(widths.reason) | format(row.mismatch_reason) }} |
{%- endfor %}

    {% endset %}

    {{ exceptions.raise_compiler_error(error_msg) }}
  {% endif %}

  {# TODO: Future enhancement - validate data types too #}
  {# {% if contract_config.alias %} #}
  {#   Add data type validation if needed #}
  {# {% endif %} #}

{% endmacro %}
