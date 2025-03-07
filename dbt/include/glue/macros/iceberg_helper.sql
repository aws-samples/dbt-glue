{% macro add_iceberg_timestamp_column(sql, relation=none) %}
  {%- set add_iceberg_timestamp = config.get('add_iceberg_timestamp', false) -%}
  {%- set file_format = config.get('file_format', default='parquet') -%}

  {% if add_iceberg_timestamp and file_format == 'iceberg' %}
    {{ log("DEBUG - Adding Iceberg timestamp column", info=true) }}

    {% set query_sql %}
      SELECT * FROM ({{ sql }}) AS _dbt_temp LIMIT 0
    {% endset %}

    {% set results = run_query(query_sql) %}
    {% set column_names = results.column_names %}

    {{ log("DEBUG - Found columns: " ~ column_names|join(", "), info=true) }}

    {%- set ns = namespace(source_columns=[]) -%}

    {% for column_name in column_names %}
      {% if column_name|trim|lower != 'update_iceberg_ts' %}
        {%- do ns.source_columns.append(column_name) -%}
      {% endif %}
    {% endfor %}

    {%- set source_columns_csv = ns.source_columns|join(', ') -%}

    {{ log("DEBUG - Selecting columns: " ~ source_columns_csv, info=true) }}
    WITH source_data AS (
      {{ sql }}
    )

    SELECT
      {% for column_name in ns.source_columns %}
        s.{{ column_name }}{% if not loop.last %},{% endif %}
      {% endfor %}
      {% if ns.source_columns %},{% endif %}
      current_timestamp() AS update_iceberg_ts
    FROM source_data s
  {% else %}
    {{ sql }}
  {% endif %}
{% endmacro %}