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

    {%- set ns = namespace(has_update_ts_column=false) -%}
    {% for column_name in column_names %}
      {% if column_name|trim|lower == 'update_iceberg_ts' %}
        {%- set ns.has_update_ts_column = true -%}
      {% endif %}
    {% endfor %}

    {{ log("DEBUG - has_update_ts_column: " ~ ns.has_update_ts_column, info=true) }}

    WITH source_data AS (
      {{ sql }}
    )

    {% if ns.has_update_ts_column %}
      {{ log("DEBUG - Using existing update_iceberg_ts column", info=true) }}
      SELECT * FROM source_data
    {% else %}
      {{ log("DEBUG - Adding new update_iceberg_ts column", info=true) }}
      SELECT
        s.*,
        current_timestamp() AS update_iceberg_ts
      FROM source_data s
    {% endif %}

  {% else %}
    {{ sql }}
  {% endif %}
{% endmacro %}