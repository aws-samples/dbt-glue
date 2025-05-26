{% macro glue__py_write_table(model, df_name) %}
    {%- set relation = this.incorporate(type='table') -%}
    {%- set dest_columns = adapter.get_columns_in_relation(this) -%}

    {%- set write_mode = 'overwrite' if should_full_refresh() else 'append' -%}
    {%- set location = adapter.get_location(this) -%}

    {%- set table_properties = config.get('table_properties', {}) -%}
    {%- set partition_by = config.get('partition_by', none) -%}

    {%- if partition_by is not none %}
    {{ df_name }}.write \
        .mode('{{ write_mode }}') \
        .partitionBy({{ partition_by | tojson }}) \
        .option('path', '{{ location }}') \
        .saveAsTable('{{ relation }}')
    {%- else %}
    {{ df_name }}.write \
        .mode('{{ write_mode }}') \
        .option('path', '{{ location }}') \
        .saveAsTable('{{ relation }}')
    {%- endif %}

    {%- if table_properties %}
    ALTER TABLE {{ relation }} SET TBLPROPERTIES (
        {%- for key, value in table_properties.items() %}
        '{{ key }}' = '{{ value }}' {{ ',' if not loop.last }}
        {%- endfor %}
    )
    {%- endif %}
{% endmacro %}

{% macro py_get_writer_options() %}
    {%- set file_format = config.get('file_format', 'parquet') -%}
    {%- set location = adapter.get_location(this) -%}
    {%- set partition_by = config.get('partition_by', none) -%}
    {%- set table_properties = config.get('table_properties', {}) -%}

    {%- set options = {
        'file_format': file_format,
        'location': location,
        'partition_by': partition_by,
        'table_properties': table_properties
    } -%}

    {{ return(options) }}
{% endmacro %}

{% macro create_python_intermediate_table(model, df_name) %}
    {%- set relation = this.incorporate(type='table') -%}
    {%- set dest_columns = adapter.get_columns_in_relation(this) -%}

    {%- set write_mode = 'overwrite' if should_full_refresh() else 'append' -%}
    {%- set location = adapter.get_location(this) -%}

    {{ df_name }}.createOrReplaceTempView('{{ relation.identifier }}_intermediate')

    CREATE TABLE {{ relation }} AS
    SELECT * FROM {{ relation.identifier }}_intermediate
{% endmacro %}
