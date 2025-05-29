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

{% macro glue__py_get_writer_options() %}
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

{% macro glue__create_python_merge_table(model, df_name, unique_key) %}
    {%- set target_relation = this.incorporate(type='table') -%}
    {%- set temp_view = model['name'] + '_temp_view' -%}

    -- Create a temporary view of the new data
    {{ df_name }}.createOrReplaceTempView('{{ temp_view }}')

    -- Merge the data using the unique key
    MERGE INTO {{ target_relation }} AS target
    USING {{ temp_view }} AS source
    ON {% for key in unique_key %}
        target.{{ key }} = source.{{ key }}
        {% if not loop.last %} AND {% endif %}
    {% endfor %}
    WHEN MATCHED THEN
        UPDATE SET
        {% for column in adapter.get_columns_in_relation(target_relation) %}
            {% if column.name not in unique_key %}
                target.{{ column.name }} = source.{{ column.name }}
                {% if not loop.last %},{% endif %}
            {% endif %}
        {% endfor %}
    WHEN NOT MATCHED THEN
        INSERT *
{% endmacro %}

{% macro glue__create_python_intermediate_table(model, df_name) %}
    {%- set relation = this.incorporate(type='table') -%}
    {%- set dest_columns = adapter.get_columns_in_relation(this) -%}

    {%- set write_mode = 'overwrite' if should_full_refresh() else 'append' -%}
    {%- set location = adapter.get_location(this) -%}

    {{ df_name }}.createOrReplaceTempView('{{ relation.identifier }}_intermediate')

    CREATE TABLE {{ relation }} AS
    SELECT * FROM {{ relation.identifier }}_intermediate
{% endmacro %}
