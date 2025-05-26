{% macro create_python_merge_table(model, df_name, unique_key) %}
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

{% macro glue__py_write_table(model, df_name) %}
    {%- set target_relation = this.incorporate(type='table') -%}
    {%- set partition_by = config.get('partition_by', none) -%}
    {%- set location = adapter.get_location(this) -%}

    {% if partition_by %}
    {{ df_name }}.write \
        .mode('overwrite') \
        .partitionBy({{ partition_by | tojson }}) \
        .option('path', '{{ location }}') \
        .saveAsTable('{{ target_relation }}')
    {% else %}
    {{ df_name }}.write \
        .mode('overwrite') \
        .option('path', '{{ location }}') \
        .saveAsTable('{{ target_relation }}')
    {% endif %}
{% endmacro %}
