{% materialization incremental, adapter='glue' -%}
  
  {#-- Validate early so we don't run SQL if the file_format + strategy combo is invalid --#}
  {%- set raw_file_format = config.get('file_format', default='parquet') -%}
  {%- set raw_strategy = config.get('incremental_strategy', default='insert_overwrite') -%}
  
  {%- set file_format = dbt_spark_validate_get_file_format(raw_file_format) -%}
  {%- set strategy = dbt_spark_validate_get_incremental_strategy(raw_strategy, file_format) -%}

  {%- set unique_key = config.get('unique_key', none) -%}
  {%- set partition_by = config.get('partition_by', none) -%}
  {%- set custom_location = config.get('custom_location', default='empty') -%}

  {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}

  {% set target_relation = this %}
  {% set existing_relation_type = adapter.get_table_type(target_relation)  %}
  {% set tmp_relation = make_temp_relation(target_relation, '_tmp') %}
  {% set is_incremental = 'False' %}

  {% if file_format == 'hudi' %}
        {{ adapter.hudi_merge_table(target_relation, sql, unique_key, partition_by, custom_location) }}
        {% set build_sql = "select * from " + target_relation.schema + "." + target_relation.identifier %}
  {% else %}
      {% if strategy == 'insert_overwrite' and partition_by %}
        {% call statement() %}
          set spark.sql.sources.partitionOverwriteMode = DYNAMIC
        {% endcall %}
      {% endif %}
      {% if existing_relation_type is none %}
        {% if file_format == 'delta' %}
            {{ adapter.delta_create_table(target_relation, sql, unique_key, partition_by, custom_location) }}
            {% set build_sql = "select * from " + target_relation.schema + "." + target_relation.identifier %}
        {% else %}
            {% set build_sql = create_table_as(False, target_relation, sql) %}
        {% endif %}
      {% elif existing_relation_type == 'view' or full_refresh_mode %}
        {{ drop_relation(target_relation) }}
        {% if file_format == 'delta' %}
            {{ adapter.delta_create_table(target_relation, sql, unique_key, partition_by, custom_location) }}
            {% set build_sql = "select * from " + target_relation.schema + "." + target_relation.identifier %}
        {% else %}
            {% set build_sql = create_table_as(False, target_relation, sql) %}
        {% endif %}
      {% else %}
        {{ glue__create_view(tmp_relation, sql) }}
        {% set is_incremental = 'True' %}
        {% set build_sql = dbt_glue_get_incremental_sql(strategy, tmp_relation, target_relation, unique_key) %}
      {% endif %}
  {% endif %}

  {%- call statement('main') -%}
     {{ build_sql }}
  {%- endcall -%}

  {% if is_incremental == 'True' %}
    {{ glue__drop_view(tmp_relation) }}
  {% endif %}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}

