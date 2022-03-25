{% materialization incremental, adapter='glue' -%}
  
  {#-- Validate early so we don't run SQL if the file_format + strategy combo is invalid --#}
  {%- set raw_file_format = config.get('file_format', default='parquet') -%}
  {%- set raw_strategy = config.get('incremental_strategy', default='insert_overwrite') -%}
  
  {%- set file_format = dbt_glue_validate_get_file_format(raw_file_format) -%}
  {%- set strategy = dbt_glue_validate_get_incremental_strategy(raw_strategy, file_format) -%}

  {%- set unique_key = config.get('unique_key', none) -%}
  {%- set partition_by = config.get('partition_by', none) -%}

  {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}

  {% set target_relation = this %}
  {% set existing_relation_type = adapter.get_table_type(this)  %}
  {% set tmp_relation = glue__make_temp_relation(this) %}

  {{ run_hooks(pre_hooks) }}

  {% if raw_strategy == 'merge' and file_format == 'hudi' %}
        {{ adapter.hudi_merge_table(target_relation, sql, unique_key, partition_by) }}
        {% set build_sql = "select * from " + target_relation.schema + "." + target_relation.identifier %}
  {% else %}
      {% if strategy == 'insert_overwrite' and partition_by %}
        {% call statement() %}
          set glue.sql.sources.partitionOverwriteMode = DYNAMIC
        {% endcall %}
      {% endif %}

      {% if existing_relation_type is none %}
        {% set build_sql = create_table_as(False, target_relation, sql) %}
      {% elif existing_relation_type == 'view' or full_refresh_mode %}
        {% do adapter.drop_view(target_relation) %}
        {% set build_sql = create_table_as(False, target_relation, sql) %}
      {% else %}
        {{ adapter.create_view_as(tmp_relation, sql) }}
        {% set build_sql = dbt_glue_get_incremental_sql(strategy, tmp_relation, target_relation, unique_key) %}
      {% endif %}
  {% endif %}

  {%- call statement('main') -%}
     {{ build_sql }}
  {%- endcall -%}

  {{ adapter.drop_view(tmp_relation) }}
  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}

