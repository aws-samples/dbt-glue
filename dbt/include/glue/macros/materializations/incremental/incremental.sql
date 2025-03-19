{% materialization incremental, adapter='glue' -%}
  {# /*-- Validate early so we don't run SQL if the file_format + strategy combo is invalid --*/ #}
  {%- set raw_file_format = config.get('file_format', default='parquet') -%}
  {%- set raw_strategy = config.get('incremental_strategy', default='insert_overwrite') -%}
  {%- set file_format = dbt_glue_validate_get_file_format(raw_file_format) -%}
  {%- set strategy = dbt_glue_validate_get_incremental_strategy(raw_strategy, file_format) -%}
  
  {# /*-- Set vars --*/ #}
  {%- set language = model['language'] -%}
  {%- set existing_relation_type = adapter.get_table_type(this) -%}
  {%- set existing_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = existing_relation or glue__make_target_relation(this, config.get('file_format')) -%}
  {%- set tmp_relation = make_temp_relation(this, '_tmp') -%}
  {%- set unique_key = config.get('unique_key', none) -%}
  {%- set partition_by = config.get('partition_by', none) -%}
  {%- set incremental_predicates = config.get('predicates', none) or config.get('incremental_predicates', none) -%}
  {%- set custom_location = config.get('custom_location', default='empty') -%}
  {%- set expire_snapshots = config.get('iceberg_expire_snapshots', 'True') -%}
  {%- set table_properties = config.get('table_properties', default='empty') -%}
  {%- set lf_tags_config = config.get('lf_tags_config') -%}
  {%- set lf_grants = config.get('lf_grants') -%}
  {%- set delta_create_table_write_options = config.get('write_options', default={}) -%}
  {%- set substitute_variables = config.get('substitute_variables', default=[]) -%}
  {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}
  {%- set is_incremental = 'False' -%}
  {%- set schema_change_mode = config.get('on_schema_change', default='ignore') -%}

  {% if existing_relation_type is not none %}
      {%- set target_relation = target_relation.incorporate(type=existing_relation_type if existing_relation_type != "iceberg_table" else "table") -%}
  {% endif %}

  {# /*-- Validate specific requirements for hudi --*/ #}
  {% if unique_key is none and file_format == 'hudi' %}
    {{ exceptions.raise_compiler_error("unique_key model configuration is required for HUDI incremental materializations.") }}
  {% endif %}

  {# /*-- Run pre-hooks --*/ #}
  {{ run_hooks(pre_hooks) }}

  {# /*-- Incremental Process Logic --*/ #}
  {% if file_format == 'hudi' %}
        {%- set hudi_options = config.get('hudi_options', default={}) -%}
        {{ adapter.hudi_merge_table(target_relation, sql, unique_key, partition_by, custom_location, hudi_options, substitute_variables) }}
        {% set build_sql = "select * from " + target_relation.schema + "." + target_relation.identifier + " limit 1 "%}
  {% else %}
      {% if existing_relation_type is none %}
        {# /*-- If the relation doesn't exist it needs to be created --*/ #}
        {% if file_format == 'delta' %}
            {{ adapter.delta_create_table(target_relation, sql, unique_key, partition_by, custom_location, delta_create_table_write_options) }}
            {% set build_sql = "select * from " + target_relation.schema + "." + target_relation.identifier + " limit 1 " %}
        {% else %}
            {% set build_sql = create_table_as(False, target_relation, sql) %}
        {% endif %}
      {% elif existing_relation_type == 'view' or should_full_refresh() %}
        {# /*-- Relation must be dropped and created --*/ #}
        {{ drop_relation(target_relation) }}
        {% if file_format == 'delta' %}
            {{ adapter.delta_create_table(target_relation, sql, unique_key, partition_by, custom_location, delta_create_table_write_options) }}
            {% set build_sql = "select * from " + target_relation.schema + "." + target_relation.identifier + " limit 1 " %}
        {% else %}
            {% set build_sql = create_table_as(False, target_relation, sql) %}
        {% endif %}
      {% else %}
        {# /*-- Relation must be merged --*/ #}
        {% if file_format == 'iceberg' and schema_change_mode in ('append_new_columns', 'sync_all_columns') %}
          {%- call statement('create_tmp_table') -%}
            {{ create_temporary_view(tmp_relation, add_iceberg_timestamp_column(sql)) }}
          {%- endcall -%}
          {%- do process_schema_changes(on_schema_change, tmp_relation, target_relation) -%}

          {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
          {%- set dest_cols_csv = dest_columns | map(attribute='name') | join(', ') -%}
          {%- set full_tmp_relation = glue__make_target_relation(tmp_relation, file_format) -%}
          {%- set full_target_relation = glue__make_target_relation(target_relation, file_format) -%}
          {% set build_sql %}
          insert into {{ full_target_relation }} ({{ dest_cols_csv }}) select {{ dest_cols_csv }} from {{ full_tmp_relation }}
          {% endset %}

        {% else %}
          {%- call statement('create_tmp_relation') -%}
            {{ create_temporary_view(tmp_relation, sql) }}
          {%- endcall -%}
          {% set build_sql = dbt_glue_get_incremental_sql(strategy, tmp_relation, target_relation, unique_key, incremental_predicates) %}
        {% endif %}

        {% set is_incremental = 'True' %}
      {% endif %}
  {% endif %}

  {# /*-- Execute the main statement --*/ #}
  {%- call statement('main') -%}
     {{ build_sql }}
  {%- endcall -%}

  {# /*-- To not break existing workloads, but I think this doesn't need to be here since it can be addeed as post_hook query --*/ #}
  {%- if file_format == 'iceberg' and expire_snapshots == 'True' -%}
    {%- set result = adapter.iceberg_expire_snapshots(target_relation) -%}
  {%- endif -%}

  {# /*-- Run post-hooks --*/ #}
  {{ run_hooks(post_hooks) }}

  {# /*-- Setup lake formation tags --*/ #}
  {% if lf_tags_config is not none %}
    {{ adapter.add_lf_tags(target_relation, lf_tags_config) }}
  {% endif %}

  {# /*-- Setup lake formation grants --*/ #}
  {% if lf_grants is not none %}
    {{ adapter.apply_lf_grants(target_relation, lf_grants) }}
  {% endif %}

  {% if is_incremental == 'True' %}
    {{ glue__drop_relation(tmp_relation) }}
    {% if file_format == 'delta' %}
        {{ adapter.delta_update_manifest(target_relation, custom_location, partition_by) }}
    {% endif %}
  {% endif %}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
