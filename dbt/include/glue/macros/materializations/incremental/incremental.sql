{% materialization incremental, adapter='glue' -%}
  
  {#-- Validate early so we don't run SQL if the file_format + strategy combo is invalid --#}
  {%- set raw_file_format = config.get('file_format', default='parquet') -%}
  {%- set raw_strategy = config.get('incremental_strategy', default='insert_overwrite') -%}
  
  {% if raw_file_format == 'iceberg' %} 
    {%- set file_format = 'iceberg' -%}
    {%- set strategy = raw_strategy -%}
  {% else %}
    {%- set file_format = dbt_spark_validate_get_file_format(raw_file_format) -%}
    {%- set strategy = dbt_spark_validate_get_incremental_strategy(raw_strategy, file_format) -%}
  {% endif %}

  {%- set unique_key = config.get('unique_key', none) -%}
  {% if unique_key is none and file_format == 'hudi' %}
    {{ exceptions.raise_compiler_error("unique_key model configuration is required for HUDI incremental materializations.") }}
  {% endif %}

  {%- set partition_by = config.get('partition_by', none) -%}
  {%- set custom_location = config.get('custom_location', default='empty') -%}
  {%- set expire_snapshots = config.get('iceberg_expire_snapshots', 'True') -%}
  {%- set table_properties = config.get('table_properties', default='empty') -%}
  {%- set delta_create_table_write_options = config.get('write_options', default={}) -%}

  {% set target_relation = this %}
  {%- set existing_relation = load_relation(this) -%}
  {% set existing_relation_type = adapter.get_table_type(target_relation)  %}
  {% set tmp_relation = make_temp_relation(target_relation, '_tmp') %}
  {% set is_incremental = 'False' %}
  {% set lf_tags_config = config.get('lf_tags_config') %}
  {% set lf_grants = config.get('lf_grants') %}
  {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}

  {% call statement() %}
    set spark.sql.autoBroadcastJoinThreshold=-1
  {% endcall %}

  {{ run_hooks(pre_hooks) }}
  {%- set substitute_variables = config.get('substitute_variables', default=[]) -%}

  {% if file_format == 'hudi' %}
        {%- set hudi_options = config.get('hudi_options', default={}) -%}
        {{ adapter.hudi_merge_table(target_relation, sql, unique_key, partition_by, custom_location, hudi_options, substitute_variables) }}
        {% set build_sql = "select * from " + target_relation.schema + "." + target_relation.identifier + " limit 1 "%}
  {% else %}
      {% if strategy == 'insert_overwrite' and partition_by %}
        {% call statement() %}
          set spark.sql.sources.partitionOverwriteMode = DYNAMIC
        {% endcall %}
      {% endif %}
      {% if existing_relation_type is none %}
        {% if file_format == 'delta' %}
            {{ adapter.delta_create_table(target_relation, sql, unique_key, partition_by, custom_location, delta_create_table_write_options) }}
            {% set build_sql = "select * from " + target_relation.schema + "." + target_relation.identifier + " limit 1 " %}
        {% elif file_format == 'iceberg' %}
            {{ adapter.iceberg_write(target_relation, sql, unique_key, partition_by, custom_location, strategy, table_properties) }}
            {% set build_sql = "select * from glue_catalog." + target_relation.schema + "." + target_relation.identifier + " limit 1 "%}
        {% else %}
            {% set build_sql = create_table_as(False, target_relation, sql) %}
        {% endif %}
      {% elif existing_relation_type == 'view' or should_full_refresh() %}
        {{ drop_relation(target_relation) }}
        {% if file_format == 'delta' %}
            {{ adapter.delta_create_table(target_relation, sql, unique_key, partition_by, custom_location, delta_create_table_write_options) }}
            {% set build_sql = "select * from " + target_relation.schema + "." + target_relation.identifier + " limit 1 " %}
        {% elif file_format == 'iceberg' %}
            {{ adapter.iceberg_write(target_relation, sql, unique_key, partition_by, custom_location, strategy, table_properties) }}
            {% set build_sql = "select * from glue_catalog." + target_relation.schema + "." + target_relation.identifier + " limit 1 "%}
        {% else %}
            {% set build_sql = create_table_as(False, target_relation, sql) %}
        {% endif %}
      {% elif file_format == 'iceberg' %}
        {{ adapter.iceberg_write(target_relation, sql, unique_key, partition_by, custom_location, strategy, table_properties) }}
        {% set build_sql = "select * from glue_catalog." + target_relation.schema + "." + target_relation.identifier + " limit 1 "%}
        {%- if expire_snapshots == 'True' -%}
  	      {%- set result = adapter.iceberg_expire_snapshots(target_relation) -%}
        {%- endif -%}
      {% else %}
        {{ glue__create_tmp_table_as(tmp_relation, sql) }}
        {% set is_incremental = 'True' %}
        {% set build_sql = dbt_glue_get_incremental_sql(strategy, tmp_relation, target_relation, unique_key) %}

        {%- do process_schema_changes(on_schema_change, tmp_relation, existing_relation) -%}
      {% endif %}
  {% endif %}

  {%- call statement('main') -%}
     {{ build_sql }}
  {%- endcall -%}

  {{ run_hooks(post_hooks) }}

  {% if lf_tags_config is not none %}
    {{ adapter.add_lf_tags(target_relation, lf_tags_config) }}
  {% endif %}

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
