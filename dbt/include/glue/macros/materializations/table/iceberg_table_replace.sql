{% materialization iceberg_table_replace, adapter='glue' -%}
  {%- set default_catalog = 'iceberg_catalog' -%}
  {%- set partition_by = config.get('partition_by', none) -%}
  {%- set expire_snapshots = config.get('expire_snapshots', true) -%}
  {% set target_relation = this %}
  {% set build_sql = create_or_replace(default_catalog, target_relation) %}

	{{ run_hooks(pre_hooks) }}

  {%- call statement('main') -%}
    {{ build_sql }}
  {%- endcall -%}

  {%- if expire_snapshots == true -%}
  	{%- set result = adapter.iceberg_expire_snapshots(default_catalog, target_relation) -%}
  {%- endif -%}

	{{ run_hooks(post_hooks) }}

	{{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}

{% macro create_or_replace(default_catalog, relation) %}
  create or replace table {{ default_catalog }}.{{ relation }}
  using ICEBERG
  {{ partition_cols(label="partitioned by") }}
  {{ glue__location_clause(relation) }}
  {{ comment_clause() }}
  as
  {{ sql }}
{% endmacro %}
