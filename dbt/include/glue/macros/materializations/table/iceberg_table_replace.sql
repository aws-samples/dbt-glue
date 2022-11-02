{% materialization iceberg_table_replace, adapter='glue' -%}
  {%- set default_catalog = 'iceberg_catalog' -%}
  {%- set partition_by = config.get('partition_by', none) -%}
  {% set target_relation = this %}
  {% set build_sql = create_or_replace(default_catalog, target_relation) %}

  {%- call statement('main') -%}
    {{ build_sql }}
  {%- endcall -%}

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
