{% materialization delta_table_replace, adapter='glue' %}
  {%- set partition_by = config.get('partition_by', none) -%}
  {%- set table_properties = config.get('table_properties', default={}) -%}
  {% set lf_tags_config = config.get('lf_tags_config') -%}
  {%- set lf_grants = config.get('lf_grants') -%}
  {%- set target_relation = this -%}
  {%- set build_sql = delta_create_or_replace(target_relation, table_properties) -%}

  {{ run_hooks(pre_hooks) }}

  {%- call statement('main') -%}
    {{ build_sql }}
  {%- endcall -%}

  {% do persist_docs(target_relation, model) %}

  {% do persist_constraints(target_relation, model) %}
    
  {% if lf_tags_config is not none %}
    {{ adapter.add_lf_tags(target_relation, lf_tags_config) }}
  {% endif %}

  {% if lf_grants is not none %}
    {{ adapter.apply_lf_grants(target_relation, lf_grants) }}
  {% endif %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}

{% macro delta_create_or_replace(relation, table_properties) %}
  create or replace table {{ relation }}
  using delta
  {{ set_table_properties(table_properties) }}
  {{ partition_cols(label="partitioned by") }}
  {{ glue__location_clause(relation) }}
  {{ comment_clause() }}
  as
  {{ sql }}
{% endmacro %}
