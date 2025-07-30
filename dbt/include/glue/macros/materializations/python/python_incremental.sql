{% materialization python_incremental, adapter='glue', supported_languages=['python'] %}
  {%- set language = model['language'] -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set existing_relation = load_relation(this) -%}
  {%- set should_full_refresh = should_full_refresh() -%}

  {% if existing_relation is not none and should_full_refresh %}
    {{ adapter.drop_relation(existing_relation) }}
    {% set existing_relation = none %}
  {% endif %}

  -- Setup
  {{ run_hooks(pre_hooks) }}

  -- Build model using standard statement approach
  {%- call statement('main', language=language) -%}
    {{ glue__py_write_table(compiled_code=model['compiled_code'], target_relation=target_relation) }}
  {%- endcall -%}

  -- Return the target relation
  {{ run_hooks(post_hooks) }}

  -- Return success with the relations
  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
