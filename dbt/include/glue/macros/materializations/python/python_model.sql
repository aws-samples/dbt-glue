{% materialization python_model, adapter='glue', supported_languages=['python'] %}
  {%- set config = model['config'] -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set existing_relation = load_relation(this) -%}
  {%- set should_full_refresh = should_full_refresh() -%}

  {% if existing_relation is not none and should_full_refresh %}
    {{ adapter.drop_relation(existing_relation) }}
    {% set existing_relation = none %}
  {% endif %}

  -- Setup
  {{ run_hooks(pre_hooks) }}

  -- Execute the Python code
  {% set result = adapter.execute_python(model['compiled_code']) %}

  -- Write the DataFrame to a table
  {% if existing_relation is none %}
    {{ glue__py_write_table(model, 'df') }}
  {% else %}
    {{ glue__py_write_table(model, 'df') }}
  {% endif %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
