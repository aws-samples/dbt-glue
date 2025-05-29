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

  -- Execute the Python code with model information
  {% set result = adapter.execute_python(
      model['compiled_code'], 
      model_name=model['alias'],
      schema=model['schema'],
      config=model['config']
  ) %}

  -- Return the target relation
  {{ run_hooks(post_hooks) }}

  -- Return success with the relations
  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
