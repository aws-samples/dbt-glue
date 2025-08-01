-- Register Python materializations
{% macro register_python_materializations() %}
  {{ return({
    'python_model': {
      'supported_languages': ['python'],
      'adapter': 'glue'
    },
    'python_incremental': {
      'supported_languages': ['python'],
      'adapter': 'glue'
    }
  }) }}
{% endmacro %}
