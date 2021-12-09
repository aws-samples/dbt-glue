
{% macro ref(model_name) %}

  {# override to strip off database, and return only schema.table #}

  {% set rel = builtins.ref(model_name) %}
  {% do return(rel.include(database=False)) %}

{% endmacro %}

{% macro source(source_name, model_name) %}

  {# override to strip off database, and return only schema.table #}

  {% set rel = builtins.source(source_name, model_name) %}
  {% do return(rel.include(database=False)) %}

{% endmacro %}