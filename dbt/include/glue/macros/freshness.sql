{% macro glue__current_timestamp() -%}
    current_timestamp()
{%- endmacro %}


{# dbt-core dispatches this macro with both arguments, but the Glue Data Catalog
   is queried by database name alone, so information_schema is not forwarded. #}
{% macro glue__get_relation_last_modified(information_schema, relations) -%}
    {{ return(adapter.get_relation_last_modified(relations)) }}
{%- endmacro %}
