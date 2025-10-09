{# Put this here so we can mock it out in unit tests much easier #}

{% macro glue__make_target_relation(relation, file_format) %}
    {%- set iceberg_catalog = adapter.get_custom_iceberg_catalog_namespace() -%}
    {%- set needs_catalog = (file_format == 'iceberg' or file_format == 's3tables') -%}
    {%- set non_null_catalog = (iceberg_catalog is not none) -%}
    {# If the identifier is the relation rendered, meaning schema was not included, 
       and it's a view, then it's a temporary view #}
    {%- if relation|string == relation.identifier and relation.type == 'view' -%}
        {# If we are a temporary view, in the Spark session, where type is view and schema is empty, 
           then make sure the relation has the schema removed when referencing it. #}
        {%- do return(relation.include(schema=false)) -%}
    {%- elif non_null_catalog and needs_catalog %}
        {# Check if the schema already includes the catalog to avoid duplication #}
        {%- if relation.schema.startswith(iceberg_catalog ~ '.') %}
            {%- do return(relation) -%}
        {%- else %}
            {%- do return(relation.incorporate(path={"schema": iceberg_catalog ~ '.' ~ relation.schema, "identifier": relation.identifier})) -%}
        {%- endif %}
    {%- else -%}
        {%- do return(relation) -%}
    {%- endif %}
{% endmacro %}


