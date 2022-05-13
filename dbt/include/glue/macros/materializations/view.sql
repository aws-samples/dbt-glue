{% materialization view, adapter='glue' -%}
    {{ return(create_or_replace_view()) }}
{%- endmaterialization %}