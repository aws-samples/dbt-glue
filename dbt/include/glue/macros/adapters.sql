{% macro glue__create_csv_table(model, agate_table) -%}
  {{ adapter.create_csv_table(model, agate_table) }}
{%- endmacro %}


{% macro glue__load_csv_rows(model, agate_table) %}
  {{return('')}}
{%- endmacro %}

{% macro glue__list_relations_without_caching(schema_relation) %}
    {% call statement('list_relations_without_caching', fetch_result=True) %}
        show tables
    {% endcall %}

    {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}

{% macro glue__create_table_as(temporary, relation, sql) -%}
  {% if temporary -%}
    {{ create_temporary_view(relation, sql) }}
  {%- else -%}
    create table {{ relation }}
    {{ file_format_clause() }}
    {{ partition_cols(label="partitioned by") }}
    {{ clustered_cols(label="clustered by") }}
    {{ adapter.get_location(relation) }}
    {{ comment_clause() }}
    as
      {{ sql }}
  {%- endif %}
{%- endmacro -%}


{% macro glue__snapshot_get_time() -%}
  datetime()
{%- endmacro %}



{% macro glue__drop_view(relation) -%}
  {% call statement('drop_view', auto_begin=False) -%}
    drop view if exists {{ relation }}
  {%- endcall %}
{% endmacro %}

{% macro glue__describe_table(relation) -%}
    {% set tmp_relation = adapter.describe_table(relation) %}
    {% do return(tmp_relation) %}
{% endmacro %}