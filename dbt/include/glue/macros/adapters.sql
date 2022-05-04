{% macro glue__location_clause(relation) %}
  {%- set custom_location = config.get('custom_location', validator=validation.any[basestring]) -%}
  {%- if custom_location is not none %}
    location '{{ custom_location }}'
  {%- else -%}
    {{ adapter.get_location(relation) }}
  {%- endif %}
{%- endmacro -%}


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

{% macro glue__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    {%- if relation.type is not none %}
        drop {{ relation.type }} if exists {{ relation }}
    {%- else -%}
        drop table if exists {{ relation }}
    {%- endif %}
  {%- endcall %}
{% endmacro %}

{% macro glue__create_temporary_view(relation, sql) -%}
  create or replace temporary view {{ relation.include(schema=false) }} as
    {{ sql }}
{% endmacro %}

{% macro glue__create_table_as(temporary, relation, sql) -%}
  {% if temporary -%}
    {{ create_temporary_view(relation, sql) }}
  {%- else -%}
    create table {{ relation }}
    {{ file_format_clause() }}
    {{ partition_cols(label="partitioned by") }}
    {{ clustered_cols(label="clustered by") }}
    {{ glue__location_clause(relation) }}
    {{ comment_clause() }}
    as
      {{ sql }}
  {%- endif %}
{%- endmacro -%}

{% macro glue__create_view_as(relation, sql) -%}
    {{ adapter.create_view_as(relation, sql) }}
{% endmacro %}

{% macro glue__create_view(relation, sql) -%}
    {{ adapter.create_view_as(relation, sql) }}
{% endmacro %}

{% macro glue__snapshot_get_time() -%}
  datetime()
{%- endmacro %}

{% macro glue__rename_relation(from_relation, to_relation) -%}
    {{ adapter.rename_relation(from_relation, to_relation) }}
{% endmacro %}

{% macro glue__drop_view(relation) -%}
  {% call statement('drop_view', auto_begin=False) -%}
    drop view if exists {{ relation }}
  {%- endcall %}
{% endmacro %}

{% macro glue__describe_table(relation) -%}
    {% set tmp_relation = adapter.describe_table(relation) %}
    {% do return(tmp_relation) %}
{% endmacro %}

{% macro glue__generate_database_name(custom_database_name=none, node=none) -%}
  {%- set default_database = target.database -%}
  {{ default_database }}
{%- endmacro %}