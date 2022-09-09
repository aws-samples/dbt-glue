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

{% macro glue__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
  {% set rel_type = adapter.get_table_type(relation)  %}
    {%- if rel_type is not none %}
        drop {{ rel_type }} if exists {{ relation }}
    {%- else -%}
        drop table if exists {{ relation }}
    {%- endif %}
  {%- endcall %}
{% endmacro %}

{% macro glue__make_temp_relation(base_relation, suffix) %}
    {% set tmp_identifier = base_relation.identifier ~ suffix %}
    {% set tmp_relation = base_relation.incorporate(path={"schema": base_relation.schema, "identifier": tmp_identifier}) -%}
    {% do return(tmp_relation) %}
{% endmacro %}

{% macro glue__create_temporary_view(relation, sql) -%}
  create or replace temporary view {{ relation.include(schema=false) }} as
    {{ sql }}
{% endmacro %}

{% macro glue__file_format_clause() %}
  {%- set file_format = config.get('file_format', validator=validation.any[basestring]) -%}
  {%- if file_format is not none %}
    using {{ file_format }}
  {%- else -%}
    using PARQUET
  {%- endif %}
{%- endmacro -%}

{% macro glue__create_table_as(temporary, relation, sql) -%}
  {% if temporary -%}
    {{ create_temporary_view(relation, sql) }}
  {%- else -%}
    create table {{ relation }}
    {{ glue__file_format_clause() }}
    {{ partition_cols(label="partitioned by") }}
    {{ clustered_cols(label="clustered by") }}
    {{ glue__location_clause(relation) }}
    {{ comment_clause() }}
    as
      {{ sql }}
  {%- endif %}
{%- endmacro -%}

{% macro glue__create_tmp_table_as(relation, sql) -%}
  {% call statement("create_tmp_table_as", fetch_result=false, auto_begin=false) %}
    create table {{ relation }}
    as
      {{ sql }}
  {% endcall %}
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

{% macro glue__generate_database_name(custom_database_name=none, node=none) -%}
  {%- set default_database = target.database -%}
  {{ default_database }}
{%- endmacro %}

{% macro glue__rename_relation(from_relation, to_relation) -%}
    {% if not from_relation.type %}
      {% do exceptions.raise_database_error("Cannot rename a relation with a blank type: " ~ from_relation.identifier) %}
    {% elif from_relation.type in ('table') %}
        {{ adapter.glue_rename_relation(from_relation, to_relation) }}
    {% elif from_relation.type == 'view' %}
        {{ glue_exec_query(adapter.duplicate_view(from_relation, to_relation)) }}
        {{ drop_view(from_relation) }}
    {% else %}
      {% do exceptions.raise_database_error("Unknown type '" ~ from_relation.type ~ "' for relation: " ~ from_relation.identifier) %}
    {% endif %}
{% endmacro %}

{% macro glue__create_view_as(relation, sql) -%}
    DROP VIEW IF EXISTS {{ relation }}
    dbt_next_query
    create or replace view {{ relation }}
        as
    {{ sql }}
{% endmacro %}

{% macro glue__create_view(relation, sql) -%}
  {% call statement("create_view(", fetch_result=false, auto_begin=false) %}
    {{ create_view_as(relation, sql) }}
  {% endcall %}
{% endmacro %}

{% macro glue_exec_query(sql) %}
  {% call statement("run_query_statement", fetch_result=false, auto_begin=false) %}
    {{ sql }}
  {% endcall %}
{% endmacro %}