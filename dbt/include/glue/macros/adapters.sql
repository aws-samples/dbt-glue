{% macro glue__location_clause(relation) %}
  {%- set custom_location = config.get('custom_location', validator=validation.any[basestring]) -%}
  {%- set file_format = config.get('file_format', validator=validation.any[basestring]) -%}
  {%- set materialized = config.get('materialized') -%}

  {%- if custom_location is not none %}
    location '{{ custom_location }}'
  {%- else -%}
    {% if file_format == 'iceberg' or materialized == 'iceberg_table_replace' %}
      {{ adapter.get_iceberg_location(relation) }}
    {%- else -%}
    	{{ adapter.get_location(relation) }}
    {%- endif %}
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
    {%- if rel_type is not none and rel_type != 'iceberg_table' %}
        drop {{ rel_type }} if exists {{ relation }}
    {%- elif rel_type is not none and rel_type == 'iceberg_table' %}
    	{%- set default_catalog = 'iceberg_catalog' -%}
        drop table if exists {{ default_catalog }}.{{ relation }}
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
  {%- set file_format = config.get('file_format', validator=validation.any[basestring]) -%}
  {%- set table_properties = config.get('table_properties', default={}) -%}

  {% if temporary -%}
    {{ create_temporary_view(relation, sql) }}
  {%- else -%}
    {% if file_format == 'iceberg' %}
    {%- set default_catalog = 'iceberg_catalog' -%}
    	create table {{ default_catalog }}.{{ relation }}
    {% else %}
    	create table {{ relation }}
    {% endif %}
    {{ glue__file_format_clause() }}
		{{ partition_cols(label="partitioned by") }}
		{{ clustered_cols(label="clustered by") }}
		{{ set_table_properties(table_properties) }}
		{{ glue__location_clause(relation) }}
		{{ comment_clause() }}
	as
	{{ sql }}
  {%- endif %}
{%- endmacro -%}

{% macro glue__create_tmp_table_as(relation, sql) -%}
  {% call statement("create_tmp_table_as", fetch_result=false, auto_begin=false) %}
    DROP TABLE IF EXISTS {{ relation }}
    dbt_next_query
    create table {{ relation }}
    {{ glue__location_clause(relation) }}
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
  {%- set default_database = target.schema -%}
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
    create view {{ relation }}
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

{% macro spark__type_string() -%}
    STRING
{%- endmacro %}

{% macro iceberg_expire_snapshots(relation, timestamp, keep_versions) -%}
    {%- set default_catalog = 'iceberg_catalog' -%}
    {%- set result = adapter.iceberg_expire_snapshots(default_catalog, relation, timestamp, keep_versions ) -%}
{%- endmacro %}


{% macro set_table_properties(table_properties) -%}
	{%- set table_properties_formatted = [] -%}

	{%- for k in table_properties -%}
  	{% set _ = table_properties_formatted.append("'" + k + "'='" + table_properties[k] + "'") -%}
  {%- endfor -%}

  {% if table_properties_formatted|length > 0 %}
  	{%- set table_properties_csv= table_properties_formatted | join(', ') -%}
		TBLPROPERTIES (
			{{table_properties_csv}}
		)
  {%- endif %}
{%- endmacro %}
