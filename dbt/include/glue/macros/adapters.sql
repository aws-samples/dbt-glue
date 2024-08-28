{% macro glue__location_clause(relation) %}
  {%- set custom_location = config.get('custom_location', validator=validation.any[basestring]) -%}
  {%- set file_format = config.get('file_format', validator=validation.any[basestring]) -%}
  {%- set materialized = config.get('materialized') -%}

  {%- if custom_location is not none %}
    location '{{ custom_location }}'
  {%- else -%}
    {% if file_format == 'iceberg' %}
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
    	{%- set default_catalog = 'glue_catalog' -%}
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

  {%- set create_statement_string -%}
    {% if file_format in ['delta', 'iceberg'] -%}
      create or replace table
    {%- else -%}
      create table
    {% endif %}
  {%- endset %}

  {% if temporary -%}
    {{ create_temporary_view(relation, sql) }}
  {%- else -%}
    	{{ create_statement_string }} {{ relation }}
    	{% set contract_config = config.get('contract') %}
      {% if contract_config.enforced %}
        {{ get_assert_columns_equivalent(sql) }}
        {#-- This does not enforce contstraints and needs to be a TODO #}
        {#-- We'll need to change up the query because with CREATE TABLE AS SELECT, #}
        {#-- you do not specify the columns #}
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
    set spark.sql.legacy.allowNonEmptyLocationInCTAS=true
    dbt_next_query
    DROP TABLE IF EXISTS {{ relation }}
    dbt_next_query
    create table {{ relation }}
    {{ adapter.get_location(relation) }}
    as
      {{ sql }}
  {% endcall %}
{%- endmacro -%}

{% macro glue__snapshot_get_time() -%}
    current_timestamp()
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
    {%- set contract_config = config.get('contract') -%}
    {%- if contract_config.enforced -%}
      {{ get_assert_columns_equivalent(sql) }}
    {%- endif -%}
    DROP VIEW IF EXISTS {{ relation }}
    dbt_next_query
    create view {{ relation }}
        as
    {{ sql }}
{% endmacro %}

{% macro glue__create_view(relation, sql) -%}
  {%- set lf_tags_config = config.get('lf_tags_config') -%}
  {%- set lf_grants = config.get('lf_grants') -%}
  {% call statement("create_view(", fetch_result=false, auto_begin=false) %}
    {{ create_view_as(relation, sql) }}
  {% endcall %}
  {% if lf_tags_config is not none %}
    {{ adapter.add_lf_tags(target_relation, lf_tags_config) }}
  {% endif %}
  {% if lf_grants is not none %}
    {{ adapter.apply_lf_grants(target_relation, lf_grants) }}
  {% endif %}
{% endmacro %}

{% macro glue_exec_query(sql) %}
  {% call statement("run_query_statement", fetch_result=false, auto_begin=false) %}
    {{ sql }}
  {% endcall %}
{% endmacro %}

{% macro spark__type_string() -%}
    STRING
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

{% macro create_or_replace_view() %}
  {%- set identifier = model['alias'] -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set exists_as_view = (old_relation is not none and old_relation.is_view) -%}
  {%- set lf_tags_config = config.get('lf_tags_config') -%}
  {%- set lf_grants = config.get('lf_grants') -%}
  {%- set target_relation = api.Relation.create(
      identifier=identifier, schema=schema, database=database,
      type='view') -%}
  {% set grant_config = config.get('grants') %}

  {{ run_hooks(pre_hooks) }}

  -- If there's a table with the same name and we weren't told to full refresh,
  -- that's an error. If we were told to full refresh, drop it. This behavior differs
  -- for Snowflake and BigQuery, so multiple dispatch is used.
  {%- if old_relation is not none and old_relation.is_table -%}
    {{ handle_existing_table(should_full_refresh(), old_relation) }}
  {%- endif -%}

  -- build model
  {% call statement('main') -%}
    {{ get_create_view_as_sql(target_relation, sql) }}
  {%- endcall %}

  {% set should_revoke = should_revoke(exists_as_view, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}
  {% if lf_tags_config is not none %}
    {{ adapter.add_lf_tags(target_relation, lf_tags_config) }}
  {% endif %}
  {% if lf_grants is not none %}
    {{ adapter.apply_lf_grants(target_relation, lf_grants) }}
  {% endif %}
  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{% endmacro %}
