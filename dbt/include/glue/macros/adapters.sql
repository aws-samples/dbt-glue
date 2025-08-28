{% macro glue__location_clause() %}
  {%- set custom_location = config.get('custom_location', validator=validation.any[basestring]) -%}
  {%- set file_format = config.get('file_format', validator=validation.any[basestring]) -%}
  {%- set materialized = config.get('materialized') -%}

  {%- if file_format == 's3tables' -%}
    {# S3 Tables automatically manages location, so no LOCATION clause needed #}
  {%- elif custom_location is not none %}
    location '{{ custom_location }}'
  {%- else -%}
    {{ adapter.get_location(this) }}
  {%- endif %}
{%- endmacro -%}

{% macro spark__alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}

  {% if remove_columns %}
    {% if relation.is_delta %}
      {% set platform_name = 'Delta Lake' %}
    {% elif relation.is_iceberg %}
      {% set platform_name = 'Iceberg' %}
    {% else %}
      {% set platform_name = 'Apache Spark' %}
    {% endif %}
    {{ exceptions.raise_compiler_error(platform_name + ' does not support dropping columns from tables') }}
  {% endif %}

  {% if add_columns is none %}
    {% set add_columns = [] %}
  {% endif %}

  {% if add_columns|length > 0 %}
    {% set sql -%}
      alter {{ relation.type }} {{ relation }} add columns
      {% for column in add_columns %}
        {{ column.name }} {{ column.data_type }}{{ ',' if not loop.last }}
      {% endfor %}
    {%- endset -%}

    {# Use statement instead of run_query to avoid expecting a result #}
    {% call statement('alter_columns') %}
      {{ sql }}
    {% endcall %}
  {% endif %}

{% endmacro %}

{% macro glue__create_csv_table(model, agate_table) -%}
    {{ adapter.create_csv_table(model, agate_table) }}
{%- endmacro %}

{% macro glue__load_csv_rows(model, agate_table) %}
  {{return('')}}
{%- endmacro %}

{%- macro glue__get_drop_sql(relation) -%}
  {%- set file_format = config.get('file_format', default='parquet') -%}
  {%- set full_relation = glue__make_target_relation(relation, file_format) -%}
  {%- set purge_dropped_iceberg_data = config.get('purge_dropped_iceberg_data', default='False') -%}
  {%- if relation.is_view -%}
    {%- if relation|string == relation.identifier -%}
      {# Is this a temporary view (in Spark context)?  
         If the identifier/entity is the same as the relation rendered/string -- schema not included
         and it's a view, then it's a temporary view #}
      drop view if exists {{ full_relation }}
    {%- elif file_format in ['iceberg', 's3tables'] -%}
      {# Otherwise, if regular view, and if we are iceberg or s3tables, then drop table.
         Why?  Because currently s3tables/iceberg and iceberg file formats in Glue Catalog do 
         not currently support views, so when using iceberg/s3tables, and not temp views, then everything 
         is a table #}
      {%- if purge_dropped_iceberg_data == 'True' -%}
        drop table if exists {{ full_relation }} purge
      {%- else -%}
        drop table if exists {{ full_relation }}
      {%- endif %}
    {%- else -%}
      {# Otherwise, if we are a regular view, not iceberg or s3table, then drop the view and cascade #}
      drop view if exists {{ full_relation }}
    {%- endif -%}
  {%- else -%}
    {# If iceberg or s3tables, then drop table, with purge option if set #}
    {%- if file_format in ['iceberg', 's3tables'] and purge_dropped_iceberg_data == 'True' -%}
      drop table if exists {{ full_relation }} purge
    {%- else -%}
      drop table if exists {{ full_relation }}
    {%- endif -%}
  {%- endif %}
{%- endmacro -%}

{% macro glue__drop_relation(relation) -%}
  {%- set drop_sql = get_drop_sql(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    {{ drop_sql }}
  {%- endcall %}
{% endmacro %}

{% macro glue__make_temp_relation(base_relation, suffix) %}
  {%- set materialization = config.get('materialized', default='') -%}
  {%- set incremental_strategy = config.get('incremental_strategy', default='') -%}
  {%- set file_format = config.get('file_format', default='parquet') -%}
  {%- set use_iceberg_temp_views = config.get('use_iceberg_temp_views', default='False')  -%} 
  {%- set temp_type = 'view' -%} 
  {%- set tmp_identifier = base_relation.identifier ~ suffix -%}
  {# -- If target profile has temp_schema set, allows _tmp relations to be built in separate catalog when physicalized #}
  {%- set tmp_schema = target.temp_schema if target.temp_schema else base_relation.schema -%}
  {# -- By default, temp relations will be temporary views, but can be set to temp tables later if warranted #} 
  {%- if file_format == 'iceberg' and use_iceberg_temp_views == 'False' -%}
    {# If iceberg and config use_iceberg_temp_views == 'False', 
       then make tmp_relation a table, otherwise, by default tmp_relation will be a view #}
    {%- set temp_type = 'table' -%} 
  {%- endif -%}
  {% set tmp_relation = base_relation.incorporate(path={"schema": tmp_schema, "identifier": tmp_identifier}, type=temp_type) -%}
  {%- if temp_type == 'view' -%}
    {% set tmp_relation = tmp_relation.include(schema=false) -%}
  {%- endif -%}
  {% do return(tmp_relation) %}
{% endmacro %}

{% macro glue__create_temporary_view(relation, sql) -%}
  {%- set file_format = config.get('file_format', default='parquet') -%}

  {% if file_format == 'iceberg' and relation.type == 'table' %}
    {# -- If the temp relation is a table and iceberg, then create temporary table, otherwise view #}
    {%- set full_relation = glue__make_target_relation(relation, file_format) -%}
    create or replace table {{ full_relation }} using iceberg as {{ sql }}
  {% else %}
    create or replace temporary view {{ relation.include(schema=false) }} as {{ sql }}
  {% endif %}
{% endmacro %}

{% macro glue__file_format_clause() %}
  {%- set file_format = config.get('file_format', validator=validation.any[basestring]) -%}
  {%- if file_format == 's3tables' -%}
    {# S3 Tables automatically manages format, so no USING clause needed #}
  {%- elif file_format is not none %}
    using {{ file_format }}
  {%- else -%}
    using PARQUET
  {%- endif %}
{%- endmacro -%}

{% macro glue__create_table_as(temporary, relation, sql) -%}
  {% if temporary is sameas true -%}
    {{ create_temporary_view(relation, sql) }}
  {%- else -%}
    {%- set file_format = config.get('file_format', validator=validation.any[basestring]) -%}
    {%- set table_properties = config.get('table_properties', default={}) -%}

    {%- set create_statement_string -%}
      {% if file_format in ['delta', 'iceberg', 's3tables'] -%}
        create or replace table
      {%- else -%}
        create table
      {% endif %}
    {%- endset %}

    {%- set full_relation = relation -%}
    {%- if file_format in ['iceberg', 's3tables'] -%}
      {%- set full_relation = glue__make_target_relation(relation, file_format) -%}
    {%- endif -%}

        {{ create_statement_string }} {{ full_relation }}
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
    {{ glue__location_clause() }}
    {{ comment_clause() }}
    as
    {{ add_iceberg_timestamp_column(sql) }}
  {%- endif %}
{%- endmacro -%}

{% macro glue__snapshot_get_time() -%}
    current_timestamp()
{%- endmacro %}

{%- macro glue__get_drop_view_sql(relation) -%}
  {%- set file_format = config.get('file_format', default='parquet') -%}
  {%- set full_relation = glue__make_target_relation(relation, file_format) -%}
  {%- set purge_dropped_iceberg_data = config.get('purge_dropped_iceberg_data', default='False') -%}
  {%- if full_relation.is_view and full_relation|string == full_relation.identifier -%}
    {# Is this a temporary view (in Spark context)?  
       If the identifier/entity is the same as the relation rendered/string -- schema not included
       and it's a view, then it's a temporary view #}
    drop view if exists {{ full_relation }}
  {%- elif file_format in ['iceberg', 's3tables'] -%}
    {# Otherwise, if we are iceberg or s3tables, then drop table.
       Why?  Because currently s3tables/iceberg and iceberg file formats in Glue Catalog do 
       not currently support views, so when using iceberg/s3tables, and not temp views, then everything 
       is a table #}
    {%- if purge_dropped_iceberg_data == 'True' -%}
      drop table if exists {{ full_relation }} purge
    {%- else -%}
      drop table if exists {{ full_relation }}
    {%- endif %}
  {%- else -%}
    drop view if exists {{ full_relation }}
  {%- endif -%}
{%- endmacro -%}

{% macro glue__drop_view(relation) -%}
  {%- set drop_view_sql = glue__get_drop_view_sql(relation) -%}
  {% call statement('drop_view', auto_begin=False) -%}
    {{ drop_view_sql }}
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
    {%- set file_format = config.get('file_format') or 'parquet' -%}
    {%- set is_iceberg = file_format == 'iceberg' -%}

    {# Currently, with Glue Catalog, and using Iceberg, there is no support for creating views 
       either using Iceberg View Spec, or by using basic Glue/Athena Iceberg Views.  
       You can create Glue catalog views on Iceberg tables, but only through Athena.  
       But Iceberg View Spec is also not available in Athena yet. #}
    {%- if is_iceberg -%}
        {%- set full_relation = glue__make_target_relation(relation, file_format) -%}
        create or replace table {{ full_relation }}
        using iceberg
        as
        {{ add_iceberg_timestamp_column(sql) }}
    {%- else -%}
        {%- if contract_config.enforced -%}
          {{ get_assert_columns_equivalent(sql) }}
        {%- endif -%}
        create or replace view {{ relation }}
            as
        {{ add_iceberg_timestamp_column(sql) }}
    {%- endif -%}
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
  {%- set file_format = config.get('file_format', validator=validation.any[basestring]) -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier, file_format=file_format) -%}
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

{% macro adapters__py_get_writer_options() %}
    {%- set file_format = config.get('file_format', 'parquet') -%}
    {%- set location = adapter.get_location(this) -%}
    {%- set partition_by = config.get('partition_by', none) -%}
    {%- set table_properties = config.get('table_properties', {}) -%}

    {%- set options = {
        'file_format': file_format,
        'location': location,
        'partition_by': partition_by,
        'table_properties': table_properties
    } -%}

    {{ return(options) }}
{% endmacro %}
