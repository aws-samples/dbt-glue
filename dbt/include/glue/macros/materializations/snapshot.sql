{% macro glue_build_snapshot_staging_table(strategy, sql, target_relation, file_format) %}
    {% set tmp_identifier = target_relation.identifier ~ '__dbt_tmp' %}

    {%- set tmp_relation = api.Relation.create(identifier=tmp_identifier,
                                                  schema=target_relation.schema,
                                                  database=none,
                                                  type='view') -%}

    {% set select = glue_snapshot_staging_table(strategy, sql, target_relation, file_format) %}

    {# needs to be a non-temp view so that its columns can be ascertained via `describe` #}
    {% call statement('build_snapshot_staging_relation') %}
        {{ create_view_as(tmp_relation, select) }}
    {% endcall %}

    {% do return(tmp_relation) %}
{% endmacro %}


{% macro glue_snapshot_staging_table(strategy, source_sql, target_relation, file_format) -%}

    with snapshot_query as (
        {% if file_format=='iceberg' %}
        {{ source_sql | replace("from ", "from glue_catalog.")}}
        {%else%}
        {{ source_sql }}
        {% endif %}

    ),

    snapshotted_data as (

        select *,
            {{ strategy.unique_key }} as dbt_unique_key

        {% if file_format=='iceberg' %}
        from glue_catalog.{{ target_relation }}
        {%else%}
        from {{ target_relation }}
        {% endif %}
        where dbt_valid_to is null

    ),

    insertions_source_data as (

        select
            *,
            {{ strategy.unique_key }} as dbt_unique_key,
            {{ strategy.updated_at }} as dbt_updated_at,
            {{ strategy.updated_at }} as dbt_valid_from,
            nullif({{ strategy.updated_at }}, {{ strategy.updated_at }}) as dbt_valid_to,
            {{ strategy.scd_id }} as dbt_scd_id

        from snapshot_query
    ),

    updates_source_data as (

        select
            *,
            {{ strategy.unique_key }} as dbt_unique_key,
            {{ strategy.updated_at }} as dbt_updated_at,
            {{ strategy.updated_at }} as dbt_valid_from,
            {{ strategy.updated_at }} as dbt_valid_to

        from snapshot_query
    ),

    {%- if strategy.invalidate_hard_deletes %}

    deletes_source_data as (

        select
            *,
            {{ strategy.unique_key }} as dbt_unique_key
        from snapshot_query
    ),
    {% endif %}

    insertions as (

        select
            'insert' as dbt_change_type,
            source_data.*

        from insertions_source_data as source_data
        left outer join snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where snapshotted_data.dbt_unique_key is null
           or (
                snapshotted_data.dbt_unique_key is not null
            and (
                {{ strategy.row_changed }}
            )
        )

    ),

    updates as (

        select
            'update' as dbt_change_type,
            source_data.*,
            snapshotted_data.dbt_scd_id

        from updates_source_data as source_data
        join snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where (
            {{ strategy.row_changed }}
        )
    )

    {%- if strategy.invalidate_hard_deletes -%}
    ,

    deletes as (

        select
            'delete' as dbt_change_type,
            source_data.*,
            {{ snapshot_get_time() }} as dbt_valid_from,
            {{ snapshot_get_time() }} as dbt_updated_at,
            {{ snapshot_get_time() }} as dbt_valid_to,
            snapshotted_data.dbt_scd_id

        from snapshotted_data
        left join deletes_source_data as source_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where source_data.dbt_unique_key is null
    )
    {%- endif %}

    select * from insertions
    union all
    select * from updates
    {%- if strategy.invalidate_hard_deletes %}
    union all
    select * from deletes
    {%- endif %}

{%- endmacro %}

{% materialization snapshot, adapter='glue' %}
  {%- set config = model['config'] -%}

  {%- set target_table = model.get('alias', model.get('name')) -%}
  {%- set lf_tags_config = config.get('lf_tags_config') -%}
  {%- set lf_grants = config.get('lf_grants') -%}
  {%- set strategy_name = config.get('strategy') -%}
  {%- set unique_key = config.get('unique_key') %}
  {%- set file_format = config.get('file_format', 'parquet') -%}
  {%- set grant_config = config.get('grants') -%}
  {%- set table_properties = config.get('table_properties', 'empty') -%}


  {% set target_relation_exists, target_relation = get_or_create_relation(
          database=none,
          schema=model.schema,
          identifier=target_table,
          type='table') -%}
  {%- if file_format not in ['delta', 'hudi', 'iceberg'] -%}
    {% set invalid_format_msg -%}
      Invalid file format: {{ file_format }}
      Snapshot functionality requires file_format be set to 'delta' or 'hudi' or 'iceberg'
    {%- endset %}
    {% do exceptions.raise_compiler_error(invalid_format_msg) %}
  {% endif %}

  {% if not adapter.check_schema_exists(model.database, model.schema) %}
    {% do create_schema(model.database, model.schema) %}
  {% endif %}

  {%- if not target_relation.is_table -%}
    {% do exceptions.relation_wrong_type(target_relation, 'table') %}
  {%- endif -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set strategy_macro = strategy_dispatch(strategy_name) %}
  {% set strategy = strategy_macro(model, "snapshotted_data", "source_data", config, target_relation_exists) %}

  {% if not target_relation_exists %}

    {%- if file_format == 'hudi' -%}
      {%- set partition_by = None -%}
      {%- set custom_location = 'empty' -%}
      {%- set unique_key = 'dbt_scd_id' -%}
      {% set build_sql = build_snapshot_table(strategy, sql) %}
      {%- set hudi_options = config.get('hudi_options') -%}
      {{ adapter.hudi_merge_table(target_relation, build_sql, unique_key, partition_by, custom_location, hudi_options) }}
      {% set final_sql = "select * from " + target_relation.schema + "." + target_relation.identifier %}
    {% elif file_format == 'iceberg'%}
      {%- set partition_by = None -%}
      {%- set custom_location = 'empty' -%}
      {%- set unique_key = 'dbt_scd_id' -%}
      {% set build_sql = build_snapshot_table(strategy, sql | replace("from ", "from glue_catalog.")) %}
      {{ adapter.iceberg_write(target_relation, build_sql, unique_key, partition_by, custom_location, 'insert_overwrite', table_properties) }}
      {% set final_sql = "select * from glue_catalog." + target_relation.schema + "." + target_relation.identifier %}
    {% else %}
      {% set build_sql = build_snapshot_table(strategy, sql) %}
      {% set final_sql = create_table_as(False, target_relation, build_sql) %}
    {% endif %}

  {% else %}
      {{ adapter.valid_snapshot_target(target_relation) }}
      {% set staging_table = glue_build_snapshot_staging_table(strategy, sql, target_relation, file_format) %}

      -- this may no-op if the database does not require column expansion
      {% do adapter.expand_target_column_types(from_relation=staging_table,
                                               to_relation=target_relation) %}

      {% set missing_columns = adapter.get_missing_columns(staging_table, target_relation)
                                   | rejectattr('name', 'equalto', 'dbt_change_type')
                                   | rejectattr('name', 'equalto', 'DBT_CHANGE_TYPE')
                                   | rejectattr('name', 'equalto', 'dbt_unique_key')
                                   | rejectattr('name', 'equalto', 'DBT_UNIQUE_KEY')
                                   | list %}

      {% do create_columns(target_relation, missing_columns) %}

      {% set source_columns = adapter.get_columns_in_relation(staging_table)
                                   | rejectattr('name', 'equalto', 'dbt_change_type')
                                   | rejectattr('name', 'equalto', 'DBT_CHANGE_TYPE')
                                   | rejectattr('name', 'equalto', 'dbt_unique_key')
                                   | rejectattr('name', 'equalto', 'DBT_UNIQUE_KEY')
                                   | list %}

      {% set quoted_source_columns = [] %}
      {% for column in source_columns %}
        {% do quoted_source_columns.append(adapter.quote(column.name)) %}
      {% endfor %}

    {%- if file_format == 'hudi' -%}
        {%- set partition_by = None -%}
        {%- set custom_location = 'empty' -%}
        {%- set build_sql = "select * from " + staging_table.schema + "." + staging_table.identifier -%}
        {%- set unique_key = 'dbt_scd_id' -%}
        {%- set hudi_options = config.get('hudi_options') -%}
        {{ adapter.hudi_merge_table(target_relation, build_sql, unique_key, partition_by, custom_location, hudi_options) }}
        {% set final_sql = "select * from " + target_relation.schema + "." + target_relation.identifier %}
      {% elif file_format == 'iceberg' %}
        {%- set partition_by = None -%}
        {%- set custom_location = 'empty' -%}
        {%- set build_sql = "select * from " + staging_table.schema + "." + staging_table.identifier -%}
        {%- set unique_key = 'dbt_scd_id' -%}
        {{ adapter.iceberg_write(target_relation, build_sql, unique_key, partition_by, custom_location, 'insert_overwrite', table_properties) }}
        {% set final_sql = "select * from glue_catalog." + target_relation.schema + "." + target_relation.identifier %}
      {% else %}
        {% set final_sql = snapshot_merge_sql(
                target = target_relation,
                source = staging_table,
                insert_cols = quoted_source_columns
             )
        %}
    {% endif %}

  {% endif %}

  {% call statement('main') %}
      {{ final_sql }}
  {% endcall %}
  
  {% if lf_tags_config is not none %}
  {{ adapter.add_lf_tags(target_relation, lf_tags_config) }}
  {% endif %}

  {% if lf_grants is not none %}
    {{ adapter.apply_lf_grants(target_relation, lf_grants) }}
  {% endif %}
  
  {% set should_revoke = should_revoke(target_relation_exists, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ adapter.commit() }}

  {% if staging_table is defined %}
      {% do post_snapshot(staging_table) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
