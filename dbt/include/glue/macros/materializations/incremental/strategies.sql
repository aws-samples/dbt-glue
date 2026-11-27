{#-- Render the column list used to write the source relation into the target.
     Columns follow the target's order. When source_columns is given, target
     columns the source does not produce (left behind by
     on_schema_change='append_new_columns' after the model stopped emitting
     them) are filled with a typed NULL instead of being selected from the
     source, matching dbt-core. Names are matched case-insensitively. --#}
{% macro dbt_glue_source_column_list(dest_columns, source_columns=none, prefix='', alias=true) -%}
  {%- set source_names = none if source_columns is none else source_columns | map(attribute='name') | map('lower') | list -%}
  {%- for col in dest_columns -%}
    {%- if source_names is none or col.name | lower in source_names -%}
      {{ prefix }}{{ col.name }}
    {%- else -%}
      cast(null as {{ col.dtype }}){% if alias %} as {{ col.name }}{% endif %}
    {%- endif -%}
    {%- if not loop.last %}, {% endif -%}
  {%- endfor -%}
{%- endmacro %}


{% macro get_insert_overwrite_sql(source_relation, target_relation, source_columns=none) %}
    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set dest_cols_csv = dbt_glue_source_column_list(dest_columns, source_columns) -%}
    {%- set file_format = config.get('file_format', default='parquet') -%}
    {%- set full_target_relation = target_relation -%}
    {%- set full_source_relation = source_relation -%}
    {%- if file_format in ['iceberg', 's3tables'] -%}
        {%- set full_target_relation = glue__make_target_relation(target_relation, file_format) -%}
        {# Both Iceberg and S3 Tables temporary relations should use catalog prefixes for consistency #}
        {%- set full_source_relation = glue__make_target_relation(source_relation, file_format) -%}
    {%- else -%}
        {%- set full_source_relation =  source_relation.include(schema=false) -%}
    {%- endif -%}
    set hive.exec.dynamic.partition.mode=nonstrict
    dbt_next_query
    insert overwrite table {{ full_target_relation }}
    {{ partition_cols(label="partition") }}
    select {{dest_cols_csv}} from {{ full_source_relation }}
{% endmacro %}


{% macro get_insert_into_sql(source_relation, target_relation, source_columns=none) %}
    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set dest_cols_csv = dbt_glue_source_column_list(dest_columns, source_columns) -%}
    {%- set schema_change_mode = config.get('on_schema_change', default='ignore') -%}
    {%- set file_format = config.get('file_format', default='parquet') -%}
    {%- set full_target_relation = target_relation -%}
    {%- set full_source_relation = source_relation -%}
    {%- if file_format in ['iceberg', 's3tables'] -%}
        {%- set full_target_relation = glue__make_target_relation(target_relation, file_format) -%}
        {# Both Iceberg and S3 Tables temporary relations should use catalog prefixes for consistency #}
        {%- set full_source_relation = glue__make_target_relation(source_relation, file_format) -%}
    {%- endif -%}
    {%- if file_format in ['iceberg', 's3tables'] or schema_change_mode != 'ignore' -%}
        insert into table {{ full_target_relation }} select {{dest_cols_csv}} from {{ full_source_relation }}
    {%- else -%}
        insert into table {{ full_target_relation }} select {{dest_cols_csv}} from {{ source_relation.include(schema=false) }}
    {%- endif -%}
{% endmacro %}


{% macro get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates, source_columns=none) %}
  {# /* need dest_columns for merge_exclude_columns, default to use "*" */ #}
  {%- set predicates = [] if incremental_predicates is none else [] + incremental_predicates -%}
  {%- if dest_columns is none -%}
    {%- set dest_columns = adapter.get_columns_in_relation(target) -%}
  {%- endif -%}
  {#-- Matched rows only update columns the source produces; target-only columns keep their value --#}
  {%- set updatable_columns = dest_columns -%}
  {%- if source_columns is not none -%}
    {%- set source_names = source_columns | map(attribute='name') | map('lower') | list -%}
    {%- set updatable_columns = [] -%}
    {%- for col in dest_columns if col.name | lower in source_names -%}
      {%- do updatable_columns.append(col) -%}
    {%- endfor -%}
  {%- endif -%}
  {%- set merge_update_columns = config.get('merge_update_columns') -%}
  {%- set merge_exclude_columns = config.get('merge_exclude_columns') -%}
  {%- set update_columns = get_merge_update_columns(merge_update_columns, merge_exclude_columns, updatable_columns) -%}
  {%- set dest_cols_csv = dest_columns | map(attribute='name') | join(', ') -%}
  {%- set file_format = config.get('file_format', default='parquet') -%}
  {%- set full_target = target -%}
  {%- set full_source = source -%}
  {%- if file_format in ['iceberg', 's3tables'] -%}
      {%- set full_target = glue__make_target_relation(target, file_format) -%}
      {# Both Iceberg and S3 Tables temporary relations should use catalog prefixes for consistency #}
      {%- set full_source = glue__make_target_relation(source, file_format) -%}
  {%- endif -%}

  {% if unique_key %}
      {% if unique_key is sequence and unique_key is not mapping and unique_key is not string %}
          {% for key in unique_key %}
              {% set this_key_match %}
                  DBT_INTERNAL_SOURCE.{{ key }} = DBT_INTERNAL_DEST.{{ key }}
              {% endset %}
              {% do predicates.append(this_key_match) %}
          {% endfor %}
      {% else %}
          {% set unique_key_match %}
              DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}
          {% endset %}
          {% do predicates.append(unique_key_match) %}
      {% endif %}
  {% else %}
      {% do predicates.append('FALSE') %}
  {% endif %}

  {{ sql_header if sql_header is not none }}

  merge into {{ full_target }} as DBT_INTERNAL_DEST
      using {{ full_source }} as DBT_INTERNAL_SOURCE
      on {{ predicates | join(' and ') }}

      when matched then update set
        {% if update_columns -%}{%- for column_name in update_columns %}
            {{ column_name }} = DBT_INTERNAL_SOURCE.{{ column_name }}
            {%- if not loop.last %}, {%- endif %}
        {%- endfor %}
        {%- else %} * {% endif %}

      when not matched then insert ({{ dest_cols_csv }}) values ({{ dbt_glue_source_column_list(dest_columns, source_columns, prefix='DBT_INTERNAL_SOURCE.', alias=false) }})
{% endmacro %}


{% macro dbt_glue_get_incremental_sql(strategy, source, target, unique_key, incremental_predicates, dest_columns=none, source_columns=none) %}
  {%- if strategy == 'append' -%}
    {#-- insert new records into existing table, without updating or overwriting #}
    {{ get_insert_into_sql(source, target, source_columns) }}
  {%- elif strategy == 'insert_overwrite' -%}
    {#-- insert statements don't like CTEs, so support them via a temp view #}
    {{ get_insert_overwrite_sql(source, target, source_columns) }}
  {%- elif strategy == 'merge' -%}
    {{ get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates, source_columns) }}
  {%- else -%}
    {% set no_sql_for_strategy_msg -%}
      No known SQL for the incremental strategy provided: {{ strategy }}
    {%- endset %}
    {%- do exceptions.raise_compiler_error(no_sql_for_strategy_msg) -%}
  {%- endif -%}
{% endmacro %}
