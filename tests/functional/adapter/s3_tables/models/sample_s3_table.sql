{{ config(materialized='table', file_format='s3tables') }}

select 
    1 as id,
    'sample_data' as name,
    current_timestamp() as created_at
union all
select 
    2 as id,
    'more_sample_data' as name,
    current_timestamp() as created_at
