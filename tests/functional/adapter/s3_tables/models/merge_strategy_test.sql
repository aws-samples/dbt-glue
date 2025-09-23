{{ config(
    materialized="incremental",
    incremental_strategy="merge", 
    file_format="s3tables",
    unique_key="id"
) }}

-- Simple test model to verify merge strategy works with S3 tables
select 
    1 as id,
    'test_record' as name,
    current_date() as date_col,
    current_timestamp() as updated_at

{% if is_incremental() %}
-- This condition will be used in incremental runs
where 1=1
{% endif %}