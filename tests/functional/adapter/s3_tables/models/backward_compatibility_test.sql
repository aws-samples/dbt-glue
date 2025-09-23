{{ config(
    materialized="incremental",
    incremental_strategy="merge", 
    file_format="s3tables",
    unique_key="id"
) }}

-- Backward compatibility test for S3 tables
-- Ensures S3 tables work alongside existing functionality
select 
    1 as id,
    'backward_compatibility' as test_type,
    's3tables' as file_format,
    'merge' as strategy,
    current_timestamp() as created_at

{% if is_incremental() %}
-- Test incremental logic compatibility
where 1=1
{% endif %}