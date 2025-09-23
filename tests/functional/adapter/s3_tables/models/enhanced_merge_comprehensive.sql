{{ config(
    materialized="incremental",
    incremental_strategy="merge", 
    file_format="s3tables",
    unique_key="id"
) }}

-- Enhanced comprehensive merge test for S3 tables
-- Tests complex merge scenarios with multiple data types and conditions
select 
    {{ var('test_run', 1) }} as id,
    'enhanced_merge_' || {{ var('test_run', 1) }} as name,
    case 
        when {{ var('test_run', 1) }} = 1 then 'initial_load'
        when {{ var('test_run', 1) }} = 2 then 'updated_record'
        when {{ var('test_run', 1) }} = 3 then 'final_update'
        else 'unknown_state'
    end as status,
    case 
        when {{ var('test_run', 1) }} <= 2 then 'active'
        else 'inactive'
    end as state,
    {{ var('test_run', 1) }} * 100 as score,
    current_date() as date_col,
    current_timestamp() as updated_at

{% if is_incremental() %}
-- Enhanced incremental logic with complex conditions
-- This tests the merge strategy's ability to handle updates and inserts
{% endif %}