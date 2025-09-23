{{ config(
    materialized="incremental",
    incremental_strategy="merge", 
    file_format="s3tables",
    unique_key="id"
) }}

-- Error boundary and edge case testing for S3 tables
-- Tests various boundary conditions and error scenarios
select 
    case 
        when '{{ var('error_test', 'normal') }}' = 'null_id' then cast(null as int)
        when '{{ var('error_test', 'normal') }}' = 'zero_id' then 0
        when '{{ var('error_test', 'normal') }}' = 'negative_id' then -1
        when '{{ var('error_test', 'normal') }}' = 'large_id' then 999999999
        else 1
    end as id,
    case 
        when '{{ var('error_test', 'normal') }}' = 'empty_string' then ''
        when '{{ var('error_test', 'normal') }}' = 'null_string' then cast(null as string)
        when '{{ var('error_test', 'normal') }}' = 'long_string' then repeat('test_', 1000)
        when '{{ var('error_test', 'normal') }}' = 'special_chars' then 'test!@#$%^&*()_+{}|:<>?[]\\;'',./'
        when '{{ var('error_test', 'normal') }}' = 'unicode' then 'test_ñáéíóú_中文_🚀_العربية'
        else 'normal_test'
    end as name,
    case 
        when '{{ var('error_test', 'normal') }}' = 'past_date' then date('1900-01-01')
        when '{{ var('error_test', 'normal') }}' = 'future_date' then date('2100-12-31')
        when '{{ var('error_test', 'normal') }}' = 'null_date' then cast(null as date)
        else current_date()
    end as date_col,
    '{{ var('error_test', 'normal') }}' as test_case

-- Filter out null IDs unless specifically testing them
{% if var('error_test', 'normal') != 'null_id' %}
where id is not null
{% endif %}

{% if is_incremental() %}
-- Handle incremental edge cases
{% endif %}