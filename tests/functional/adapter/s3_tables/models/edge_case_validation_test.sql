{{ config(
    materialized="incremental",
    incremental_strategy="merge", 
    file_format="s3tables",
    unique_key="id"
) }}

-- Edge case validation test for S3 tables
-- Tests boundary conditions and special scenarios
select 
    case 
        when {{ var('edge_case', 'normal') }} = 'null_values' then null
        when {{ var('edge_case', 'normal') }} = 'zero_values' then 0
        when {{ var('edge_case', 'normal') }} = 'negative_values' then -1 * row_number() over (order by 1)
        when {{ var('edge_case', 'normal') }} = 'large_values' then 999999 + row_number() over (order by 1)
        else row_number() over (order by 1)
    end as id,
    case 
        when {{ var('edge_case', 'normal') }} = 'empty_strings' then ''
        when {{ var('edge_case', 'normal') }} = 'null_strings' then cast(null as string)
        when {{ var('edge_case', 'normal') }} = 'special_chars' then 'test_!@#$%^&*()_+{}|:<>?[]\\;'',./'
        when {{ var('edge_case', 'normal') }} = 'unicode' then 'test_ñáéíóú_中文_🚀'
        else 'normal_test_' || row_number() over (order by 1)
    end as name,
    case 
        when {{ var('edge_case', 'normal') }} = 'past_date' then date('1900-01-01')
        when {{ var('edge_case', 'normal') }} = 'future_date' then date('2100-12-31')
        when {{ var('edge_case', 'normal') }} = 'null_date' then cast(null as date)
        else current_date()
    end as date_col,
    '{{ var('edge_case', 'normal') }}' as test_case
from (
    select 1 union all select 2
) t(dummy)
{% if is_incremental() %}
-- Edge case handling in incremental mode
{% endif %}