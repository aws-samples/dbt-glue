{{ config(
    materialized="incremental",
    incremental_strategy="merge", 
    file_format="s3tables",
    unique_key="id"
) }}

-- Comprehensive test for merge strategy with S3 tables
-- Tests various data scenarios and edge cases
select 
    case 
        when {{ var('test_scenario', 'initial') }} = 'initial' then row_number() over (order by 1)
        when {{ var('test_scenario', 'initial') }} = 'update' then row_number() over (order by 1)
        when {{ var('test_scenario', 'initial') }} = 'mixed' then 
            case when row_number() over (order by 1) <= 2 then row_number() over (order by 1)
                 else row_number() over (order by 1) + 10
            end
        else row_number() over (order by 1) + 100
    end as id,
    case 
        when {{ var('test_scenario', 'initial') }} = 'initial' then 'initial_' || row_number() over (order by 1)
        when {{ var('test_scenario', 'initial') }} = 'update' then 'updated_' || row_number() over (order by 1)
        when {{ var('test_scenario', 'initial') }} = 'mixed' then 'mixed_' || row_number() over (order by 1)
        else 'new_' || row_number() over (order by 1)
    end as name,
    '{{ var('test_scenario', 'initial') }}' as scenario,
    current_timestamp() as updated_at
from (
    select 1 union all select 2 union all select 3
) t(dummy)
{% if is_incremental() %}
-- This will be handled by the merge strategy
{% endif %}