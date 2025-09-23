{{ config(
    materialized="incremental",
    incremental_strategy="merge", 
    file_format="s3tables",
    unique_key="id"
) }}
select 
    {{ var('run_number', 1) }} as run_id,
    case 
        when {{ var('run_number', 1) }} = 1 then row_number() over (order by 1)
        else row_number() over (order by 1) + 10
    end as id,
    'merge_row_' || row_number() over (order by 1) as name,
    current_timestamp() as updated_at
from (
    select 1 union all select 2 union all select 3
) t(dummy)
{% if is_incremental() %}
-- In incremental runs, we'll update existing records and add new ones
{% endif %}