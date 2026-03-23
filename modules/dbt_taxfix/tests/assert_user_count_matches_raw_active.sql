-- Fails if clean.users row count differs from the count of raw user_ids
-- whose latest event is not a DELETE.
with raw_active as (
    select json_extract_string(payload, '$._id') as user_id
    from {{ source('raw', 'cdc_events') }}
    qualify row_number() over (
        partition by json_extract_string(payload, '$._id')
        order by source_timestamp desc
    ) = 1
    and change_type != 'DELETE'
),
counts as (
    select
        (select count(*) from {{ ref('users') }})   as clean_count,
        (select count(*) from raw_active)            as raw_active_count
)
select * from counts where clean_count != raw_active_count
