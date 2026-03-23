-- Fails if any user_id present in clean.users had DELETE as its latest raw event.
-- clean.users should never contain a user whose latest event is a DELETE.
with latest_events as (
    select
        json_extract_string(payload, '$._id') as user_id,
        change_type,
        row_number() over (
            partition by json_extract_string(payload, '$._id')
            order by source_timestamp desc
        ) as rn
    from {{ source('raw', 'cdc_events') }}
),
deleted_users as (
    select user_id from latest_events where rn = 1 and change_type = 'DELETE'
)
select u.user_id
from {{ ref('users') }} u
inner join deleted_users d on u.user_id = d.user_id
