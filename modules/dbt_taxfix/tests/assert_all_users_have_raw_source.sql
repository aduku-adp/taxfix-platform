-- Fails if any user_id in clean.users has no corresponding row in raw.cdc_events.
select u.user_id
from {{ ref('users') }} u
left join {{ source('raw', 'cdc_events') }} r
    on u.user_id = json_extract_string(r.payload, '$._id')
where r.uuid is null
