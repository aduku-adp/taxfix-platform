with source as (
    select * from {{ source('raw', 'cdc_events') }}
)

select
    payload->>'_id'                                         as user_id,
    change_type,
    source_timestamp,
    read_timestamp,
    payload->>'email'                                       as email,
    payload->>'firstname'                                   as first_name,
    payload->>'lastname'                                    as last_name,
    json_extract_string(payload, '$.address.country')       as country,
    try_cast(payload->>'birthday' as date)                  as date_of_birth,
    try_cast(payload->>'created_at' as timestamptz)         as created_at
from source
