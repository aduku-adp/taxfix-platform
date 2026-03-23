with ranked as (
    select
        *,
        row_number() over (
            partition by user_id
            order by source_timestamp desc
        ) as rn
    from {{ ref('stg_cdc_events') }}
)

select
    user_id,
    email,
    first_name,
    last_name,
    country,
    case
        when date_of_birth is not null then
            '['
            || (floor(date_diff('year', date_of_birth, current_date) / 10) * 10)::int
            || '-'
            || ((floor(date_diff('year', date_of_birth, current_date) / 10) * 10) + 10)::int
            || ']'
    end                                 as age_group,
    created_at,
    source_timestamp                    as updated_at
from ranked
where rn = 1
  and change_type != 'DELETE'
