{{ config(materialized='view') }}

select
    user_id,
    split_part(email, '@', 2)   as email_domain,
    country,
    age_group,
    created_at,
    updated_at
from {{ ref('users') }}
