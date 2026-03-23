-- Fails if any email is null or does not contain exactly one @ character.
select user_id
from {{ ref('users') }}
where email is null
   or length(email) - length(replace(email, '@', '')) != 1
