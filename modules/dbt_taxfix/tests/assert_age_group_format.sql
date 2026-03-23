-- Fails if any non-null age_group does not match the expected [NN-NN] format.
-- age_group is NULL for users whose source birthday was null — that is expected.
select user_id
from {{ ref('users') }}
where age_group is not null
  and not regexp_matches(age_group, '^\[\d+-\d+\]$')
