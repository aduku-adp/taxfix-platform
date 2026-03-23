-- Fails if the age bucket width is not exactly 10 (e.g. [30-40] is valid, [30-45] is not).
select user_id
from {{ ref('users') }}
where cast(regexp_extract(age_group, '\[(\d+)-\d+\]', 1) as int)
    + 10
    != cast(regexp_extract(age_group, '\[\d+-(\d+)\]', 1) as int)
