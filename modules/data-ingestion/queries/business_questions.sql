-- Business questions answered against the DuckDB analytics layer.

-- Q1: How many distinct active users are in the current snapshot (not deleted)?
SELECT count(*) AS active_users
FROM clean.users;

-- Q2: What percentage of active users use Gmail (@gmail.com) as their email provider?
SELECT
    round(
        100.0 * count(*) FILTER (WHERE email ILIKE '%@gmail.com') / count(*),
        2
    ) AS gmail_pct
FROM clean.users;

-- Q3: Which are the top 3 countries by number of Gmail users?
SELECT
    country,
    count(*) AS gmail_users
FROM clean.users
WHERE email ILIKE '%@gmail.com'
GROUP BY country
ORDER BY gmail_users DESC
LIMIT 3;

-- Q4a: How many users changed their email address at least once during the captured period?
WITH email_events AS (
    SELECT
        user_id,
        email,
        source_timestamp,
        lag(email) OVER (
            PARTITION BY user_id
            ORDER BY source_timestamp
        ) AS prev_email
    FROM staging.stg_cdc_events
    WHERE change_type IN ('INSERT', 'UPDATE')
      AND email IS NOT NULL
)
SELECT count(DISTINCT user_id) AS users_with_email_change
FROM email_events
WHERE prev_email IS NOT NULL
  AND email != prev_email;

-- Q4b: What are the top 5 email domain transitions (e.g., outlook.com -> gmail.com)?
WITH email_events AS (
    SELECT
        email,
        lag(email) OVER (
            PARTITION BY user_id
            ORDER BY source_timestamp
        ) AS prev_email
    FROM staging.stg_cdc_events
    WHERE change_type IN ('INSERT', 'UPDATE')
      AND email IS NOT NULL
),
transitions AS (
    SELECT
        split_part(prev_email, '@', 2) || ' -> ' || split_part(email, '@', 2) AS transition
    FROM email_events
    WHERE prev_email IS NOT NULL
      AND split_part(email, '@', 2) != split_part(prev_email, '@', 2)
)
SELECT transition, count(*) AS occurrences
FROM transitions
GROUP BY transition
ORDER BY occurrences DESC
LIMIT 5;

-- Q5: What is the average time span (in minutes) between the first and last CDC event
--     for users who have more than one event?
SELECT
    round(
        avg(date_diff('minute', first_event, last_event)),
        2
    ) AS avg_minutes_first_to_last
FROM (
    SELECT
        min(source_timestamp) AS first_event,
        max(source_timestamp) AS last_event
    FROM staging.stg_cdc_events
    GROUP BY user_id
    HAVING count(*) > 1
) t;
