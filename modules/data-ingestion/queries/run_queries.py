"""Run business questions against DuckDB and print tabular results.

Usage:
    python queries/run_queries.py <db_path>

Note: json_extract_string() is used instead of payload->>'key' in WHERE/PARTITION
clauses because DuckDB's ->> operator has an operator-precedence conflict with
IS NOT NULL in those positions on this version.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb

QUERIES = [
    (
        "Q1: Active user count",
        "SELECT count(*) AS active_users FROM clean.users",
    ),
    (
        "Q2: Gmail user percentage",
        """
        SELECT round(
            100.0 * count(*) FILTER (WHERE email ILIKE '%@gmail.com') / count(*),
            2
        ) AS gmail_pct
        FROM clean.users
        """,
    ),
    (
        "Q3: Top 3 countries by Gmail user count",
        """
        SELECT country, count(*) AS gmail_users
        FROM clean.users
        WHERE email ILIKE '%@gmail.com'
        GROUP BY country
        ORDER BY gmail_users DESC
        LIMIT 3
        """,
    ),
    (
        "Q4a: Users who changed email at least once",
        """
        WITH email_events AS (
            SELECT
                json_extract_string(payload, '$._id')                   AS user_id,
                json_extract_string(payload, '$.email')                 AS email,
                source_timestamp,
                lag(json_extract_string(payload, '$.email')) OVER (
                    PARTITION BY json_extract_string(payload, '$._id')
                    ORDER BY source_timestamp
                )                                                       AS prev_email
            FROM raw.cdc_events
            WHERE change_type IN ('INSERT', 'UPDATE')
              AND json_extract_string(payload, '$.email') IS NOT NULL
        )
        SELECT count(DISTINCT user_id) AS users_with_email_change
        FROM email_events
        WHERE prev_email IS NOT NULL AND email != prev_email
        """,
    ),
    (
        "Q4b: Top 5 email domain transitions",
        """
        WITH email_events AS (
            SELECT
                json_extract_string(payload, '$.email')                 AS email,
                lag(json_extract_string(payload, '$.email')) OVER (
                    PARTITION BY json_extract_string(payload, '$._id')
                    ORDER BY source_timestamp
                )                                                       AS prev_email
            FROM raw.cdc_events
            WHERE change_type IN ('INSERT', 'UPDATE')
              AND json_extract_string(payload, '$.email') IS NOT NULL
        ),
        transitions AS (
            SELECT split_part(prev_email, '@', 2) || ' -> ' || split_part(email, '@', 2) AS transition
            FROM email_events
            WHERE prev_email IS NOT NULL AND email != prev_email
        )
        SELECT transition, count(*) AS occurrences
        FROM transitions
        GROUP BY transition
        ORDER BY occurrences DESC
        LIMIT 5
        """,
    ),
    (
        "Q5: Avg minutes between first and last event (users with >1 event)",
        """
        SELECT round(
            avg(date_diff('minute', first_event, last_event)),
            2
        ) AS avg_minutes_first_to_last
        FROM (
            SELECT min(source_timestamp) AS first_event, max(source_timestamp) AS last_event
            FROM raw.cdc_events
            GROUP BY json_extract_string(payload, '$._id')
            HAVING count(*) > 1
        ) t
        """,
    ),
]


def _print_table(label: str, cols: list[str], rows: list[tuple]) -> None:
    widths = [
        max(len(c), max((len(str(v)) for v in (r[i] for r in rows)), default=0))
        for i, c in enumerate(cols)
    ]
    sep = "  ".join("-" * w for w in widths)
    header = "  ".join(c.ljust(w) for c, w in zip(cols, widths))

    print(f"\n{'=' * 60}")
    print(label)
    print("=" * 60)
    print(header)
    print(sep)
    for row in rows:
        print("  ".join(str(v).ljust(w) for v, w in zip(row, widths)))


def main() -> None:
    if len(sys.argv) != 2:
        sys.exit(f"Usage: {sys.argv[0]} <db_path>")

    db_path = sys.argv[1]
    if not Path(db_path).exists():
        sys.exit(f"Database not found: {db_path}")

    conn = duckdb.connect(db_path, read_only=True)
    try:
        for label, sql in QUERIES:
            rel = conn.execute(sql)
            cols = [d[0] for d in rel.description]
            rows = rel.fetchall()
            _print_table(label, cols, rows)
    finally:
        conn.close()

    print()


if __name__ == "__main__":
    main()
