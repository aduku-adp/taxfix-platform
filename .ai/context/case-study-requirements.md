# Data Platform Engineer — Case Study

> **Taxfix** makes tax filing simple, fast, and accessible for millions of users across Europe.
>
> This case study is part of our interview process for the Data Platform team.

## Context

At Taxfix, our data platform ingests data from multiple sources — application databases, third-party APIs, and event streams. A critical part of our infrastructure is **Change Data Capture (CDC)**: we capture every INSERT, UPDATE, and DELETE from our application databases as a stream of events, then process them into analytics-ready tables.

In this case study, you will work with a realistic CDC dataset and build a local data pipeline using primarily **Airflow** and **DuckDB**.

## The Data

You are provided with CDC events captured from a MongoDB change stream for a `users` collection. The events are stored as **partitioned JSONL files**:

```
data/
  users/
    YYYY/MM/DD/HH/
      mm/
        events-YYYYMMDD_HHMMSS.jsonl
```

Each line is a JSON object representing a single database change event.

**Key fields:**

| Field | Description |
|-------|-------------|
| `uuid` | Unique event identifier |
| `source_timestamp` | When the change occurred in MongoDB |
| `read_timestamp` | When the CDC connector captured the event |
| `source_metadata.change_type` | `INSERT`, `UPDATE`, or `DELETE` |
| `payload` | The document fields (full document for INSERT/UPDATE, only `_id` for DELETE) |

**Important characteristics of the data:**

- **INSERT** and **UPDATE** events contain the full document in `payload`
- **DELETE** events contain only the `_id` in `payload`

The dataset captures a window of live database activity — some records existed **before** the capture window began.

## Part 1: Build the Data Pipeline

**Goal:** Load the raw CDC events into DuckDB, create clean current state of each user, and apply appropriate anonymization.

**Requirements:**

1. Create a **raw layer** that holds all CDC events
2. Create a **clean layer** that represents the current state of each user by correctly applying INSERT, UPDATE, and DELETE events in chronological order
3. Anonymize date of birth by substituting it with a ten-year age group (e.g., a birthdate of 1991-04-12 becomes `[30-40]`)
4. Document any assumptions or edge cases you encounter

**Optional:**

5. Protect fields you consider sensitive in the clean layer from being available by default (e.g., names, full email addresses)

**Deliverable:** Python code that builds the DuckDB database with at least two layers (raw + clean).

## Part 2: Answer Business Questions

Using SQL queries against your DuckDB database, answer the following questions. **Each question should be answered with a single SQL query.**

1. **How many distinct users are in your active snapshot** (i.e., not deleted)?

2. **What percentage of active users use Gmail** (`@gmail.com`) as their email provider?

3. **Which are the top 3 countries by number of Gmail users?**

4. **How many users changed their email address** at least once during the captured period? What are the **top 5 email domain transitions** (e.g., `outlook.com -> gmail.com`)?

5. **What is the average time span** (in minutes) between the first and last CDC event for users who have more than one event?

**Deliverable:** SQL queries and their results (in README, a notebook, or printed output).

## Part 3: Production Architecture *(Discussion)*

Be prepared to discuss how you would take this pipeline to production. In production, assume a cloud data warehouse (e.g., Snowflake, BigQuery) replaces DuckDB. We're interested in your thinking on:

- **Orchestration & compute:** How would you schedule and monitor the pipeline?
- **Late-arriving and out-of-order events:** How would you handle them?
- **Schema evolution:** A new field appears in the payload starting next week. What happens?
- **Backfills and reprocessing:** How do you re-process historical data when a bug is found?
- **Idempotency:** How do you ensure re-running the pipeline produces the same result?
- **Scaling:** The users collection grows to 50M records with 10M change events per day. What changes?
- **Latency:** How quickly can data be delivered to consumers? What techniques could reduce end-to-end latency?

*No code required for this part — this will be discussed during the interview.*

## Part 4: Data Privacy & AI Readiness *(Discussion)*

Be prepared to discuss:

- What **anonymization or pseudonymization** would you apply to make this data safe for analytics and AI use cases?
- How would you implement a **"right to be forgotten"** (data deletion request) across raw and derived layers of the pipeline?
- What are the main **risks** in using personal data in AI services, and what **safeguards** would you put in place before doing so?

*No code required for this part — this will be discussed during the interview.*

## Submission Guidelines

- Implement your solution as a **Python project**
- Use **DuckDB** as your analytical database
- Use **Airflow** as your orchestration engine
- **Containerize with Docker** — provide clear instructions to build and run
- Include a **README** with:
  - Setup and run instructions
  - Design decisions and trade-offs
  - Any assumptions you made
  - Whether you used AI assistance and how
- Submit your solution as a **git repository** (GitHub or GitLab)

## What We Evaluate

| Area | What we look for |
|------|------------------|
| **CDC Understanding** | Correct handling of INSERT/UPDATE/DELETE semantics, event ordering, edge cases |
| **Data Modeling** | Clean layering (raw → snapshot), appropriate schema design |
| **SQL Proficiency** | Correct, efficient queries; proper use of window functions, CTEs, ranking |
| **Code Quality** | Readability, structure, error handling, testing |
| **Production Thinking** | Realistic architecture proposals, awareness of failure modes and scalability |
| **Privacy & AI Awareness** | Understanding of PII, anonymization techniques, regulatory requirements |

## Time Advice

Aim to spend **4–6 hours** on implementation (Parts 1 & 2). Parts 3 & 4 are discussion-only — prepare your thoughts but no code is needed.

If you face issues with any part, explain in the README how you would solve it.

## AI Tools

We acknowledge that AI coding assistants — such as GitHub Copilot, Cursor, or Claude — can meaningfully boost developer productivity. You are welcome to use such tools during this case study.

In your README, please note whether you used any AI assistance during implementation, and briefly describe how it helped.



---

*Please feel free to reach out if you have any questions. Good luck!*


## Getting Started: Download the Data

https://drive.google.com/file/d/1HH7i51_Ug9XGRIgG_Vlm3yXA4OYlqN6G/view?usp=sharing

The data file `cdc-data.tar.gz`  is in the same shared folder as this notebook. Download it and extract.
