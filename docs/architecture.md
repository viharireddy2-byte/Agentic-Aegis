# Architecture

## Overview

Agentic Aegis has three logical layers:

1. **Agentic Control Layer** — four stateless-by-default agent classes
   (`ScoutAgent`, `SentinelAgent`, `HealerAgent`, `OracleAgent`) that operate
   purely on Polars DataFrames and plain dict/list structures in and out.
   None of them hold a database connection; this keeps them independently
   unit-testable and reusable outside Prefect (e.g. in a notebook).

2. **Data Processing Layer** — the medallion pattern:
   - **Bronze**: exact copy of the source data, loaded as-is, never mutated.
   - **Silver**: the HealerAgent's output — deduplicated, typed, validated.
   - **Gold**: SQL aggregates computed from Silver, tuned for dashboard reads.

3. **Storage Layer** — `MedallionVault`, a thin wrapper around a single
   DuckDB file with three schemas (`bronze`, `silver`, `gold`) plus two
   observability tables: `pipeline_runs` (quality score history) and
   `data_lineage` (every promotion event, who/what/how many rows).

## Data flow

```
CSV/JSON/Parquet
   │
   ▼
extract() ─────────────────────────► raw Polars DataFrame
   │
   ▼
ScoutAgent.profile_dataset() ───────► profile dict (issues, column stats)
   │
   ▼
SentinelAgent.calculate_quality_score() ► score dict (0-100, per dimension, trend)
   │
   ▼
HealerAgent.auto_remediate() ───────► (clean DataFrame, actions list)
   │
   ▼
OracleAgent.detect_anomalies() ─────► anomaly dict (IsolationForest)
   │
   ├──► MedallionVault.load_to_bronze(raw_df)
   ├──► MedallionVault.promote_to_silver(clean_df)
   ├──► MedallionVault.promote_to_gold(sql_aggregation)
   └──► MedallionVault.record_run(score, actions, issue_count)
```

## Orchestration

`src/orchestration/foundry_flows.py` wires the above into a single Prefect
`@flow` (`run_foundry_pipeline`), with each stage as a retryable `@task`.
Prefect gives us:

- Automatic retries on the extract step (transient file/network issues)
- Structured, timestamped logging per task via `get_run_logger()`
- A clean mental model: tasks are pure functions, the flow is the wiring

## State management

There is no global mutable state. Each pipeline run:

- Creates its own `run_id` (UUID)
- Creates a fresh `SentinelAgent()` instance so score history is scoped
  to that flow run's in-memory object (persisted history for
  cross-run trends lives in DuckDB's `pipeline_runs` table, not in the
  agent's memory)
- Opens one `MedallionVault` connection, used for the duration of the run,
  then closed

This mirrors the reference project's approach: agents are computation, the
database is the only durable state.

## Why DuckDB in-process (not Postgres/Snowflake)

- Zero infrastructure: the whole platform runs on a laptop with no server.
- Arrow-native: Polars ⇄ DuckDB conversions are (near) zero-copy.
- SQL when you want it, DataFrame API when you don't — both hit the same
  storage.

## Extending the pipeline

- **New issue category**: add a `_check_*` method to `ScoutAgent`, append
  its `Issue` list, and (optionally) add a fix branch in `HealerAgent`.
- **New data source**: replace the `extract()` task in
  `foundry_flows.py` — everything downstream is source-agnostic since it
  only depends on receiving a Polars DataFrame.
- **New Gold aggregate**: add another `@task` calling
  `vault.promote_to_gold(name, sql)` with your own query.
