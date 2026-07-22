# Observability, Alerting, Streaming & Natural-Language Querying

## Metrics

`src.observability.get_metrics()` returns a process-wide `PipelineMetrics`
singleton. `run_foundry_pipeline()` records to it automatically after
every run — no configuration required.

- If `prometheus-client` is installed (`requirements-extra.txt`), metrics
  are real Prometheus Gauges/Counters/Histograms. Serve them with a tiny
  wrapper:

  ```python
  from src.observability import get_metrics
  # e.g. inside a Flask/FastAPI /metrics route:
  return get_metrics().latest_metrics_text(), 200, {"Content-Type": "text/plain"}
  ```

- If it isn't installed, the same calls work against an in-memory
  fallback — `get_metrics().snapshot()` returns a plain dict, which the
  dashboard's **📡 Observability** page displays directly.

Metrics recorded per run: quality score, rows processed, issues
detected, remediations applied, anomalies flagged, pipeline duration,
and a running total of runs by outcome.

## Alerting

`src.observability.AlertManager` checks each run's quality score and
anomaly rate against configurable thresholds and posts a Slack-style
webhook message when either is crossed.

```bash
export AEGIS_ALERT_WEBHOOK_URL=https://hooks.slack.com/services/...
export AEGIS_ALERT_QUALITY_THRESHOLD=75      # default
export AEGIS_ALERT_ANOMALY_PCT_THRESHOLD=10  # default
```

With no webhook configured, `AlertManager.enabled` is `False` and
`check()` still runs but only logs — it never blocks or fails a
pipeline run. `run_foundry_pipeline()` builds a default `AlertManager`
from these env vars automatically; pass your own instance via the
`alert_manager=` kwarg to override thresholds per-call.

## Streaming ingestion

`src.streaming.StreamWatcher` watches a directory for new files and
calls a callback per file — the natural fit is triggering a pipeline
run per arriving file:

```python
from src.streaming import StreamWatcher
from src.orchestration.foundry_flows import run_foundry_pipeline

def handle_new_file(path: str) -> None:
    run_foundry_pipeline(dataset_name="orders", raw_path=path)

watcher = StreamWatcher("data/incoming", on_new_file=handle_new_file)
watcher.start()   # non-blocking, runs in a background thread
...
watcher.stop()
```

Uses `watchdog` (event-driven) if installed via `requirements-extra.txt`;
otherwise falls back to polling every `poll_interval_seconds` — no hard
dependency either way. Files already sitting in the directory when
`start()` is called are treated as already-ingested, matching normal
streaming semantics.

## Natural-language querying (QueryAgent)

`src.nlquery.QueryAgent` lets you ask a plain-English question and get
back the SQL Claude generated, an explanation, and the query result —
the dashboard's **🤖 Ask Aegis** page is a thin wrapper around this.

```python
from src.database import MedallionVault
from src.nlquery import QueryAgent

vault = MedallionVault("data/foundry.duckdb")
agent = QueryAgent(vault)  # reads ANTHROPIC_API_KEY from the environment
result = agent.ask("Which category has the highest average order value?")
print(result["sql"])          # the generated SQL
print(result["explanation"])  # one-sentence explanation from the model
print(result["result"])       # a Polars DataFrame
```

**Safety model:** only `SELECT` statements are ever executed — the
generated SQL is checked against an allowlist/denylist before it
touches the vault, and INSERT/UPDATE/DELETE/DROP/ALTER/CREATE/ATTACH
statements are rejected outright, as are stacked (`;`-separated)
statements. The model sees table/column names and types when composing
a query — never row-level data.

Requires `ANTHROPIC_API_KEY` and the `anthropic` package
(`requirements-extra.txt`). Without a key, `QueryAgent.ask()` raises
`QueryAgentError` with a clear message rather than crashing the
dashboard — the **🤖 Ask Aegis** page reflects this back to the user.
