# Multi-Source Connectors

Aegis's agents and medallion layers never need to know where a
DataFrame came from — that's the point of `src/connectors/`. Every
source implements one method (`extract() -> pl.DataFrame`), so adding
a new source later doesn't touch the pipeline or the agents at all.

## Built-in sources

| Source     | Class            | Requires                          |
|------------|------------------|-------------------------------------|
| CSV        | `CSVSource`      | nothing extra (default, unchanged)  |
| PostgreSQL | `PostgresSource` | `requirements-extra.txt` (SQLAlchemy + psycopg2-binary) |
| MySQL      | `MySQLSource`    | `requirements-extra.txt` (SQLAlchemy + PyMySQL)         |
| S3         | `S3Source`       | `requirements-extra.txt` (boto3)                        |

## Using a connector directly

```python
from src.connectors import get_source

df = get_source("csv", path="data/raw/orders.csv").extract()

df = get_source(
    "postgres", host="db.internal", database="orders",
    user="readonly", password="...", table="orders",
).extract()

df = get_source(
    "s3", bucket="my-data-lake", key="orders/2026-07-22.parquet",
    file_format="parquet",
).extract()
```

## Wiring a connector into the pipeline

`run_foundry_pipeline()` accepts an optional `source_type` / `source_config`
pair. Leave both unset and behavior is identical to before (reads
`raw_path` as a CSV):

```python
from src.orchestration.foundry_flows import run_foundry_pipeline

run_foundry_pipeline(
    dataset_name="orders",
    source_type="postgres",
    source_config={
        "host": "db.internal", "database": "orders",
        "user": "readonly", "password": "...", "table": "orders",
    },
)
```

Or configure it entirely through environment variables (see
`config/settings.py` for the full list) and read from `config.settings`:

```bash
export AEGIS_SOURCE_TYPE=postgres
export AEGIS_PG_HOST=db.internal
export AEGIS_PG_DATABASE=orders
export AEGIS_PG_USER=readonly
export AEGIS_PG_PASSWORD=***
export AEGIS_PG_TABLE=orders
```

```python
from config import settings
from src.orchestration.foundry_flows import run_foundry_pipeline

run_foundry_pipeline(
    source_type=settings.SOURCE_TYPE,
    source_config=settings.SOURCE_CONFIG[settings.SOURCE_TYPE],
)
```

## Failure behavior

Every connector raises `src.connectors.ConnectorError` on failure —
never a bare exception — so orchestration and alerting code can catch
it specifically. If an optional dependency (SQLAlchemy, boto3) isn't
installed, `extract()` raises a `ConnectorError` with the exact
`pip install` command to fix it, rather than an `ImportError` traceback.

## Adding a new source

Subclass `src.connectors.base.DataSource`, implement `extract()`, and
register it in `src/connectors/registry.py`. That's the entire surface
area — nothing else in the codebase needs to change.
