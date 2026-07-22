"""Central configuration for Agentic Aegis."""

from __future__ import annotations

import os

# -- storage -----------------------------------------------------------------
DATA_DIR = os.getenv("AEGIS_DATA_DIR", "data")
DB_PATH = os.getenv("AEGIS_DB_PATH", f"{DATA_DIR}/foundry.duckdb")
RAW_DATA_PATH = os.getenv("AEGIS_RAW_PATH", f"{DATA_DIR}/raw/orders.csv")

# -- quality thresholds --------------------------------------------------------
QUALITY_STATUS_THRESHOLDS = {
    "excellent": 90,
    "good": 75,
    "needs_attention": 50,
}

# -- OracleAgent (anomaly detection) --------------------------------------------
ANOMALY_CONTAMINATION = float(os.getenv("AEGIS_ANOMALY_CONTAMINATION", "0.03"))
DRIFT_THRESHOLD_PCT = float(os.getenv("AEGIS_DRIFT_THRESHOLD_PCT", "15.0"))

# -- dashboard -----------------------------------------------------------------
DASHBOARD_TITLE = "Aegis Data Foundry"
DASHBOARD_ICON = "🛡️"

# -- data source (multi-source connectors) --------------------------------------
# Defaults preserve exact current behavior: a plain CSV at RAW_DATA_PATH.
# Set AEGIS_SOURCE_TYPE to "postgres" | "mysql" | "s3" to point the pipeline
# at a different source instead - see src/connectors/registry.py.
SOURCE_TYPE = os.getenv("AEGIS_SOURCE_TYPE", "csv")
SOURCE_CONFIG = {
    "csv": {"path": RAW_DATA_PATH},
    "postgres": {
        "host": os.getenv("AEGIS_PG_HOST", "localhost"),
        "port": int(os.getenv("AEGIS_PG_PORT", "5432")),
        "database": os.getenv("AEGIS_PG_DATABASE", ""),
        "user": os.getenv("AEGIS_PG_USER", ""),
        "password": os.getenv("AEGIS_PG_PASSWORD", ""),
        "table": os.getenv("AEGIS_PG_TABLE") or None,
        "query": os.getenv("AEGIS_PG_QUERY") or None,
    },
    "mysql": {
        "host": os.getenv("AEGIS_MYSQL_HOST", "localhost"),
        "port": int(os.getenv("AEGIS_MYSQL_PORT", "3306")),
        "database": os.getenv("AEGIS_MYSQL_DATABASE", ""),
        "user": os.getenv("AEGIS_MYSQL_USER", ""),
        "password": os.getenv("AEGIS_MYSQL_PASSWORD", ""),
        "table": os.getenv("AEGIS_MYSQL_TABLE") or None,
        "query": os.getenv("AEGIS_MYSQL_QUERY") or None,
    },
    "s3": {
        "bucket": os.getenv("AEGIS_S3_BUCKET", ""),
        "key": os.getenv("AEGIS_S3_KEY", ""),
        "file_format": os.getenv("AEGIS_S3_FORMAT", "csv"),
        "endpoint_url": os.getenv("AEGIS_S3_ENDPOINT_URL") or None,
    },
}

# -- observability (metrics + alerting) ------------------------------------------
METRICS_ENABLED = os.getenv("AEGIS_METRICS_ENABLED", "true").lower() == "true"
ALERT_WEBHOOK_URL = os.getenv("AEGIS_ALERT_WEBHOOK_URL") or None
ALERT_QUALITY_THRESHOLD = float(os.getenv("AEGIS_ALERT_QUALITY_THRESHOLD", "75.0"))
ALERT_ANOMALY_PCT_THRESHOLD = float(os.getenv("AEGIS_ALERT_ANOMALY_PCT_THRESHOLD", "10.0"))

# -- natural-language querying (QueryAgent) --------------------------------------
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY") or None
ANTHROPIC_MODEL = os.getenv("AEGIS_ANTHROPIC_MODEL", "claude-sonnet-4-6")

# -- streaming ingestion ----------------------------------------------------------
STREAMING_WATCH_DIR = os.getenv("AEGIS_STREAM_DIR", f"{DATA_DIR}/incoming")
STREAMING_POLL_INTERVAL_SECONDS = float(os.getenv("AEGIS_STREAM_POLL_SECONDS", "2.0"))
