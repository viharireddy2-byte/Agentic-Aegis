"""
MedallionVault
===============

A thin, opinionated wrapper around DuckDB that implements the Bronze / Silver
/ Gold medallion pattern, plus two Aegis-specific additions:

* ``pipeline_runs``   — every run's quality score + remediation actions, so the
                         dashboard can chart quality trends over time.
* ``data_lineage``     — a row-level event log ("bronze.orders -> silver.orders
                         via HealerAgent: trim_whitespace") for audit purposes.

DuckDB is used in-process (no server), and Polars DataFrames pass through the
Arrow interface with zero-copy registration wherever possible.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import polars as pl


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class MedallionVault:
    """DuckDB-backed store for the Bronze / Silver / Gold layers."""

    def __init__(self, db_path: str = "data/foundry.duckdb"):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self._init_schemas()

    def _init_schemas(self) -> None:
        for schema in ("bronze", "silver", "gold"):
            self.conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id VARCHAR,
                dataset_name VARCHAR,
                stage VARCHAR,
                row_count BIGINT,
                overall_score DOUBLE,
                issue_count INTEGER,
                actions_taken INTEGER,
                recorded_at VARCHAR
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS data_lineage (
                event_id VARCHAR,
                source VARCHAR,
                target VARCHAR,
                agent VARCHAR,
                action VARCHAR,
                rows_affected BIGINT,
                recorded_at VARCHAR
            )
        """)

    # -- layer loads ----------------------------------------------------

    def load_to_bronze(self, df: pl.DataFrame, table_name: str) -> None:
        """Loads raw data as-is into the Bronze layer. Immutable, append-only."""
        arrow_tbl = df.to_arrow()  # noqa: F841 - kept alive for duckdb registration
        self.conn.register("_incoming", arrow_tbl)
        self.conn.execute(f"CREATE OR REPLACE TABLE bronze.{table_name} AS SELECT * FROM _incoming")
        self.conn.unregister("_incoming")
        self._log_lineage("raw_source", f"bronze.{table_name}", "IngestPipeline", "load_bronze", df.height)

    def promote_to_silver(self, source_table: str, target_table: str,
                           cleaned_df: pl.DataFrame, agent: str = "HealerAgent") -> None:
        """Writes the cleaned/validated DataFrame into the Silver layer."""
        arrow_tbl = cleaned_df.to_arrow()  # noqa: F841
        self.conn.register("_clean", arrow_tbl)
        self.conn.execute(f"CREATE OR REPLACE TABLE silver.{target_table} AS SELECT * FROM _clean")
        self.conn.unregister("_clean")
        self._log_lineage(f"bronze.{source_table}", f"silver.{target_table}", agent,
                           "promote_silver", cleaned_df.height)

    def promote_to_gold(self, target_table: str, query: str, source_table: str = "") -> pl.DataFrame:
        """Runs an aggregation query against Silver and materializes it as Gold."""
        self.conn.execute(f"CREATE OR REPLACE TABLE gold.{target_table} AS {query}")
        result = self.conn.execute(f"SELECT * FROM gold.{target_table}").pl()
        self._log_lineage(f"silver.{source_table}" if source_table else "silver.*",
                           f"gold.{target_table}", "AggregationEngine", "promote_gold", result.height)
        return result

    # -- reads ------------------------------------------------------------

    def read_table(self, schema: str, table_name: str) -> pl.DataFrame:
        return self.conn.execute(f"SELECT * FROM {schema}.{table_name}").pl()

    def list_tables(self, schema: str) -> list[str]:
        rows = self.conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = ?", [schema]
        ).fetchall()
        return [r[0] for r in rows]

    def query(self, sql: str) -> pl.DataFrame:
        return self.conn.execute(sql).pl()

    # -- observability ------------------------------------------------------

    def record_run(self, run_id: str, dataset_name: str, stage: str, row_count: int,
                    overall_score: float, issue_count: int, actions_taken: int) -> None:
        self.conn.execute(
            "INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [run_id, dataset_name, stage, row_count, overall_score, issue_count,
             actions_taken, _now()],
        )

    def _log_lineage(self, source: str, target: str, agent: str, action: str, rows_affected: int) -> None:
        import uuid
        self.conn.execute(
            "INSERT INTO data_lineage VALUES (?, ?, ?, ?, ?, ?, ?)",
            [str(uuid.uuid4()), source, target, agent, action, rows_affected, _now()],
        )

    def lineage_for(self, table_name: str) -> pl.DataFrame:
        return self.conn.execute(
            "SELECT * FROM data_lineage WHERE source LIKE ? OR target LIKE ? ORDER BY recorded_at",
            [f"%{table_name}%", f"%{table_name}%"],
        ).pl()

    def quality_history(self, dataset_name: str | None = None) -> pl.DataFrame:
        if dataset_name:
            return self.conn.execute(
                "SELECT * FROM pipeline_runs WHERE dataset_name = ? ORDER BY recorded_at",
                [dataset_name],
            ).pl()
        return self.conn.execute("SELECT * FROM pipeline_runs ORDER BY recorded_at").pl()

    def close(self) -> None:
        self.conn.close()
