"""
Agentic Aegis — Prefect Orchestration
=============================================

Wires the four agents and the MedallionVault into a single, observable
Prefect flow:

    extract -> Scout (profile) -> Sentinel (score) -> Healer (remediate)
             -> Oracle (anomalies + drift) -> Bronze -> Silver -> Gold

Run directly:  python -m src.orchestration.foundry_flows
"""

from __future__ import annotations

import time
import uuid
from pathlib import Path

import polars as pl
from prefect import flow, get_run_logger, task

from src.agents import HealerAgent, OracleAgent, ScoutAgent, SentinelAgent
from src.connectors import ConnectorError, get_source
from src.database import MedallionVault
from src.observability import AlertManager, get_metrics

RAW_DATA_PATH = "data/raw/orders.csv"
DB_PATH = "data/foundry.duckdb"


@task(name="extract-raw-data", retries=2, retry_delay_seconds=5)
def extract(path: str = RAW_DATA_PATH) -> pl.DataFrame:
    """Original CSV extraction path — unchanged, kept for backward
    compatibility with anything importing ``extract`` directly."""
    logger = get_run_logger()
    if not Path(path).exists():
        raise FileNotFoundError(
            f"No raw data found at {path}. Run `python scripts/generate_sample_data.py` first."
        )
    df = pl.read_csv(path, try_parse_dates=True)
    logger.info(f"Extracted {df.height:,} rows from {path}")
    return df


@task(name="extract-from-source", retries=2, retry_delay_seconds=5)
def extract_from_source(source_type: str, source_config: dict) -> pl.DataFrame:
    """Source-agnostic extraction via the connectors registry — this is
    what lets the same pipeline read from CSV, Postgres, MySQL, or S3.
    Raises ``ConnectorError`` (not a bare Exception) on failure so callers
    can distinguish a bad source from a bug elsewhere in the flow.
    """
    logger = get_run_logger()
    source = get_source(source_type, **source_config)
    try:
        df = source.extract()
    except ConnectorError:
        raise
    except Exception as exc:  # noqa: BLE001 - normalize into ConnectorError
        raise ConnectorError(f"Extraction failed for {source.describe()}: {exc}") from exc
    logger.info(f"Extracted {df.height:,} rows via {source_type} source ({source.describe()})")
    return df


@task(name="scout-profile")
def profile_step(df: pl.DataFrame, dataset_name: str) -> dict:
    logger = get_run_logger()
    profile = ScoutAgent().profile_dataset(df, dataset_name)
    logger.info(f"Scout found {profile['issue_count']} issue(s) across "
                f"{profile['category_count']} categories")
    return profile


@task(name="sentinel-score")
def score_step(profile: dict, sentinel: SentinelAgent) -> dict:
    logger = get_run_logger()
    score = sentinel.calculate_quality_score(profile)
    logger.info(f"Sentinel quality score: {score['overall_score']}/100 ({score['status']})")
    return score


@task(name="healer-remediate")
def remediate_step(df: pl.DataFrame, profile: dict) -> tuple[pl.DataFrame, list[dict]]:
    logger = get_run_logger()
    clean_df, actions = HealerAgent().auto_remediate(df, profile["issues_detected"])
    logger.info(f"Healer applied {len(actions)} remediation action(s)")
    return clean_df, actions


@task(name="oracle-anomalies")
def anomaly_step(df: pl.DataFrame) -> dict:
    logger = get_run_logger()
    result = OracleAgent().detect_anomalies(df)
    logger.info(f"Oracle flagged {result.get('anomaly_count', 0)} anomalous row(s)")
    return result


@task(name="load-bronze")
def load_bronze(vault: MedallionVault, df: pl.DataFrame, table_name: str) -> None:
    vault.load_to_bronze(df, table_name)


@task(name="promote-silver")
def load_silver(vault: MedallionVault, table_name: str, clean_df: pl.DataFrame) -> None:
    vault.promote_to_silver(table_name, f"{table_name}_clean", clean_df)


@task(name="promote-gold")
def build_gold(vault: MedallionVault, table_name: str) -> pl.DataFrame:
    query = f"""
        SELECT
            category,
            COUNT(*)               AS order_count,
            ROUND(SUM(amount), 2)  AS total_revenue,
            ROUND(AVG(amount), 2)  AS avg_order_value
        FROM silver.{table_name}_clean
        GROUP BY category
        ORDER BY total_revenue DESC
    """
    return vault.promote_to_gold(f"{table_name}_by_category", query, source_table=f"{table_name}_clean")


@flow(name="aegis-foundry-pipeline", log_prints=True)
def run_foundry_pipeline(dataset_name: str = "orders", raw_path: str = RAW_DATA_PATH,
                          db_path: str = DB_PATH, source_type: str | None = None,
                          source_config: dict | None = None,
                          alert_manager: AlertManager | None = None) -> dict:
    """The complete agentic ETL pipeline, from raw data to Gold-layer aggregates.

    By default this reads a CSV from ``raw_path``, exactly as before.
    Pass ``source_type`` (e.g. ``"postgres"``, ``"mysql"``, ``"s3"``) plus
    a matching ``source_config`` dict to pull from a different connector
    instead — see ``src.connectors.get_source`` and ``config.settings.SOURCE_CONFIG``.

    Metrics are always recorded (in-memory if ``prometheus_client`` isn't
    installed). Alerting only fires if ``alert_manager`` is supplied with
    a configured webhook, or if ``config.settings.ALERT_WEBHOOK_URL`` is
    set and the default manager is used.
    """
    run_id = str(uuid.uuid4())
    print(f"Starting Agentic Aegis pipeline — run {run_id[:8]}")
    started_at = time.perf_counter()

    vault = MedallionVault(db_path)
    sentinel = SentinelAgent()
    metrics = get_metrics()
    if alert_manager is None:
        from config import settings
        alert_manager = AlertManager(
            webhook_url=settings.ALERT_WEBHOOK_URL,
            quality_score_threshold=settings.ALERT_QUALITY_THRESHOLD,
            anomaly_pct_threshold=settings.ALERT_ANOMALY_PCT_THRESHOLD,
        )

    if source_type:
        raw_df = extract_from_source(source_type, source_config or {})
    else:
        raw_df = extract(raw_path)

    profile = profile_step(raw_df, dataset_name)
    score = score_step(profile, sentinel)
    clean_df, actions = remediate_step(raw_df, profile)
    anomalies = anomaly_step(clean_df)

    load_bronze(vault, raw_df, dataset_name)
    load_silver(vault, dataset_name, clean_df)
    gold_df = build_gold(vault, dataset_name)

    vault.record_run(
        run_id=run_id, dataset_name=dataset_name, stage="complete",
        row_count=clean_df.height, overall_score=score["overall_score"],
        issue_count=profile["issue_count"], actions_taken=len(actions),
    )
    vault.close()

    duration_seconds = time.perf_counter() - started_at
    try:
        metrics.record_run(
            dataset_name=dataset_name, row_count=clean_df.height,
            overall_score=score["overall_score"], issue_count=profile["issue_count"],
            actions_taken=len(actions), anomaly_count=anomalies.get("anomaly_count", 0),
            duration_seconds=duration_seconds,
        )
        alert_manager.check(
            dataset_name=dataset_name, overall_score=score["overall_score"],
            issue_count=profile["issue_count"], anomaly_pct=anomalies.get("anomaly_pct", 0.0),
        )
    except Exception:  # noqa: BLE001 - observability must never break the pipeline
        get_run_logger().exception("Observability recording failed; pipeline result unaffected.")

    print(f"Pipeline complete. Quality score {score['overall_score']}/100, "
          f"{len(actions)} remediation action(s), "
          f"{anomalies.get('anomaly_count', 0)} anomaly(ies) flagged, "
          f"{gold_df.height} gold rows produced.")

    return {
        "run_id": run_id,
        "profile": profile,
        "score": score,
        "remediation_actions": actions,
        "anomalies": anomalies,
        "gold_rows": gold_df.height,
        "duration_seconds": round(duration_seconds, 3),
    }


if __name__ == "__main__":
    run_foundry_pipeline()
