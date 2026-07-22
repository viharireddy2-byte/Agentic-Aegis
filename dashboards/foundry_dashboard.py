"""
Agentic Aegis — Streamlit Dashboard
============================================

Run with:  streamlit run dashboards/foundry_dashboard.py

Pages:
  Overview · Bronze Explorer · Silver Analytics · Gold Insights ·
  Quality Monitoring · Data Lineage · Pipeline Performance ·
  Data Connectors · Ask Aegis · Observability
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.database import MedallionVault  # noqa: E402
from src.observability import get_metrics  # noqa: E402

DB_PATH = "data/foundry.duckdb"

st.set_page_config(page_title="Agentic Aegis", page_icon="🛡️", layout="wide")


@st.cache_resource
def get_vault() -> MedallionVault:
    return MedallionVault(DB_PATH)


def _safe_read(vault: MedallionVault, schema: str, table: str) -> pl.DataFrame | None:
    try:
        return vault.read_table(schema, table)
    except Exception:
        return None


def page_overview(vault: MedallionVault) -> None:
    st.title("🛡️ Aegis Data Foundry")
    st.caption("Self-healing ETL — autonomous agents keep your medallion pipeline clean.")

    runs = vault.quality_history()
    col1, col2, col3, col4 = st.columns(4)

    if runs.height > 0:
        latest = runs.tail(1).to_dicts()[0]
        col1.metric("Latest Quality Score", f"{latest['overall_score']:.1f}/100")
        col2.metric("Rows Processed", f"{latest['row_count']:,}")
        col3.metric("Issues Found (last run)", latest["issue_count"])
        col4.metric("Actions Taken (last run)", latest["actions_taken"])
    else:
        col1.metric("Latest Quality Score", "—")
        col2.metric("Rows Processed", "—")
        col3.metric("Issues Found", "—")
        col4.metric("Actions Taken", "—")
        st.info("No pipeline runs recorded yet. Run `python -m src.orchestration.foundry_flows` first.")

    st.subheader("Quality Score Trend")
    if runs.height > 1:
        st.line_chart(runs.to_pandas().set_index("recorded_at")["overall_score"])
    else:
        st.caption("Run the pipeline a few times to see a trend line.")

    st.subheader("Medallion Layers")
    b, s, g = st.columns(3)
    b.markdown("### 🥉 Bronze\nImmutable raw data, full audit trail.")
    s.markdown("### 🥈 Silver\nDeduplicated, validated, cleaned by HealerAgent.")
    g.markdown("### 🥇 Gold\nBusiness-ready aggregates, dashboard-ready.")


def page_bronze(vault: MedallionVault) -> None:
    st.title("🥉 Bronze Layer Explorer")
    tables = vault.list_tables("bronze")
    if not tables:
        st.warning("No Bronze tables yet. Run the pipeline first.")
        return
    table = st.selectbox("Table", tables)
    df = vault.read_table("bronze", table)
    st.write(f"{df.height:,} rows × {df.width} columns")
    st.dataframe(df.to_pandas(), use_container_width=True)


def page_silver(vault: MedallionVault) -> None:
    st.title("🥈 Silver Layer Analytics")
    tables = vault.list_tables("silver")
    if not tables:
        st.warning("No Silver tables yet. Run the pipeline first.")
        return
    table = st.selectbox("Table", tables)
    df = vault.read_table("silver", table)
    st.write(f"{df.height:,} rows × {df.width} columns")
    st.dataframe(df.to_pandas(), use_container_width=True)

    numeric_cols = [c for c in df.columns if df[c].dtype.is_numeric()]
    if numeric_cols:
        col = st.selectbox("Chart a numeric column", numeric_cols)
        st.bar_chart(df.to_pandas()[col])


def page_gold(vault: MedallionVault) -> None:
    st.title("🥇 Gold Layer Insights")
    tables = vault.list_tables("gold")
    if not tables:
        st.warning("No Gold tables yet. Run the pipeline first.")
        return
    table = st.selectbox("Table", tables)
    df = vault.read_table("gold", table)
    st.dataframe(df.to_pandas(), use_container_width=True)

    if "total_revenue" in df.columns:
        label_col = df.columns[0]
        st.bar_chart(df.to_pandas().set_index(label_col)["total_revenue"])


def page_quality(vault: MedallionVault) -> None:
    st.title("📊 Quality Monitoring")
    runs = vault.quality_history()
    if runs.height == 0:
        st.warning("No quality history yet. Run the pipeline first.")
        return
    st.dataframe(runs.to_pandas(), use_container_width=True)
    st.subheader("Score Over Time")
    st.line_chart(runs.to_pandas().set_index("recorded_at")["overall_score"])
    st.subheader("Issues vs. Actions Taken")
    st.bar_chart(runs.to_pandas().set_index("recorded_at")[["issue_count", "actions_taken"]])


def page_lineage(vault: MedallionVault) -> None:
    st.title("🔍 Data Lineage")
    df = vault.query("SELECT * FROM data_lineage ORDER BY recorded_at DESC")
    if df.height == 0:
        st.warning("No lineage events recorded yet.")
        return
    st.dataframe(df.to_pandas(), use_container_width=True)


def page_performance(vault: MedallionVault) -> None:
    st.title("⚙️ Pipeline Performance")
    runs = vault.quality_history()
    if runs.height == 0:
        st.warning("No runs recorded yet.")
        return
    st.metric("Total Pipeline Runs", runs.height)
    st.metric("Average Quality Score", f"{runs.to_pandas()['overall_score'].mean():.1f}/100")
    st.metric("Average Rows / Run", f"{runs.to_pandas()['row_count'].mean():.0f}")
    st.dataframe(runs.to_pandas(), use_container_width=True)


def page_connectors(vault: MedallionVault) -> None:
    st.title("🔌 Data Connectors")
    st.caption("Aegis can pull from any of these sources without touching the "
               "agents or the medallion layers — swap `AEGIS_SOURCE_TYPE` and go.")

    from config import settings

    for name, label, extra in [
        ("csv", "CSV (default)", "requirements.txt"),
        ("postgres", "PostgreSQL", "requirements-extra.txt"),
        ("mysql", "MySQL / MariaDB", "requirements-extra.txt"),
        ("s3", "Amazon S3", "requirements-extra.txt"),
    ]:
        active = settings.SOURCE_TYPE == name
        with st.container(border=True):
            cols = st.columns([3, 2, 2])
            cols[0].markdown(f"**{label}**")
            cols[1].markdown("🟢 Active" if active else "⚪ Available")
            cols[2].caption(f"needs: `{extra}`")

    st.divider()
    st.subheader("Current configuration")
    st.json(settings.SOURCE_CONFIG.get(settings.SOURCE_TYPE, {}))
    st.caption("Set `AEGIS_SOURCE_TYPE` plus the matching `AEGIS_<SOURCE>_*` "
               "env vars to switch — see `docs/connectors.md`.")


def page_ask_aegis(vault: MedallionVault) -> None:
    st.title("🤖 Ask Aegis")
    st.caption("Ask a question in plain English — answered against the Silver/Gold layers.")

    try:
        from src.nlquery import QueryAgent, QueryAgentError
    except ImportError:
        st.warning("The `anthropic` package isn't installed. Run "
                   "`pip install -r requirements-extra.txt` to enable this page.")
        return

    question = st.text_input("Your question",
                              placeholder="Which category has the highest average order value?")
    if st.button("Ask", type="primary") and question:
        agent = QueryAgent(vault)
        try:
            with st.spinner("Thinking..."):
                result = agent.ask(question)
        except QueryAgentError as exc:
            st.error(str(exc))
            return
        st.markdown(f"**{result['explanation']}**")
        st.code(result["sql"], language="sql")
        st.dataframe(result["result"].to_pandas(), use_container_width=True)


def page_observability(vault: MedallionVault) -> None:
    st.title("📡 Observability")
    st.caption("Live process metrics for this dashboard/pipeline process, "
               "plus the alerting configuration currently in effect.")

    from config import settings

    metrics = get_metrics()
    snapshot = metrics.snapshot()
    st.subheader("Metrics backend")
    st.write(f"**{snapshot['backend']}**" +
             (" — scrape `/metrics` in production" if snapshot["backend"] == "prometheus_client"
              else " — install `prometheus-client` for a real `/metrics` endpoint"))

    if snapshot["backend"] == "prometheus_client":
        st.code(metrics.latest_metrics_text(), language="text")
    else:
        st.json(snapshot["values"])

    st.subheader("Alerting")
    if settings.ALERT_WEBHOOK_URL:
        st.success("Webhook alerting is configured.")
    else:
        st.info("No `AEGIS_ALERT_WEBHOOK_URL` configured — alerts are logged, not sent.")
    st.write(f"Quality-score threshold: **{settings.ALERT_QUALITY_THRESHOLD}**")
    st.write(f"Anomaly-rate threshold: **{settings.ALERT_ANOMALY_PCT_THRESHOLD}%**")


PAGES = {
    "🏠 Overview": page_overview,
    "🥉 Bronze Explorer": page_bronze,
    "🥈 Silver Analytics": page_silver,
    "🥇 Gold Insights": page_gold,
    "📊 Quality Monitoring": page_quality,
    "🔍 Data Lineage": page_lineage,
    "⚙️ Pipeline Performance": page_performance,
    "🔌 Data Connectors": page_connectors,
    "🤖 Ask Aegis": page_ask_aegis,
    "📡 Observability": page_observability,
}


def main() -> None:
    vault = get_vault()
    st.sidebar.title("🛡️ Agentic Aegis")
    choice = st.sidebar.radio("Navigate", list(PAGES.keys()))
    st.sidebar.divider()
    st.sidebar.caption("Autonomous agents: Scout · Sentinel · Healer · Oracle")
    st.sidebar.caption("Connectors, alerting, and NL querying are additive — "
                       "see 🔌 Data Connectors, 📡 Observability, 🤖 Ask Aegis.")
    PAGES[choice](vault)


if __name__ == "__main__":
    main()
