# API Reference

## `src.agents`

### `ScoutAgent`
```python
ScoutAgent().profile_dataset(df: pl.DataFrame, dataset_name: str,
                              reference_schema: dict[str, str] | None = None) -> dict
```

### `SentinelAgent`
```python
sentinel = SentinelAgent()
sentinel.calculate_quality_score(profile: dict) -> dict
sentinel.history_for(dataset_name: str) -> list[dict]
```

### `HealerAgent`
```python
HealerAgent().auto_remediate(df: pl.DataFrame, issues: list[dict]) -> tuple[pl.DataFrame, list[dict]]
```

### `OracleAgent`
```python
oracle = OracleAgent(contamination=0.03, random_state=42)
oracle.detect_anomalies(df: pl.DataFrame, numeric_columns: list[str] | None = None) -> dict
oracle.detect_drift(baseline_profile: dict, current_profile: dict, threshold_pct: float = 15.0) -> dict
```

---

## `src.database.MedallionVault`

```python
vault = MedallionVault(db_path: str = "data/foundry.duckdb")

vault.load_to_bronze(df: pl.DataFrame, table_name: str) -> None
vault.promote_to_silver(source_table: str, target_table: str,
                         cleaned_df: pl.DataFrame, agent: str = "HealerAgent") -> None
vault.promote_to_gold(target_table: str, query: str, source_table: str = "") -> pl.DataFrame

vault.read_table(schema: str, table_name: str) -> pl.DataFrame
vault.list_tables(schema: str) -> list[str]
vault.query(sql: str) -> pl.DataFrame

vault.record_run(run_id, dataset_name, stage, row_count,
                  overall_score, issue_count, actions_taken) -> None
vault.lineage_for(table_name: str) -> pl.DataFrame
vault.quality_history(dataset_name: str | None = None) -> pl.DataFrame

vault.close() -> None
```

---

## `src.orchestration.foundry_flows`

```python
run_foundry_pipeline(dataset_name: str = "orders",
                      raw_path: str = "data/raw/orders.csv",
                      db_path: str = "data/foundry.duckdb") -> dict
```

Returns:
```python
{
  "run_id": "...",
  "profile": {...},           # ScoutAgent output
  "score": {...},              # SentinelAgent output
  "remediation_actions": [...],# HealerAgent actions
  "anomalies": {...},          # OracleAgent output
  "gold_rows": 7
}
```

Run from the CLI:
```bash
python -m src.orchestration.foundry_flows
```

---

## Example: end-to-end in a script

```python
import polars as pl
from src.agents import ScoutAgent, SentinelAgent, HealerAgent, OracleAgent
from src.database import MedallionVault

df = pl.read_csv("data/raw/orders.csv")

profile = ScoutAgent().profile_dataset(df, "orders")
score = SentinelAgent().calculate_quality_score(profile)
clean_df, actions = HealerAgent().auto_remediate(df, profile["issues_detected"])
anomalies = OracleAgent().detect_anomalies(clean_df)

vault = MedallionVault("data/foundry.duckdb")
vault.load_to_bronze(df, "orders")
vault.promote_to_silver("orders", "orders_clean", clean_df)
gold = vault.promote_to_gold(
    "orders_by_category",
    "SELECT category, SUM(amount) AS total FROM silver.orders_clean GROUP BY category",
)
vault.close()
```
