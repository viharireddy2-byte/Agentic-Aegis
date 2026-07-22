# Agents Deep-Dive

## ScoutAgent — discovery

`ScoutAgent.profile_dataset(df, dataset_name, reference_schema=None) -> dict`

Runs eight independent checks against a Polars DataFrame and returns:

```python
{
  "dataset_name": "orders",
  "profiled_at": "2026-...Z",
  "row_count": 1020,
  "column_count": 8,
  "columns": [...],
  "column_stats": {...},          # per-column dtype, nulls, min/max/mean/std
  "issues_detected": [ {category, column, severity, description, affected_rows, metadata}, ... ],
  "issue_count": 9,
  "issue_categories": [...],
  "category_count": 7
}
```

| Category | Detection method |
|---|---|
| `nulls` | Per-column null count / percentage |
| `duplicates` | Full-row duplicate count via `df.unique()` |
| `whitespace` | String columns where the trimmed value differs from the raw value |
| `negative_values` | Numeric columns whose name suggests non-negativity (price, amount, qty, age, ...) but contain negatives |
| `outliers` | IQR method (1.5× IQR fences) on numeric columns |
| `inconsistent_case` | String columns where lower-casing reduces the distinct-value count |
| `format_violations` | Regex validation for columns whose name contains "email" |
| `schema_drift` | Optional: compares current dtypes/columns against a supplied reference schema |

Severity is assigned per-check (`low` / `medium` / `high`) based on the
share of affected rows.

## SentinelAgent — scoring

`SentinelAgent.calculate_quality_score(profile) -> dict`

Maps each issue category to one of four quality dimensions:

| Dimension | Categories |
|---|---|
| Completeness | nulls |
| Validity | format_violations, negative_values, schema_drift |
| Consistency | inconsistent_case, whitespace |
| Uniqueness | duplicates |

Each dimension score starts at 100 and is reduced by a severity-weighted,
row-share-scaled penalty:

```
penalty = severity_weight × (1 + affected_rows / total_rows)
dimension_score = max(0, 100 - sum(penalty) × 2.5)
overall_score = mean(dimension_scores)
```

`SentinelAgent` keeps an in-memory history for the life of the process and
reports a `trend` (delta vs. the previous run for the same dataset name).
Cross-process history is read from `MedallionVault.quality_history()`.

## HealerAgent — remediation

`HealerAgent.auto_remediate(df, issues) -> (clean_df, actions)`

One deterministic fix per issue category:

| Category | Fix applied |
|---|---|
| whitespace | `str.strip_chars()` |
| duplicates | `df.unique()` |
| negative_values | absolute value |
| inconsistent_case | lower-case normalization |
| nulls (numeric) | median imputation |
| nulls (string) | `"unknown"` placeholder |
| outliers | clip to the IQR bounds reported by ScoutAgent |

Every fix produces an `action` record (`action`, `column`, `rows_affected`,
`applied_at`) that feeds both the pipeline's return value and, when run
through the orchestration flow, the `data_lineage` table.

## OracleAgent — anomaly & drift detection

`OracleAgent.detect_anomalies(df, numeric_columns=None) -> dict`

Fits a scikit-learn `IsolationForest` (default `contamination=0.03`) across
all fully-populated numeric columns and returns the indices flagged as
outlying multivariate points — catching combinations of values that look
fine individually but are jointly unusual (e.g. a very high amount *and* an
unusually high quantity together).

`OracleAgent.detect_drift(baseline_profile, current_profile, threshold_pct=15.0) -> dict`

Compares two `ScoutAgent` profiles' `column_stats` blocks and flags any
column whose `mean` or `null_pct` moved by more than `threshold_pct`
between runs — useful for catching upstream schema or business-logic
changes before they silently corrupt the Gold layer.
