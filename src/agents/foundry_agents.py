"""
Aegis Data Foundry — Autonomous Agents
========================================

Four cooperating agents drive the self-healing pipeline:

* ``ScoutAgent``     — profiles a dataset and discovers data-quality issues.
* ``SentinelAgent``  — turns a profile into a weighted, explainable quality score.
* ``HealerAgent``    — applies deterministic, auditable fixes for known issue types.
* ``OracleAgent``    — flags statistical anomalies and detects distribution drift
                        between two snapshots of the same table (new in Aegis).

Every agent returns plain dict / DataFrame structures (no hidden global state),
which keeps them trivially composable inside a Prefect flow or a notebook.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import numpy as np
import polars as pl

try:
    from sklearn.ensemble import IsolationForest
except ImportError:  # pragma: no cover - sklearn is a hard requirement, guarded for clarity
    IsolationForest = None


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class Issue:
    """A single, structured data-quality finding."""

    category: str
    column: str | None
    severity: str  # "low" | "medium" | "high"
    description: str
    affected_rows: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "category": self.category,
            "column": self.column,
            "severity": self.severity,
            "description": self.description,
            "affected_rows": self.affected_rows,
            "metadata": self.metadata,
        }


SEVERITY_WEIGHT = {"low": 1, "medium": 3, "high": 6}


# ---------------------------------------------------------------------------
# ScoutAgent — discovery
# ---------------------------------------------------------------------------

class ScoutAgent:
    """Autonomously profiles a Polars DataFrame and surfaces data-quality issues.

    Discovery categories:
        - nulls              missing values per column
        - duplicates         fully duplicated rows
        - whitespace         leading/trailing/irregular whitespace in strings
        - negative_values     negative numbers in columns that should be >= 0
        - outliers           IQR-based outliers in numeric columns
        - inconsistent_case  mixed casing for categorical/string columns
        - format_violations  values that don't match an expected regex (emails, phone, etc.)
        - schema_drift       column type/name differences vs. a reference schema
    """

    NON_NEGATIVE_HINTS = ("price", "amount", "qty", "quantity", "total", "cost", "revenue", "age")
    EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

    def profile_dataset(self, df: pl.DataFrame, dataset_name: str,
                         reference_schema: dict[str, str] | None = None) -> dict[str, Any]:
        issues: list[Issue] = []
        column_stats: dict[str, Any] = {}
        n_rows = df.height

        issues += self._check_nulls(df)
        issues += self._check_duplicates(df)
        issues += self._check_whitespace(df)
        issues += self._check_negative_values(df)
        issues += self._check_outliers(df)
        issues += self._check_inconsistent_case(df)
        issues += self._check_format_violations(df)
        if reference_schema:
            issues += self._check_schema_drift(df, reference_schema)

        for col in df.columns:
            column_stats[col] = self._column_stats(df, col)

        categories = sorted({i.category for i in issues})

        return {
            "dataset_name": dataset_name,
            "profiled_at": _now(),
            "row_count": n_rows,
            "column_count": df.width,
            "columns": df.columns,
            "column_stats": column_stats,
            "issues_detected": [i.to_dict() for i in issues],
            "issue_count": len(issues),
            "issue_categories": categories,
            "category_count": len(categories),
        }

    # -- individual checks --------------------------------------------------

    def _column_stats(self, df: pl.DataFrame, col: str) -> dict[str, Any]:
        s = df[col]
        stats: dict[str, Any] = {
            "dtype": str(s.dtype),
            "null_count": int(s.null_count()),
            "null_pct": round(100 * s.null_count() / max(df.height, 1), 2),
            "unique_count": int(s.n_unique()),
        }
        if s.dtype.is_numeric():
            stats.update({
                "min": s.min(),
                "max": s.max(),
                "mean": round(s.mean(), 4) if s.mean() is not None else None,
                "std": round(s.std(), 4) if s.std() is not None else None,
            })
        return stats

    def _check_nulls(self, df: pl.DataFrame) -> list[Issue]:
        out = []
        for col in df.columns:
            n_null = df[col].null_count()
            if n_null > 0:
                pct = 100 * n_null / max(df.height, 1)
                severity = "high" if pct > 20 else ("medium" if pct > 5 else "low")
                out.append(Issue(
                    category="nulls", column=col, severity=severity,
                    description=f"{n_null} null value(s) ({pct:.1f}%) in '{col}'",
                    affected_rows=n_null, metadata={"null_pct": round(pct, 2)},
                ))
        return out

    def _check_duplicates(self, df: pl.DataFrame) -> list[Issue]:
        n_dupes = df.height - df.unique().height
        if n_dupes > 0:
            return [Issue(
                category="duplicates", column=None,
                severity="high" if n_dupes > df.height * 0.05 else "medium",
                description=f"{n_dupes} fully duplicated row(s) detected",
                affected_rows=n_dupes,
            )]
        return []

    def _check_whitespace(self, df: pl.DataFrame) -> list[Issue]:
        out = []
        for col in df.columns:
            if df[col].dtype != pl.Utf8:
                continue
            stripped_mismatch = df.select(
                (pl.col(col) != pl.col(col).str.strip_chars()).sum().alias("n")
            )["n"][0]
            if stripped_mismatch:
                out.append(Issue(
                    category="whitespace", column=col, severity="low",
                    description=f"{stripped_mismatch} value(s) with leading/trailing whitespace in '{col}'",
                    affected_rows=int(stripped_mismatch),
                ))
        return out

    def _check_negative_values(self, df: pl.DataFrame) -> list[Issue]:
        out = []
        for col in df.columns:
            if not df[col].dtype.is_numeric():
                continue
            if not any(h in col.lower() for h in self.NON_NEGATIVE_HINTS):
                continue
            n_neg = df.select((pl.col(col) < 0).sum().alias("n"))["n"][0]
            if n_neg:
                out.append(Issue(
                    category="negative_values", column=col, severity="medium",
                    description=f"{n_neg} negative value(s) in '{col}' where negatives are implausible",
                    affected_rows=int(n_neg),
                ))
        return out

    def _check_outliers(self, df: pl.DataFrame) -> list[Issue]:
        out = []
        for col in df.columns:
            if not df[col].dtype.is_numeric():
                continue
            series = df[col].drop_nulls()
            if series.len() < 8:
                continue
            q1, q3 = series.quantile(0.25), series.quantile(0.75)
            if q1 is None or q3 is None:
                continue
            iqr = q3 - q1
            if iqr == 0:
                continue
            lower, upper = q1 - 1.5 * iqr, q3 + 1.5 * iqr
            n_out = series.filter((series < lower) | (series > upper)).len()
            if n_out:
                out.append(Issue(
                    category="outliers", column=col, severity="medium",
                    description=f"{n_out} statistical outlier(s) in '{col}' (IQR method)",
                    affected_rows=n_out,
                    metadata={"lower_bound": round(float(lower), 3), "upper_bound": round(float(upper), 3)},
                ))
        return out

    def _check_inconsistent_case(self, df: pl.DataFrame) -> list[Issue]:
        out = []
        for col in df.columns:
            if df[col].dtype != pl.Utf8:
                continue
            vals = df[col].drop_nulls()
            if vals.len() == 0:
                continue
            n_variants = vals.str.to_lowercase().n_unique()
            n_actual = vals.n_unique()
            if n_actual > n_variants:
                out.append(Issue(
                    category="inconsistent_case", column=col, severity="low",
                    description=f"'{col}' has {n_actual - n_variants} case-variant duplicate value(s)",
                    affected_rows=int(n_actual - n_variants),
                ))
        return out

    def _check_format_violations(self, df: pl.DataFrame) -> list[Issue]:
        out = []
        for col in df.columns:
            if df[col].dtype != pl.Utf8:
                continue
            if "email" not in col.lower():
                continue
            vals = df[col].drop_nulls()
            if vals.len() == 0:
                continue
            bad = sum(1 for v in vals.to_list() if not self.EMAIL_RE.match(v))
            if bad:
                out.append(Issue(
                    category="format_violations", column=col, severity="medium",
                    description=f"{bad} malformed email value(s) in '{col}'",
                    affected_rows=bad,
                ))
        return out

    def _check_schema_drift(self, df: pl.DataFrame, reference_schema: dict[str, str]) -> list[Issue]:
        out = []
        current = {c: str(df[c].dtype) for c in df.columns}
        for col, expected_type in reference_schema.items():
            if col not in current:
                out.append(Issue(
                    category="schema_drift", column=col, severity="high",
                    description=f"Expected column '{col}' is missing from the dataset",
                ))
            elif current[col] != expected_type:
                out.append(Issue(
                    category="schema_drift", column=col, severity="medium",
                    description=f"'{col}' type changed: expected {expected_type}, found {current[col]}",
                ))
        return out


# ---------------------------------------------------------------------------
# SentinelAgent — scoring
# ---------------------------------------------------------------------------

class SentinelAgent:
    """Converts a ScoutAgent profile into a weighted 0-100 quality score
    across four dimensions, and keeps a rolling history for trend reporting.
    """

    DIMENSION_CATEGORIES = {
        "completeness": {"nulls"},
        "validity": {"format_violations", "negative_values", "schema_drift"},
        "consistency": {"inconsistent_case", "whitespace"},
        "uniqueness": {"duplicates"},
    }

    def __init__(self) -> None:
        self._history: list[dict[str, Any]] = []

    def calculate_quality_score(self, profile: dict[str, Any]) -> dict[str, Any]:
        issues = profile["issues_detected"]
        row_count = max(profile["row_count"], 1)

        dimension_scores: dict[str, float] = {}
        for dimension, categories in self.DIMENSION_CATEGORIES.items():
            penalty = sum(
                SEVERITY_WEIGHT[i["severity"]] * (1 + i["affected_rows"] / row_count)
                for i in issues if i["category"] in categories
            )
            dimension_scores[dimension] = round(max(0.0, 100.0 - penalty * 2.5), 2)

        overall = round(sum(dimension_scores.values()) / len(dimension_scores), 2)

        result = {
            "dataset_name": profile["dataset_name"],
            "scored_at": _now(),
            "overall_score": overall,
            "dimension_scores": dimension_scores,
            "issue_count": profile["issue_count"],
            "status": self._status_label(overall),
        }

        trend = None
        prior = [h for h in self._history if h["dataset_name"] == profile["dataset_name"]]
        if prior:
            trend = round(overall - prior[-1]["overall_score"], 2)
        result["trend"] = trend

        self._history.append(result)
        return result

    def history_for(self, dataset_name: str) -> list[dict[str, Any]]:
        return [h for h in self._history if h["dataset_name"] == dataset_name]

    @staticmethod
    def _status_label(score: float) -> str:
        if score >= 90:
            return "excellent"
        if score >= 75:
            return "good"
        if score >= 50:
            return "needs_attention"
        return "critical"


# ---------------------------------------------------------------------------
# HealerAgent — remediation
# ---------------------------------------------------------------------------

class HealerAgent:
    """Applies deterministic, auditable fixes for issues discovered by ScoutAgent.

    Every remediation is logged as an action so the pipeline keeps a full
    audit trail of what was changed, on which column, and how many rows
    were affected — this feeds the lineage log.
    """

    def auto_remediate(self, df: pl.DataFrame, issues: list[dict[str, Any]]
                        ) -> tuple[pl.DataFrame, list[dict[str, Any]]]:
        actions: list[dict[str, Any]] = []
        working = df

        for issue in issues:
            category = issue["category"]
            col = issue.get("column")

            if category == "whitespace" and col:
                working = working.with_columns(pl.col(col).str.strip_chars())
                actions.append(self._action("trim_whitespace", col, issue["affected_rows"]))

            elif category == "duplicates":
                before = working.height
                working = working.unique()
                actions.append(self._action("drop_duplicates", None, before - working.height))

            elif category == "negative_values" and col:
                before = working.select((pl.col(col) < 0).sum().alias("n"))["n"][0]
                working = working.with_columns(
                    pl.when(pl.col(col) < 0).then(pl.col(col).abs()).otherwise(pl.col(col)).alias(col)
                )
                actions.append(self._action("fix_negative_values", col, int(before)))

            elif category == "inconsistent_case" and col:
                working = working.with_columns(pl.col(col).str.to_lowercase().alias(col))
                actions.append(self._action("normalize_case", col, issue["affected_rows"]))

            elif category == "nulls" and col:
                if working[col].dtype.is_numeric():
                    median = working[col].median()
                    if median is not None:
                        n_filled = working[col].null_count()
                        working = working.with_columns(pl.col(col).fill_null(median))
                        actions.append(self._action("impute_median", col, n_filled))
                elif working[col].dtype == pl.Utf8:
                    n_filled = working[col].null_count()
                    working = working.with_columns(pl.col(col).fill_null("unknown"))
                    actions.append(self._action("impute_placeholder", col, n_filled))

            elif category == "outliers" and col:
                bounds = issue.get("metadata", {})
                lower, upper = bounds.get("lower_bound"), bounds.get("upper_bound")
                if lower is not None and upper is not None:
                    working = working.with_columns(
                        pl.col(col).clip(lower, upper).alias(col)
                    )
                    actions.append(self._action("clip_outliers", col, issue["affected_rows"]))

        return working, actions

    @staticmethod
    def _action(action_type: str, column: str | None, rows_affected: int) -> dict[str, Any]:
        return {
            "action": action_type,
            "column": column,
            "rows_affected": int(rows_affected),
            "applied_at": _now(),
        }


# ---------------------------------------------------------------------------
# OracleAgent — anomaly & drift detection (new in Aegis Data Foundry)
# ---------------------------------------------------------------------------

class OracleAgent:
    """Statistical anomaly detection and cross-run drift detection.

    This agent did not exist in earlier single-agent data-cleaning tools —
    it closes the loop by watching for *new* kinds of problems (outlier
    rows via IsolationForest, and shifting distributions release-over-release)
    rather than only the fixed rule set ScoutAgent already knows about.
    """

    def __init__(self, contamination: float = 0.03, random_state: int = 42) -> None:
        self.contamination = contamination
        self.random_state = random_state

    def detect_anomalies(self, df: pl.DataFrame, numeric_columns: list[str] | None = None
                          ) -> dict[str, Any]:
        if IsolationForest is None:
            return {"error": "scikit-learn is required for anomaly detection", "anomaly_count": 0}

        numeric_columns = numeric_columns or [
            c for c in df.columns if df[c].dtype.is_numeric()
        ]
        numeric_columns = [c for c in numeric_columns if df[c].null_count() == 0]
        if not numeric_columns or df.height < 10:
            return {"anomaly_count": 0, "anomaly_indices": [], "columns_used": numeric_columns}

        matrix = df.select(numeric_columns).to_numpy()
        model = IsolationForest(contamination=self.contamination, random_state=self.random_state)
        labels = model.fit_predict(matrix)
        anomaly_indices = np.where(labels == -1)[0].tolist()

        return {
            "anomaly_count": len(anomaly_indices),
            "anomaly_pct": round(100 * len(anomaly_indices) / df.height, 2),
            "anomaly_indices": anomaly_indices[:200],
            "columns_used": numeric_columns,
            "detected_at": _now(),
        }

    def detect_drift(self, baseline_profile: dict[str, Any], current_profile: dict[str, Any],
                      threshold_pct: float = 15.0) -> dict[str, Any]:
        """Compares two ScoutAgent column_stats blocks and flags columns whose
        mean or null-rate has moved by more than ``threshold_pct``.
        """
        drifted: list[dict[str, Any]] = []
        base_stats = baseline_profile.get("column_stats", {})
        cur_stats = current_profile.get("column_stats", {})

        for col, cur in cur_stats.items():
            base = base_stats.get(col)
            if not base:
                continue
            for metric in ("mean", "null_pct"):
                b_val, c_val = base.get(metric), cur.get(metric)
                if b_val is None or c_val is None:
                    continue
                denom = abs(b_val) if b_val != 0 else 1
                pct_change = abs(c_val - b_val) / denom * 100
                if pct_change > threshold_pct:
                    drifted.append({
                        "column": col, "metric": metric,
                        "baseline": b_val, "current": c_val,
                        "pct_change": round(pct_change, 2),
                    })

        return {
            "drift_detected": len(drifted) > 0,
            "drifted_metrics": drifted,
            "checked_at": _now(),
        }
