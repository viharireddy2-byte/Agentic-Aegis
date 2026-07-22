"""
PipelineMetrics — Prometheus-compatible metrics for pipeline runs
====================================================================

If ``prometheus_client`` is installed (``requirements-extra.txt``), every
metric below is a real Prometheus Gauge/Counter/Histogram and
``latest_metrics_text()`` returns the standard exposition format you can
serve from ``/metrics`` or scrape directly. If it isn't installed, the
same calls still work against a tiny in-memory fallback — so calling
code never has to branch on whether observability is "on".

This makes metrics recording safe to call unconditionally from
``foundry_flows.py`` without adding a hard dependency.
"""

from __future__ import annotations

import threading
from typing import Any

try:
    from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest
    _HAS_PROMETHEUS = True
except ImportError:  # pragma: no cover - optional dependency
    _HAS_PROMETHEUS = False


class _FallbackMetric:
    """Minimal stand-in for a Prometheus metric when the library is absent."""

    def __init__(self, name: str):
        self.name = name
        self._value = 0.0
        self._lock = threading.Lock()

    def set(self, value: float) -> None:
        with self._lock:
            self._value = value

    def inc(self, amount: float = 1.0) -> None:
        with self._lock:
            self._value += amount

    def observe(self, value: float) -> None:
        with self._lock:
            self._value = value

    def get(self) -> float:
        return self._value


class PipelineMetrics:
    """Records the metrics that matter for an ETL pipeline's health.

    Metrics exposed:
        aegis_quality_score          (gauge)  latest Sentinel score, 0-100
        aegis_rows_processed_total   (counter) rows processed across all runs
        aegis_issues_detected_total  (counter) Scout issues found across all runs
        aegis_remediations_total     (counter) Healer actions applied across all runs
        aegis_anomalies_total        (counter) Oracle anomalies flagged across all runs
        aegis_pipeline_duration_seconds (histogram) wall-clock time per run
        aegis_pipeline_runs_total    (counter) total pipeline runs, labeled by outcome
    """

    def __init__(self) -> None:
        if _HAS_PROMETHEUS:
            self.registry = CollectorRegistry()
            self.quality_score = Gauge(
                "aegis_quality_score", "Latest Sentinel quality score (0-100)",
                ["dataset"], registry=self.registry,
            )
            self.rows_processed = Counter(
                "aegis_rows_processed_total", "Total rows processed",
                ["dataset"], registry=self.registry,
            )
            self.issues_detected = Counter(
                "aegis_issues_detected_total", "Total Scout issues detected",
                ["dataset"], registry=self.registry,
            )
            self.remediations = Counter(
                "aegis_remediations_total", "Total Healer remediation actions",
                ["dataset"], registry=self.registry,
            )
            self.anomalies = Counter(
                "aegis_anomalies_total", "Total Oracle anomalies flagged",
                ["dataset"], registry=self.registry,
            )
            self.pipeline_duration = Histogram(
                "aegis_pipeline_duration_seconds", "Pipeline run duration in seconds",
                ["dataset"], registry=self.registry,
            )
            self.pipeline_runs = Counter(
                "aegis_pipeline_runs_total", "Total pipeline runs",
                ["dataset", "outcome"], registry=self.registry,
            )
        else:
            self._fallback: dict[str, _FallbackMetric] = {}

    # -- unified recording API, works with or without prometheus_client -----

    def record_run(self, dataset_name: str, row_count: int, overall_score: float,
                    issue_count: int, actions_taken: int, anomaly_count: int,
                    duration_seconds: float, outcome: str = "success") -> None:
        if _HAS_PROMETHEUS:
            self.quality_score.labels(dataset=dataset_name).set(overall_score)
            self.rows_processed.labels(dataset=dataset_name).inc(row_count)
            self.issues_detected.labels(dataset=dataset_name).inc(issue_count)
            self.remediations.labels(dataset=dataset_name).inc(actions_taken)
            self.anomalies.labels(dataset=dataset_name).inc(anomaly_count)
            self.pipeline_duration.labels(dataset=dataset_name).observe(duration_seconds)
            self.pipeline_runs.labels(dataset=dataset_name, outcome=outcome).inc()
        else:
            self._set_fallback(f"aegis_quality_score{{dataset={dataset_name}}}", overall_score)
            self._inc_fallback(f"aegis_rows_processed_total{{dataset={dataset_name}}}", row_count)
            self._inc_fallback(f"aegis_issues_detected_total{{dataset={dataset_name}}}", issue_count)
            self._inc_fallback(f"aegis_remediations_total{{dataset={dataset_name}}}", actions_taken)
            self._inc_fallback(f"aegis_anomalies_total{{dataset={dataset_name}}}", anomaly_count)
            self._set_fallback(f"aegis_pipeline_duration_seconds{{dataset={dataset_name}}}", duration_seconds)
            self._inc_fallback(f"aegis_pipeline_runs_total{{dataset={dataset_name},outcome={outcome}}}", 1)

    def _set_fallback(self, key: str, value: float) -> None:
        self._fallback.setdefault(key, _FallbackMetric(key)).set(value)

    def _inc_fallback(self, key: str, amount: float) -> None:
        self._fallback.setdefault(key, _FallbackMetric(key)).inc(amount)

    def latest_metrics_text(self) -> str:
        """Prometheus exposition-format text, suitable for a `/metrics` endpoint."""
        if _HAS_PROMETHEUS:
            return generate_latest(self.registry).decode("utf-8")
        lines = [f"{name} {metric.get()}" for name, metric in self._fallback.items()]
        return "\n".join(lines) + "\n" if lines else ""

    def snapshot(self) -> dict[str, Any]:
        """Plain-dict snapshot, handy for the dashboard or logging."""
        if _HAS_PROMETHEUS:
            return {"backend": "prometheus_client", "text": self.latest_metrics_text()}
        return {
            "backend": "in_memory_fallback",
            "values": {k: v.get() for k, v in self._fallback.items()},
        }


# Module-level singleton so orchestration code and the dashboard share one
# set of counters within a process, without wiring dependency injection
# through every task.
_default_metrics: PipelineMetrics | None = None
_metrics_lock = threading.Lock()


def get_metrics() -> PipelineMetrics:
    global _default_metrics
    with _metrics_lock:
        if _default_metrics is None:
            _default_metrics = PipelineMetrics()
        return _default_metrics
