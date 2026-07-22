"""
AlertManager — threshold-based alerting for pipeline health
===============================================================

Sends a Slack-compatible webhook message when a pipeline run's quality
score drops below a threshold, or when Oracle flags more anomalies
than expected. Uses only the standard library (``urllib``), so alerting
needs no new required dependency — just an env var.

Fully opt-in: with no webhook URL configured, ``AlertManager.check()``
is a documented no-op. This means wiring it into ``foundry_flows.py``
can never break a pipeline that hasn't configured alerting.
"""

from __future__ import annotations

import json
import logging
import urllib.request
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger("aegis.alerts")


@dataclass
class Alert:
    severity: str  # "warning" | "critical"
    title: str
    message: str
    context: dict[str, Any] = field(default_factory=dict)

    def to_slack_payload(self) -> dict[str, Any]:
        emoji = "🚨" if self.severity == "critical" else "⚠️"
        lines = [f"{emoji} *{self.title}*", self.message]
        if self.context:
            lines.append("```" + json.dumps(self.context, indent=2, default=str) + "```")
        return {"text": "\n".join(lines)}


class AlertManager:
    """Evaluates pipeline results against configurable thresholds and
    fires a webhook notification for anything that crosses them.
    """

    def __init__(self, webhook_url: str | None = None,
                 quality_score_threshold: float = 75.0,
                 anomaly_pct_threshold: float = 10.0,
                 timeout_seconds: float = 5.0) -> None:
        self.webhook_url = webhook_url
        self.quality_score_threshold = quality_score_threshold
        self.anomaly_pct_threshold = anomaly_pct_threshold
        self.timeout_seconds = timeout_seconds
        self.sent_alerts: list[Alert] = []  # kept for tests / dashboard display

    @property
    def enabled(self) -> bool:
        return bool(self.webhook_url)

    def check(self, dataset_name: str, overall_score: float, issue_count: int,
              anomaly_pct: float = 0.0, drift_detected: bool = False) -> list[Alert]:
        """Evaluates one run's results, sends any alerts that fire, and
        returns them (empty list if nothing crossed a threshold, or if
        alerting isn't configured)."""
        alerts: list[Alert] = []

        if overall_score < self.quality_score_threshold:
            alerts.append(Alert(
                severity="critical" if overall_score < self.quality_score_threshold / 2 else "warning",
                title=f"Data quality below threshold — {dataset_name}",
                message=(f"Quality score is {overall_score:.1f}/100, below the "
                         f"{self.quality_score_threshold:.0f} threshold "
                         f"({issue_count} issue(s) detected)."),
                context={"dataset": dataset_name, "overall_score": overall_score,
                         "issue_count": issue_count},
            ))

        if anomaly_pct > self.anomaly_pct_threshold:
            alerts.append(Alert(
                severity="warning",
                title=f"Elevated anomaly rate — {dataset_name}",
                message=f"Oracle flagged {anomaly_pct:.1f}% of rows as anomalous "
                        f"(threshold: {self.anomaly_pct_threshold:.1f}%).",
                context={"dataset": dataset_name, "anomaly_pct": anomaly_pct},
            ))

        if drift_detected:
            alerts.append(Alert(
                severity="warning",
                title=f"Schema/distribution drift detected — {dataset_name}",
                message="Oracle's cross-run comparison found columns that moved "
                         "beyond the configured drift threshold.",
                context={"dataset": dataset_name},
            ))

        for alert in alerts:
            self._send(alert)
        return alerts

    def _send(self, alert: Alert) -> None:
        self.sent_alerts.append(alert)
        if not self.enabled:
            logger.info("Alert (not sent, no webhook configured): %s — %s",
                        alert.title, alert.message)
            return
        try:
            payload = json.dumps(alert.to_slack_payload()).encode("utf-8")
            request = urllib.request.Request(
                self.webhook_url, data=payload,
                headers={"Content-Type": "application/json"}, method="POST",
            )
            urllib.request.urlopen(request, timeout=self.timeout_seconds)
        except Exception:  # noqa: BLE001 - alerting must never break the pipeline
            logger.exception("Failed to deliver alert webhook: %s", alert.title)
