import json

import pytest

from src.observability import AlertManager, PipelineMetrics
from src.observability.alerts import Alert


def test_pipeline_metrics_records_a_run():
    metrics = PipelineMetrics()
    metrics.record_run(
        dataset_name="orders", row_count=100, overall_score=88.4, issue_count=3,
        actions_taken=2, anomaly_count=4, duration_seconds=1.23,
    )
    snapshot = metrics.snapshot()
    assert snapshot["backend"] in {"prometheus_client", "in_memory_fallback"}
    text = metrics.latest_metrics_text()
    assert "aegis_quality_score" in text or snapshot["backend"] == "prometheus_client"


def test_alert_manager_noop_when_no_webhook_configured():
    manager = AlertManager(webhook_url=None)
    assert manager.enabled is False
    alerts = manager.check("orders", overall_score=95.0, issue_count=0, anomaly_pct=0.0)
    assert alerts == []


def test_alert_manager_fires_on_low_quality_score():
    manager = AlertManager(webhook_url=None, quality_score_threshold=75.0)
    alerts = manager.check("orders", overall_score=60.0, issue_count=5, anomaly_pct=0.0)
    assert len(alerts) == 1
    assert alerts[0].severity in {"warning", "critical"}
    assert "quality" in alerts[0].title.lower()


def test_alert_manager_fires_on_elevated_anomaly_rate():
    manager = AlertManager(webhook_url=None, anomaly_pct_threshold=10.0)
    alerts = manager.check("orders", overall_score=95.0, issue_count=0, anomaly_pct=25.0)
    assert len(alerts) == 1
    assert "anomaly" in alerts[0].title.lower()


def test_alert_manager_sends_webhook_when_configured(monkeypatch):
    captured = {}

    def fake_urlopen(request, timeout):
        captured["url"] = request.full_url
        captured["payload"] = json.loads(request.data.decode("utf-8"))

        class _Resp:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        return _Resp()

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)

    manager = AlertManager(webhook_url="https://hooks.example.com/webhook",
                            quality_score_threshold=75.0)
    manager.check("orders", overall_score=50.0, issue_count=10, anomaly_pct=0.0)

    assert captured["url"] == "https://hooks.example.com/webhook"
    assert "text" in captured["payload"]


def test_alert_to_slack_payload_includes_context():
    alert = Alert(severity="critical", title="Test", message="msg", context={"a": 1})
    payload = alert.to_slack_payload()
    assert "Test" in payload["text"]
    assert "msg" in payload["text"]
