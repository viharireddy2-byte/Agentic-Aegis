"""Observability for Agentic Aegis — metrics + threshold alerting.

    from src.observability import get_metrics, AlertManager

Both are additive: the pipeline records to them on every run, but
neither requires any new mandatory dependency or configuration to be
present for the pipeline to keep working exactly as before.
"""

from .alerts import Alert, AlertManager
from .metrics import PipelineMetrics, get_metrics

__all__ = ["Alert", "AlertManager", "PipelineMetrics", "get_metrics"]
