"""Observability configuration — Prometheus metrics and future tracing settings."""

from shared.config.settings import settings

# Prometheus
METRICS_ENABLED: bool = settings.metrics_enabled
METRICS_ENDPOINT: str = "/metrics"
