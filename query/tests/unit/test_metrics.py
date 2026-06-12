"""Unit tests for the Prometheus /metrics endpoint (TASKS.md P3-Q2)."""

from __future__ import annotations

from fastapi.testclient import TestClient

from src.main import app

# Plain TestClient (no "with") does NOT trigger the app lifespan,
# so /metrics and /health respond without needing live ES/Qdrant.
client = TestClient(app)


class TestMetricsEndpoint:
    """Tests for GET /metrics."""

    def test_metrics_returns_200(self) -> None:
        """/metrics must return HTTP 200."""
        response = client.get("/metrics")
        assert response.status_code == 200

    def test_metrics_contains_metric_names(self) -> None:
        """/metrics body must contain all three required metric names."""
        body = client.get("/metrics").text
        assert "nexgen_query_latency_seconds" in body
        assert "nexgen_query_refinement_attempts_total" in body
        assert "nexgen_schema_cache_age_seconds" in body
        