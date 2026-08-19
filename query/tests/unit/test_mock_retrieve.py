"""POST /retrieve in MOCK_SERVICES mode returns seeded hits without Elasticsearch."""

from __future__ import annotations

from fastapi.testclient import TestClient

from src.main import app


def test_retrieve_in_mock_mode_returns_payments_hits(monkeypatch) -> None:
    """Fixture pathway must succeed without a live Elasticsearch cluster."""
    monkeypatch.setenv("MOCK_SERVICES", "true")
    body = {
        "query_id": "test-mock-001",
        "natural_language": "Why did payments fail with HTTP 500?",
        "index_hints": ["payments-*"],
        "time_range": {"from": "now-30m", "to": "now"},
        "max_results": 10,
        "schema_context": {
            "known_fields": ["service.name"],
            "value_samples": {},
        },
    }
    with TestClient(app) as client:
        response = client.post("/retrieve", json=body)
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "success"
    assert payload["hit_count"] >= 1
    assert payload["hits"][0]["service"] == "payments"
    assert "payments" in payload["kql_generated"]
