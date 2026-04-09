"""Integration tests for the FastAPI application endpoints (TASKS.md P0-Q1).

These tests verify the three stub endpoints defined in AGENTS.md §6.1
behave correctly before any real pipeline logic is wired in.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from src.main import app

client = TestClient(app)


class TestHealthEndpoint:
    """Tests for GET /health."""

    def test_health_returns_200(self) -> None:
        """Health endpoint must return HTTP 200."""
        response = client.get("/health")
        assert response.status_code == 200

    def test_health_returns_correct_body(self) -> None:
        """Health endpoint must return service name and ok status."""
        response = client.get("/health")
        body = response.json()
        assert body["status"] == "ok"
        assert body["service"] == "query"


class TestSchemaCacheEndpoint:
    """Tests for GET /schema-cache/status."""

    def test_schema_cache_returns_200(self) -> None:
        """Schema cache endpoint must return HTTP 200."""
        response = client.get("/schema-cache/status")
        assert response.status_code == 200

    def test_schema_cache_returns_expected_keys(self) -> None:
        """Schema cache response must contain all expected keys."""
        response = client.get("/schema-cache/status")
        body = response.json()
        assert "last_refreshed" in body
        assert "index_count" in body
        assert "field_count" in body
        assert "is_stale" in body

    def test_schema_cache_is_stale_before_linker_is_built(self) -> None:
        """Cache must report stale=True until SchemaLinker is implemented."""
        response = client.get("/schema-cache/status")
        body = response.json()
        assert body["is_stale"] is True
        assert body["index_count"] == 0


class TestRetrieveEndpoint:
    """Tests for POST /retrieve."""

    def _valid_request_body(self) -> dict:
        """Return a minimal valid LogRetrievalRequest payload."""
        return {
            "query_id": "test-001",
            "natural_language": "show me ERROR logs from auth in the last hour",
            "index_hints": ["logs-*"],
            "time_range": {"from": "now-1h", "to": "now"},
            "max_results": 10,
            "schema_context": {
                "known_fields": ["service.name", "log.level", "@timestamp"],
                "value_samples": {"service.name": ["auth", "payments"]},
            },
        }

    def test_retrieve_returns_200(self) -> None:
        """Retrieve endpoint must return HTTP 200 for a valid request."""
        response = client.post("/retrieve", json=self._valid_request_body())
        assert response.status_code == 200

    def test_retrieve_returns_correct_query_id(self) -> None:
        """Response query_id must match the request query_id."""
        response = client.post("/retrieve", json=self._valid_request_body())
        body = response.json()
        assert body["query_id"] == "test-001"

    def test_retrieve_returns_success_status(self) -> None:
        """Stub must return success status."""
        response = client.post("/retrieve", json=self._valid_request_body())
        body = response.json()
        assert body["status"] == "success"

    def test_retrieve_returns_empty_hits(self) -> None:
        """Stub must return empty hits list until pipeline is wired."""
        response = client.post("/retrieve", json=self._valid_request_body())
        body = response.json()
        assert body["hits"] == []
        assert body["hit_count"] == 0

    def test_retrieve_rejects_missing_field(self) -> None:
        """Endpoint must return 422 if a required field is missing."""
        bad_body = self._valid_request_body()
        del bad_body["natural_language"]
        response = client.post("/retrieve", json=bad_body)
        assert response.status_code == 422

    def test_retrieve_rejects_extra_fields(self) -> None:
        """LogRetrievalRequest has extra=forbid — unknown fields must fail."""
        bad_body = self._valid_request_body()
        bad_body["unexpected_field"] = "should not be allowed"
        response = client.post("/retrieve", json=bad_body)
        assert response.status_code == 422