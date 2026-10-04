"""Full Master pipeline over HTTP, with Query/RAG replaced by fixtures and no LLM."""

from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from src.main import app


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv("MOCK_SERVICES", "true")
    monkeypatch.setenv("OPENAI_API_KEY", "")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:1/0")  # nothing listens here -> in-memory sessions
    with TestClient(app) as c:
        yield c


def test_query_finds_root_cause_and_saves_session(client):
    body = {
        "query_id": "q1",
        "raw_text": "Why is the payments service throwing 500s?",  # scenario db-03 (a trap)
        "session_id": "s1",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
    }
    report = client.post("/query", json=body).json()
    assert report["root_cause_summary"].startswith("db-primary")
    assert report["confidence"] > 0
    assert report["evidence"]
    assert "notifications: rejected ([E008]" in report["reasoning_trace_summary"]

    session = client.get("/session/s1").json()
    assert [m["role"] for m in session["messages"]] == ["user", "assistant"]


def test_unknown_session_is_404(client):
    assert client.get("/session/missing").status_code == 404
