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


async def test_missing_logs_reason_names_the_logs_not_the_docs():
    from nexgen_shared.schemas import KnowledgeResult, LogRetrievalResult

    from src.intent import IntentResult
    from src.orchestrator import MasterOrchestrator

    intent = IntentResult(logs_needed=True, docs_needed=True)
    logs = LogRetrievalResult(query_id="q", status="success", kql_generated="x", syntax_valid=True,
                              refinement_attempts=0, hits=[], hit_count=0, error=None)
    docs = KnowledgeResult(query_id="q", status="failure", chunks=[], total_tokens_after_compression=0,
                           conflict_detected=False, error="E004: rag down")
    assert MasterOrchestrator._missing_reason(intent, logs, docs) == "no matching log lines were found"


def test_trace_shows_the_log_search(client):
    from datetime import datetime, timezone

    body = {"query_id": "q2", "raw_text": "Why is the payments service throwing 500s?", "session_id": "s2",
            "timestamp_utc": datetime.now(timezone.utc).isoformat()}
    trace = client.post("/query", json=body).json()["reasoning_trace_summary"]
    assert trace.startswith("log search: (fixture) -> ")
