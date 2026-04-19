from datetime import UTC, datetime

from fastapi.testclient import TestClient

from src.main import app


def test_health_ok():
    client = TestClient(app)

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "rag"}


def test_ingest_stub_ok():
    client = TestClient(app)

    response = client.post("/ingest")

    assert response.status_code == 200
    assert response.json() == {"status": "accepted"}


def test_knowledge_stub_ok():
    client = TestClient(app)

    response = client.post(
        "/knowledge",
        json={
            "query_id": "q-123",
            "semantic_query": "payments database connection refused",
            "source_filters": ["runbooks"],
            "time_window": {"not_after": datetime.now(UTC).isoformat()},
            "max_chunks": 12,
            "compression_budget_tokens": 2000,
        },
    )

    assert response.status_code == 200
    assert response.json()["query_id"] == "q-123"
    assert response.json()["status"] == "success"
    assert response.json()["chunks"] == []
    assert response.json()["conflict_detected"] is False
    assert response.json()["total_tokens_after_compression"] == 0
    assert response.json()["error"] is None
