from datetime import UTC, datetime

from fastapi.testclient import TestClient

from src.ingest_service import IngestResponse
from src.main import app


def test_health_ok():
    client = TestClient(app)

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "rag"}


class _FakeIngestService:
    async def ingest(self, request):
        return IngestResponse(
            status="success",
            source_type=request.source_type,
            documents_indexed=0,
            chunks_indexed=0,
        )


def test_ingest_endpoint_ok():
    with TestClient(app) as client:
        client.app.state.ingest_service = _FakeIngestService()
        response = client.post("/ingest", json={"source_type": "local_file", "full_reindex": True})

    assert response.status_code == 200
    assert response.json() == {
        "status": "success",
        "source_type": "local_file",
        "documents_indexed": 0,
        "chunks_indexed": 0,
    }


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
