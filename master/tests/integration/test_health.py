from datetime import datetime, timezone
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient
from nexgen_shared.schemas import RCAReport

from src.main import app
from src.orchestrator import MasterOrchestrator
from src.settings import Settings


def test_health_ok():
    client = TestClient(app)
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok", "service": "master"}


def test_query_uses_orchestrator():
    """POST /query must invoke MasterOrchestrator rather than a Phase-0 stub."""
    settings = Settings()
    orchestrator = MasterOrchestrator(settings=settings)
    report = RCAReport(
        query_id="q-wired",
        root_cause_summary="Wired.",
        confidence=0.9,
        evidence=[],
        recommended_actions=["ok"],
        reasoning_trace_summary="ok",
        mttr_estimate_minutes=1,
        generated_at=datetime.now(timezone.utc),
    )
    orchestrator.execute_query = AsyncMock(return_value=report)

    with TestClient(app) as client:
        client.app.state.orchestrator = orchestrator
        response = client.post(
            "/query",
            json={
                "query_id": "q-wired",
                "raw_text": "Why did payments fail at 09:57?",
                "session_id": "s-1",
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            },
        )

    assert response.status_code == 200
    body = response.json()
    assert body["root_cause_summary"] == "Wired."
    assert body["confidence"] == 0.9
    orchestrator.execute_query.assert_awaited_once()
