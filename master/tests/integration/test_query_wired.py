"""End-to-end Master DAG using HTTP-mocked Query and RAG services."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
from nexgen_shared.schemas import (
    KnowledgeChunk,
    KnowledgeResult,
    LogHit,
    LogRetrievalResult,
    UserQuery,
)

from src.orchestrator import MasterOrchestrator
from src.settings import Settings


@pytest.mark.asyncio
async def test_execute_query_dual_route_http(monkeypatch: pytest.MonkeyPatch) -> None:
    """Master must call Query and RAG over HTTP and synthesise a real RCA."""
    monkeypatch.setenv("MOCK_SERVICES", "false")
    monkeypatch.setenv("OPENAI_API_KEY", "")

    logs = LogRetrievalResult(
        query_id="q-e2e",
        status="success",
        kql_generated="service.name: payments AND http.status_code: 500",
        syntax_valid=True,
        refinement_attempts=0,
        hits=[
            LogHit(
                timestamp=datetime(2026, 4, 6, 9, 58, 21, tzinfo=timezone.utc),
                service="payments",
                level="ERROR",
                message="Connection refused: db-primary:5432",
                trace_id="abc123",
            )
        ],
        hit_count=1,
        error=None,
    )
    docs = KnowledgeResult(
        query_id="q-e2e",
        status="success",
        chunks=[
            KnowledgeChunk(
                chunk_id="c-001",
                source_type="runbook",
                source_uri="confluence://runbooks/payments-db-failover",
                authority_tier="A",
                recency_score=0.98,
                content="Trigger manual failover to db-replica-1 via ops-console.",
                retrieved_at=datetime.now(timezone.utc),
            )
        ],
        total_tokens_after_compression=40,
        conflict_detected=False,
        error=None,
    )

    settings = Settings()
    orch = MasterOrchestrator(settings=settings)
    orch.executor.mock_mode = False

    async def fake_execute(graph, query_id, natural_language):
        return {"fetch_logs": logs, "fetch_docs": docs}

    orch.executor.execute = AsyncMock(side_effect=fake_execute)

    report = await orch.execute_query(
        UserQuery(
            query_id="q-e2e",
            raw_text="Why did payments fail at 09:57?",
            session_id="s-e2e",
            timestamp_utc=datetime.now(timezone.utc),
        )
    )

    assert report.confidence > 0
    assert "Connection refused" in report.root_cause_summary
    assert report.evidence
    assert any(item.type == "log" for item in report.evidence)
    assert any(item.type == "runbook" for item in report.evidence)
    orch.executor.execute.assert_awaited_once()
