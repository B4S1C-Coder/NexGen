from datetime import UTC, datetime

from nexgen_shared.schemas import KnowledgeRequest, KnowledgeTimeWindow

from src.dev_fixtures import mock_knowledge


def test_mock_knowledge_returns_runbook_chunk() -> None:
    """Fixture knowledge must include an authority-A payments runbook."""
    request = KnowledgeRequest(
        query_id="q-1",
        semantic_query="payments service database connection refused 500 errors",
        source_filters=["runbooks"],
        time_window=KnowledgeTimeWindow(not_after=datetime.now(UTC)),
        max_chunks=12,
        compression_budget_tokens=2000,
    )
    result = mock_knowledge(request)
    assert result.status == "success"
    assert result.conflict_detected is False
    assert any(c.source_type == "runbook" for c in result.chunks)
    assert result.chunks[0].authority_tier == "A"
