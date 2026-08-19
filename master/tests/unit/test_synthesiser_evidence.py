from datetime import datetime, timezone

from nexgen_shared.schemas import (
    KnowledgeChunk,
    KnowledgeResult,
    LogHit,
    LogRetrievalResult,
    UserQuery,
)

from src.synthesiser import RCASynthesiser


async def test_evidence_report_cites_logs_and_runbooks():
    """Without an LLM, synthesis must still cite both evidence channels."""
    query = UserQuery(
        query_id="q-ev",
        raw_text="Why did payments fail?",
        session_id="s",
        timestamp_utc=datetime.now(timezone.utc),
    )
    logs = LogRetrievalResult(
        query_id="q-ev",
        status="success",
        kql_generated="service.name: payments",
        syntax_valid=True,
        refinement_attempts=0,
        hits=[
            LogHit(
                timestamp=datetime.now(timezone.utc),
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
        query_id="q-ev",
        status="success",
        chunks=[
            KnowledgeChunk(
                chunk_id="c-001",
                source_type="runbook",
                source_uri="confluence://runbooks/payments-db-failover",
                authority_tier="A",
                recency_score=0.9,
                content="Trigger manual failover to db-replica-1 via ops-console.",
                retrieved_at=datetime.now(timezone.utc),
            )
        ],
        total_tokens_after_compression=12,
        conflict_detected=False,
        error=None,
    )
    report = await RCASynthesiser(openai_client=None).synthesize(
        query, logs, docs, session_history=[]
    )
    assert report.confidence > 0
    assert "Connection refused" in report.root_cause_summary
    assert {item.type for item in report.evidence} >= {"log", "runbook"}
