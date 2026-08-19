"""Fixture-backed knowledge retrieval used when MOCK_SERVICES=true.

Returns a compact runbook / ticket set for the payments database outage
so the Master orchestrator can run without Qdrant or embedding models.
"""

from __future__ import annotations

from datetime import datetime, timezone

from nexgen_shared.schemas import KnowledgeChunk, KnowledgeRequest, KnowledgeResult


def fixture_chunks(semantic_query: str) -> list[KnowledgeChunk]:
    """Return seeded knowledge chunks ranked for a payments-DB incident.

    Args:
        semantic_query: Operator question used for light keyword filtering.

    Returns:
        Authority-tagged chunks the Master can cite in an RCA.
    """
    now = datetime(2026, 4, 6, 10, 0, 1, tzinfo=timezone.utc)
    chunks = [
        KnowledgeChunk(
            chunk_id="c-001",
            source_type="runbook",
            source_uri="confluence://runbooks/payments-db-failover",
            authority_tier="A",
            recency_score=0.98,
            content=(
                "When the payments service cannot reach db-primary, trigger "
                "manual failover to db-replica-1 via ops-console. Verify with "
                "systemctl status postgresql."
            ),
            retrieved_at=now,
        ),
        KnowledgeChunk(
            chunk_id="c-002",
            source_type="jira",
            source_uri="jira://PAY-1842",
            authority_tier="B",
            recency_score=0.81,
            content=(
                "PAY-1842: Recurring TCP timeouts to db-primary:5432 after "
                "storage saturation. Workaround is replica failover; root "
                "fix is expanding primary disk."
            ),
            retrieved_at=now,
        ),
        KnowledgeChunk(
            chunk_id="c-003",
            source_type="slack",
            source_uri="slack://#payments-incidents/1743932221",
            authority_tier="B",
            recency_score=0.74,
            content=(
                "Incident channel: gateway 500s lining up with payments "
                "Connection refused. Auth and billing stayed healthy."
            ),
            retrieved_at=now,
        ),
    ]
    text = semantic_query.lower()
    if "auth" in text and "payment" not in text:
        return [
            KnowledgeChunk(
                chunk_id="c-auth",
                source_type="runbook",
                source_uri="confluence://runbooks/auth-token-issuer",
                authority_tier="A",
                recency_score=0.9,
                content="If the token issuer cannot reach auth-db, bounce auth-db-replica-2.",
                retrieved_at=now,
            )
        ]
    return chunks


def mock_knowledge(request: KnowledgeRequest) -> KnowledgeResult:
    """Assemble a successful ``KnowledgeResult`` from local fixtures.

    Args:
        request: Canonical knowledge request from the Master service.

    Returns:
        A ``success`` result with compressed token estimate and no conflict.
    """
    chunks = fixture_chunks(request.semantic_query)[: request.max_chunks]
    tokens = sum(len(c.content.split()) for c in chunks)
    return KnowledgeResult(
        query_id=request.query_id,
        status="success",
        chunks=chunks,
        total_tokens_after_compression=min(tokens, request.compression_budget_tokens),
        conflict_detected=False,
        error=None,
    )
