"""Small factories shared by the Master tests."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Callable

import pytest

from nexgen_shared.schemas import KnowledgeChunk, LogHit, RCASynthesisInput

from src.topology import Topology


@pytest.fixture
def topology() -> Topology:
    """gateway -> payments -> db-primary; notifications is unrelated."""
    return Topology({
        "gateway": {"dependencies": ["payments"]},
        "payments": {"dependencies": ["db-primary"]},
        "notifications": {"dependencies": []},
    })


@pytest.fixture
def hit() -> Callable[..., LogHit]:
    """hit("09:57:01", "payments", "ERROR", "message") -> LogHit."""
    def make(time: str, service: str, level: str, message: str) -> LogHit:
        h, m, s = (int(x) for x in time.split(":"))
        return LogHit(
            timestamp=datetime(2026, 9, 14, h, m, s, tzinfo=timezone.utc),
            service=service, level=level, message=message, trace_id=f"{service}-{time}",
        )
    return make


@pytest.fixture
def chunk() -> Callable[[str], KnowledgeChunk]:
    """chunk("text") -> KnowledgeChunk."""
    def make(content: str) -> KnowledgeChunk:
        return KnowledgeChunk(
            chunk_id="c1", source_type="runbook", source_uri="confluence://runbooks/x",
            authority_tier="A", recency_score=0.9, content=content,
            retrieved_at=datetime(2026, 9, 14, tzinfo=timezone.utc),
        )
    return make


@pytest.fixture
def context(hit: Callable[..., LogHit], chunk: Callable[[str], KnowledgeChunk]) -> RCASynthesisInput:
    """A db-primary outage seen from payments, plus unrelated earlier notifications noise."""
    return RCASynthesisInput(
        query_id="q1",
        original_query="Why is payments failing?",
        log_evidence=[
            hit("09:30:00", "notifications", "ERROR", "SMTP send failed"),
            hit("09:56:58", "db-primary", "ERROR", "database system is shutting down"),
            hit("09:57:01", "payments", "ERROR", "Connection refused: db-primary:5432"),
            hit("09:57:03", "gateway", "ERROR", "upstream payments returned 500"),
        ],
        knowledge_context=[chunk("Runbook: db-primary failover. Promote db-replica-1. Restart pools.")],
        reasoning_trace=[],
    )
