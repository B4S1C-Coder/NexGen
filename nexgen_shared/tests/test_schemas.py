"""Round-trip JSON serialisation for all public schemas."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import BaseModel

from nexgen_shared.schemas import (
    KnowledgeChunk,
    KnowledgeRequest,
    KnowledgeResult,
    KnowledgeTimeWindow,
    LogHit,
    LogRetrievalRequest,
    LogRetrievalResult,
    RCAEvidenceItem,
    RCAReport,
    RCASynthesisInput,
    SchemaContextPayload,
    TimeRange,
    UserQuery,
)


def _roundtrip(model: type[BaseModel], data: dict) -> None:
    parsed = model.model_validate(data)
    dumped = parsed.model_dump(mode="json", by_alias=True)
    again = model.model_validate(dumped)
    assert again == parsed


def test_user_query_roundtrip() -> None:
    _roundtrip(
        UserQuery,
        {
            "query_id": "550e8400-e29b-41d4-a716-446655440000",
            "raw_text": "Show HTTP 500s",
            "session_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
            "timestamp_utc": "2026-04-06T10:00:00Z",
        },
    )


def test_log_retrieval_request_roundtrip() -> None:
    _roundtrip(
        LogRetrievalRequest,
        {
            "query_id": "550e8400-e29b-41d4-a716-446655440000",
            "natural_language": "HTTP 500 errors from payments service last 30 min",
            "index_hints": ["payments-*", "gateway-*"],
            "time_range": {"from": "now-30m", "to": "now"},
            "max_results": 500,
            "schema_context": {
                "known_fields": ["service.name", "http.status_code", "log.level"],
                "value_samples": {"service.name": ["payments", "gateway", "auth"]},
            },
        },
    )


def test_log_retrieval_result_roundtrip() -> None:
    _roundtrip(
        LogRetrievalResult,
        {
            "query_id": "550e8400-e29b-41d4-a716-446655440000",
            "status": "success",
            "kql_generated": "service.name: 'payments' AND http.status_code: 500",
            "syntax_valid": True,
            "refinement_attempts": 1,
            "hits": [
                {
                    "timestamp": "2026-04-06T09:58:21Z",
                    "service": "payments",
                    "level": "ERROR",
                    "message": "Connection refused",
                    "trace_id": "abc123",
                }
            ],
            "hit_count": 47,
            "error": None,
        },
    )


def test_log_hit_preserves_extra_fields_roundtrip() -> None:
    data = {
        "timestamp": "2026-04-06T09:58:21Z",
        "service": "payments",
        "ecs.version": "8.0",
    }
    hit = LogHit.model_validate(data)
    dumped = hit.model_dump(mode="json", by_alias=True)
    assert dumped["ecs.version"] == "8.0"
    assert LogHit.model_validate(dumped) == hit


def test_knowledge_request_and_result_roundtrip() -> None:
    _roundtrip(
        KnowledgeRequest,
        {
            "query_id": "550e8400-e29b-41d4-a716-446655440000",
            "semantic_query": "payments DB errors",
            "source_filters": ["runbooks", "jira", "slack", "github"],
            "time_window": {"not_after": "2026-04-06T10:00:00Z"},
            "max_chunks": 12,
            "compression_budget_tokens": 2000,
        },
    )
    _roundtrip(
        KnowledgeResult,
        {
            "query_id": "550e8400-e29b-41d4-a716-446655440000",
            "status": "success",
            "chunks": [
                {
                    "chunk_id": "c-001",
                    "source_type": "runbook",
                    "source_uri": "confluence://runbooks/payments-db-failover",
                    "authority_tier": "A",
                    "recency_score": 0.98,
                    "content": "When the payments service cannot reach db-primary...",
                    "retrieved_at": "2026-04-06T10:00:01Z",
                }
            ],
            "total_tokens_after_compression": 1847,
            "conflict_detected": False,
            "error": None,
        },
    )


def test_rca_synthesis_input_roundtrip() -> None:
    chunk = KnowledgeChunk(
        chunk_id="c-001",
        source_type="runbook",
        source_uri="confluence://x",
        authority_tier="A",
        recency_score=0.9,
        content="body",
        retrieved_at=datetime(2026, 4, 6, 10, 0, 1, tzinfo=timezone.utc),
    )
    hit = LogHit(timestamp=None, message="boom", trace_id="t1")
    _roundtrip(
        RCASynthesisInput,
        {
            "query_id": "550e8400-e29b-41d4-a716-446655440000",
            "original_query": "why 500s",
            "log_evidence": [hit.model_dump(mode="json", by_alias=True)],
            "knowledge_context": [chunk.model_dump(mode="json", by_alias=True)],
            "reasoning_trace": ["considered network; ruled out"],
        },
    )


def test_rca_report_roundtrip() -> None:
    _roundtrip(
        RCAReport,
        {
            "query_id": "550e8400-e29b-41d4-a716-446655440000",
            "root_cause_summary": "DB primary unreachable.",
            "confidence": 0.91,
            "evidence": [
                {
                    "type": "log",
                    "ref": "trace_id:abc123",
                    "snippet": "Connection refused: db-primary:5432",
                },
                {"type": "runbook", "ref": "confluence://runbooks/payments-db-failover"},
            ],
            "recommended_actions": ["Fail over to replica"],
            "reasoning_trace_summary": "Explored 3 hypotheses",
            "mttr_estimate_minutes": 5,
            "generated_at": "2026-04-06T10:00:05Z",
        },
    )


def test_time_range_and_schema_context_forbid_unknown_keys() -> None:
    with pytest.raises(Exception):
        TimeRange.model_validate({"from": "now-1h", "to": "now", "extra": 1})
    with pytest.raises(Exception):
        SchemaContextPayload.model_validate({"known_fields": ["a"], "unknown": 1})
