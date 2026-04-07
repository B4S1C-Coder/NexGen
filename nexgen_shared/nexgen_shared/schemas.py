"""Canonical Pydantic v2 models for inter-component JSON contracts (AGENTS.md §5)."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field


class UserQuery(BaseModel):
    """Inbound user question to the Master orchestrator."""

    model_config = ConfigDict(extra="forbid")

    query_id: str
    raw_text: str
    session_id: str
    timestamp_utc: datetime


class TimeRange(BaseModel):
    """Inclusive time window using Kibana-style bounds (e.g. ``now-30m`` … ``now``)."""

    model_config = ConfigDict(extra="forbid")

    from_: str = Field(validation_alias="from", serialization_alias="from")
    to: str


class SchemaContextPayload(BaseModel):
    """Field names and sample values supplied by the Master to steer KQL generation."""

    model_config = ConfigDict(extra="forbid")

    known_fields: list[str]
    value_samples: dict[str, list[str]] = Field(default_factory=dict)


class LogRetrievalRequest(BaseModel):
    """Request to translate NL to KQL and fetch log rows from Elasticsearch."""

    model_config = ConfigDict(extra="forbid")

    query_id: str
    natural_language: str
    index_hints: list[str]
    time_range: TimeRange
    max_results: int
    schema_context: SchemaContextPayload


class LogHit(BaseModel):
    """One masked log record; extra ES fields are preserved on (de)serialisation."""

    model_config = ConfigDict(extra="allow")

    timestamp: datetime | None = None
    service: str | None = None
    level: str | None = None
    message: str | None = None
    trace_id: str | None = None


LogRetrievalStatus = Literal["success", "partial", "failure"]


class LogRetrievalResult(BaseModel):
    """Outcome of KQL execution, including masked hits and status."""

    model_config = ConfigDict(extra="forbid")

    query_id: str
    status: LogRetrievalStatus
    kql_generated: str
    syntax_valid: bool
    refinement_attempts: int
    hits: list[LogHit]
    hit_count: int
    error: str | None


class KnowledgeTimeWindow(BaseModel):
    """Upper bound on document recency for RAG retrieval."""

    model_config = ConfigDict(extra="forbid")

    not_after: datetime


class KnowledgeRequest(BaseModel):
    """Request to retrieve and compress organisational knowledge from the RAG pipeline."""

    model_config = ConfigDict(extra="forbid")

    query_id: str
    semantic_query: str
    source_filters: list[str]
    time_window: KnowledgeTimeWindow
    max_chunks: int
    compression_budget_tokens: int


AuthorityTier = Literal["A", "B"]
KnowledgeResultStatus = Literal["success", "partial", "failure"]


class KnowledgeChunk(BaseModel):
    """One ranked snippet from runbooks, tickets, chat, or code context."""

    model_config = ConfigDict(extra="forbid")

    chunk_id: str
    source_type: str
    source_uri: str
    authority_tier: AuthorityTier
    recency_score: Annotated[float, Field(ge=0.0, le=1.0)]
    content: str
    retrieved_at: datetime


class KnowledgeResult(BaseModel):
    """Response from RAG with retrieved chunks and optional failure details."""

    model_config = ConfigDict(extra="forbid")

    query_id: str
    status: KnowledgeResultStatus
    chunks: list[KnowledgeChunk]
    total_tokens_after_compression: int
    conflict_detected: bool
    error: str | None


class RCASynthesisInput(BaseModel):
    """Master-internal payload for the RCA synthesis step."""

    model_config = ConfigDict(extra="forbid")

    query_id: str
    original_query: str
    log_evidence: list[LogHit]
    knowledge_context: list[KnowledgeChunk]
    reasoning_trace: list[str]


class RCAEvidenceItem(BaseModel):
    """Single citation (log line, doc link, etc.) in an RCA report."""

    model_config = ConfigDict(extra="allow")

    type: str
    ref: str
    snippet: str | None = None


class RCAReport(BaseModel):
    """Final root-cause analysis returned to the user."""

    model_config = ConfigDict(extra="forbid")

    query_id: str
    root_cause_summary: str
    confidence: Annotated[float, Field(ge=0.0, le=1.0)]
    evidence: list[RCAEvidenceItem]
    recommended_actions: list[str]
    reasoning_trace_summary: str
    mttr_estimate_minutes: int
    generated_at: datetime


def _dump_load_roundtrip(model: type[BaseModel], data: dict[str, Any]) -> dict[str, Any]:
    """Serialize to JSON-compatible dict and parse back; used in tests."""
    inst = model.model_validate(data)
    dumped = inst.model_dump(mode="json")
    return dumped
