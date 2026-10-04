"""Step 3: run the fetch tasks of the graph in parallel."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import httpx

from nexgen_shared.schemas import (
    KnowledgeRequest,
    KnowledgeResult,
    KnowledgeTimeWindow,
    LogRetrievalRequest,
    LogRetrievalResult,
    SchemaContextPayload,
    TimeRange,
)

from src.fixtures import FixtureBackend
from src.planner import ExecutionGraph, ExecutionNode


def failed_logs(query_id: str, error: str) -> LogRetrievalResult:
    """A LogRetrievalResult that records a failed call (E003)."""
    return LogRetrievalResult(
        query_id=query_id, status="failure", kql_generated="", syntax_valid=False,
        refinement_attempts=0, hits=[], hit_count=0, error=f"E003: {error}",
    )


def failed_knowledge(query_id: str, error: str) -> KnowledgeResult:
    """A KnowledgeResult that records a failed call (E004)."""
    return KnowledgeResult(
        query_id=query_id, status="failure", chunks=[], total_tokens_after_compression=0,
        conflict_detected=False, error=f"E004: {error}",
    )


class DAGExecutor:
    """
    Calls the Query service (/retrieve) and RAG service (/knowledge) at the same time.
    If ``fixtures`` is given, it answers from local scenario data instead of HTTP.
    """

    def __init__(
        self,
        query_url: str = "http://localhost:8001",
        rag_url: str = "http://localhost:8002",
        fixtures: FixtureBackend | None = None,
        timeout: float = 30.0,
    ) -> None:
        self.query_url = query_url
        self.rag_url = rag_url
        self.fixtures = fixtures
        self.timeout = timeout

    async def execute(
        self, graph: ExecutionGraph, question: str
    ) -> tuple[LogRetrievalResult | None, KnowledgeResult | None]:
        """Run every fetch task concurrently and return (log result, knowledge result)."""
        fetches = [n for n in graph.nodes if n.action_type in ("FETCH_LOGS", "FETCH_DOCS")]
        results = await asyncio.gather(*(self._run(n, graph.query_id, question) for n in fetches))
        logs = next((r for r in results if isinstance(r, LogRetrievalResult)), None)
        docs = next((r for r in results if isinstance(r, KnowledgeResult)), None)
        return logs, docs

    async def _run(self, node: ExecutionNode, query_id: str, question: str) -> LogRetrievalResult | KnowledgeResult:
        if node.action_type == "FETCH_LOGS":
            log_request = LogRetrievalRequest(
                query_id=query_id,
                natural_language=node.payload.get("natural_language", question),
                index_hints=node.payload.get("index_hints", []),
                time_range=TimeRange.model_validate({"from": "now-30m", "to": "now"}),
                max_results=200,
                schema_context=SchemaContextPayload(known_fields=["service.name", "log.level", "message"]),
            )
            if self.fixtures:
                return self.fixtures.logs(log_request, question)
            try:
                return LogRetrievalResult.model_validate(await self._post(f"{self.query_url}/retrieve", log_request))
            except httpx.HTTPError as exc:
                return failed_logs(query_id, f"query service call failed: {exc!r}")

        doc_request = KnowledgeRequest(
            query_id=query_id,
            semantic_query=question,
            source_filters=["runbooks", "jira", "slack", "github"],
            time_window=KnowledgeTimeWindow(not_after=datetime.now(timezone.utc)),
            max_chunks=5,
            compression_budget_tokens=2000,
        )
        if self.fixtures:
            return self.fixtures.knowledge(doc_request)
        try:
            return KnowledgeResult.model_validate(await self._post(f"{self.rag_url}/knowledge", doc_request))
        except httpx.HTTPError as exc:
            return failed_knowledge(query_id, f"rag service call failed: {exc!r}")

    async def _post(self, url: str, request: LogRetrievalRequest | KnowledgeRequest) -> object:
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(url, json=request.model_dump(mode="json", by_alias=True))
            response.raise_for_status()
            return response.json()
