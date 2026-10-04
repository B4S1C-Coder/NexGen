"""
Offline stand-in for the Query and RAG services (used when MOCK_SERVICES=true).

It serves the logs and docs of one incident from data/scenarios.json, chosen by
matching the user's question, and returns them in the same shared schemas the
real services use. This lets Master be demoed and benchmarked without
Elasticsearch, Qdrant or the other two services running.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from nexgen_shared.schemas import (
    KnowledgeChunk,
    KnowledgeRequest,
    KnowledgeResult,
    LogHit,
    LogRetrievalRequest,
    LogRetrievalResult,
)

from src.settings import MASTER_DIR

SCENARIOS_PATH = MASTER_DIR / "data" / "scenarios.json"


def _words(text: str) -> set[str]:
    return set(re.findall(r"[a-z0-9-]+", text.lower()))


class FixtureBackend:
    """Answers log and knowledge requests from the scenario file."""

    def __init__(self, path: Path = SCENARIOS_PATH) -> None:
        data = json.loads(path.read_text())
        self.date: str = data["date"]
        self.docs: dict[str, dict[str, str]] = data["docs"]
        self.scenarios: list[dict[str, Any]] = data["scenarios"]

    def find(self, question: str) -> dict[str, Any]:
        """Return the scenario with this exact question, else the one sharing the most words."""
        for scenario in self.scenarios:
            if scenario["question"] == question:
                return scenario
        words = _words(question)
        return max(self.scenarios, key=lambda s: len(words & _words(s["question"])))

    def logs(self, request: LogRetrievalRequest, question: str) -> LogRetrievalResult:
        """Build a LogRetrievalResult from the log lines of the scenario matching ``question``."""
        scenario = self.find(question)
        hits = [
            LogHit(
                timestamp=datetime.fromisoformat(f"{self.date}T{time}+00:00"),
                service=service,
                level=level,
                message=message,
                trace_id=f"{scenario['id']}-{i}",
            )
            for i, (time, service, level, message) in enumerate(scenario["logs"])
        ][: request.max_results]
        return LogRetrievalResult(
            query_id=request.query_id, status="success", kql_generated="(fixture)",
            syntax_valid=True, refinement_attempts=0, hits=hits, hit_count=len(hits), error=None,
        )

    def knowledge(self, request: KnowledgeRequest) -> KnowledgeResult:
        """
        Build a KnowledgeResult. A benchmark question gets its scenario's documents;
        any other question gets the two documents sharing the most words with it.
        """
        question = request.semantic_query
        exact = [s for s in self.scenarios if s["question"] == question]
        if exact:
            doc_ids = exact[0]["docs"]
        else:
            words = _words(question)
            doc_ids = sorted(self.docs, key=lambda d: -len(words & _words(self.docs[d]["content"])))[:2]
        now = datetime.now(timezone.utc)
        chunks = [
            KnowledgeChunk(
                chunk_id=doc_id, source_type=self.docs[doc_id]["type"],
                source_uri=self.docs[doc_id]["uri"], authority_tier=self.docs[doc_id]["tier"],  # type: ignore[arg-type]
                recency_score=0.9, content=self.docs[doc_id]["content"], retrieved_at=now,
            )
            for doc_id in doc_ids
        ][: request.max_chunks]
        return KnowledgeResult(
            query_id=request.query_id, status="success", chunks=chunks,
            total_tokens_after_compression=sum(len(c.content) // 4 for c in chunks),
            conflict_detected=False, error=None,
        )
