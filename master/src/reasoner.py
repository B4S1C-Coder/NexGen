"""Step 5a: propose candidate root causes from the log evidence."""

from __future__ import annotations

import json
import logging
from collections import Counter
from datetime import datetime

from openai import AsyncOpenAI
from pydantic import BaseModel

from nexgen_shared.schemas import LogHit, RCASynthesisInput

from src.context import log_line
from src.llm import ask_json, load_prompt

logger = logging.getLogger(__name__)

PROBLEM_LEVELS = {"WARN", "WARNING", "ERROR", "FATAL", "CRITICAL"}


class Hypothesis(BaseModel):
    """``culprit_service`` failed first and caused the errors seen in ``symptom_service``."""

    culprit_service: str
    symptom_service: str
    first_seen: datetime | None
    example_error: str
    supporting_logs: int


def is_problem(hit: LogHit) -> bool:
    """True for WARN/ERROR-level log lines."""
    return (hit.level or "").upper() in PROBLEM_LEVELS


class ReasonerAgent:
    """
    Rules: every service that logs a problem, or is named inside another service's
    error message, is a candidate. Candidates are ranked by when they first showed a
    problem (root causes fail first). Optionally an LLM re-ranks them after reading
    the actual log lines and docs. The LLM can only reorder, never invent a service.
    """

    def __init__(self, services: list[str], llm: AsyncOpenAI | None = None, model: str = "", max_candidates: int = 5) -> None:
        self.services = services
        self.llm = llm
        self.model = model
        self.max_candidates = max_candidates

    def pick_symptom(self, context: RCASynthesisInput, named_services: list[str]) -> str:
        """The service the user asked about, else the service with the most problem lines."""
        if named_services:
            return named_services[0]
        counts = Counter(h.service for h in context.log_evidence if is_problem(h) and h.service)
        return counts.most_common(1)[0][0] if counts else "unknown"

    def candidates(self, context: RCASynthesisInput, symptom: str) -> list[Hypothesis]:
        """Rank services by first problem time; ties go to non-symptom services, then more evidence."""
        first_seen: dict[str, datetime | None] = {}
        example: dict[str, str] = {}
        support: Counter[str] = Counter()

        for hit in context.log_evidence:
            if not is_problem(hit):
                continue
            message = (hit.message or "").lower()
            involved = {hit.service} if hit.service else set()
            involved |= {s for s in self.services if s in message}
            for service in involved:
                support[service] += 1
                first_seen.setdefault(service, hit.timestamp)
                example.setdefault(service, hit.message or "")

        def rank(service: str) -> tuple[float, bool, int]:
            seen = first_seen[service]
            return (seen.timestamp() if seen else float("inf"), service == symptom, -support[service])

        return [
            Hypothesis(
                culprit_service=s, symptom_service=symptom, first_seen=first_seen[s],
                example_error=example[s], supporting_logs=support[s],
            )
            for s in sorted(first_seen, key=rank)[: self.max_candidates]
        ]

    async def reason(self, context: RCASynthesisInput, named_services: list[str]) -> list[Hypothesis]:
        """Return candidate hypotheses, most likely first."""
        symptom = self.pick_symptom(context, named_services)
        ranked = self.candidates(context, symptom)
        if self.llm is None or len(ranked) < 2:
            return ranked
        try:
            return await self._llm_rerank(context, ranked)
        except Exception as exc:  # LLM down or bad JSON: keep the rule-based order
            logger.warning("LLM re-rank failed, using rule order: %s", exc)
            return ranked

    async def _llm_rerank(self, context: RCASynthesisInput, ranked: list[Hypothesis]) -> list[Hypothesis]:
        payload = {
            "question": context.original_query,
            "symptom_service": ranked[0].symptom_service,
            "candidates": [h.culprit_service for h in ranked],
            "logs": [log_line(h) for h in context.log_evidence[:30]],
            "docs": [c.content[:300] for c in context.knowledge_context[:3]],
        }
        data = await ask_json(self.llm, self.model, load_prompt("reasoner"), json.dumps(payload))  # type: ignore[arg-type]
        by_name = {h.culprit_service: h for h in ranked}
        order = [s for s in data.get("ranking", []) if s in by_name]
        order += [s for s in by_name if s not in order]
        return [by_name[s] for s in order]
