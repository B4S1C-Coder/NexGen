"""Step 6: build the final RCAReport."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from openai import AsyncOpenAI

from nexgen_shared.schemas import KnowledgeChunk, LogHit, RCAEvidenceItem, RCAReport, RCASynthesisInput

from src.context import log_line
from src.intent import IntentResult
from src.llm import ask_json, load_prompt
from src.reasoner import Hypothesis, is_problem, names

logger = logging.getLogger(__name__)


def mentions(hit: LogHit, service: str) -> bool:
    """True if the log line comes from ``service`` or names it."""
    return hit.service == service or names(hit.message or "", service)


class RCASynthesiser:
    """
    Facts (confidence, evidence, culprit) are always computed in code.
    The LLM, when configured, only writes the summary sentence and the actions.
    """

    def __init__(self, llm: AsyncOpenAI | None = None, model: str = "") -> None:
        self.llm = llm
        self.model = model

    @staticmethod
    def confidence(context: RCASynthesisInput, hypothesis: Hypothesis | None) -> float:
        """
        confidence = 0.6 * log_support + 0.3 * doc_support + 0.1 * topology_ok

        log_support: share of WARN/ERROR log lines that come from or mention the culprit.
        doc_support: share of retrieved docs that mention the culprit.
        topology_ok: 1 when a hypothesis passed validation (which includes the topology check).
        Without a hypothesis (count or docs-only questions) the supports are 1 when that
        kind of evidence exists, and topology_ok is 0.
        """
        problems = [h for h in context.log_evidence if is_problem(h)]
        docs = context.knowledge_context
        if hypothesis is None:
            log_support = 1.0 if context.log_evidence else 0.0
            doc_support = 1.0 if docs else 0.0
            topology_ok = 0.0
        else:
            culprit = hypothesis.culprit_service
            log_support = sum(mentions(h, culprit) for h in problems) / len(problems) if problems else 0.0
            doc_support = sum(names(c.content, culprit) for c in docs) / len(docs) if docs else 0.0
            topology_ok = 1.0
        return round(0.6 * log_support + 0.3 * doc_support + 0.1 * topology_ok, 2)

    async def synthesize(
        self,
        query_id: str,
        intent: IntentResult,
        context: RCASynthesisInput,
        hypothesis: Hypothesis | None,
    ) -> RCAReport:
        """Create the report. ``hypothesis`` is the accepted root cause, or None if there is none."""
        best_doc = self._best_doc(context.knowledge_context, hypothesis)
        summary, actions = self._template(intent, context, hypothesis, best_doc)
        if self.llm is not None:
            try:
                summary, actions = await self._llm_text(context, hypothesis, summary, actions)
            except Exception as exc:  # keep the template text if the LLM fails
                logger.warning("LLM summary failed, using template: %s", exc)

        if hypothesis is not None:
            related = [h for h in context.log_evidence if is_problem(h) and mentions(h, hypothesis.culprit_service)]
            docs = [c for c in context.knowledge_context if names(c.content, hypothesis.culprit_service)]
        else:
            related, docs = context.log_evidence, context.knowledge_context
        evidence = []
        if hypothesis is not None:  # machine-readable answer, first in the list
            evidence.append(RCAEvidenceItem(type="root_cause", ref=f"service:{hypothesis.culprit_service}",
                                            snippet=hypothesis.example_error))
        evidence += [RCAEvidenceItem(type="log", ref=f"trace_id:{h.trace_id}", snippet=h.message) for h in related[:5]]
        evidence += [RCAEvidenceItem(type=c.source_type, ref=c.source_uri) for c in docs[:3]]

        return RCAReport(
            query_id=query_id,
            root_cause_summary=summary,
            confidence=self.confidence(context, hypothesis),
            evidence=evidence,
            recommended_actions=actions,
            reasoning_trace_summary=" | ".join(context.reasoning_trace),
            # Rough heuristic: a matching runbook usually means a known, faster fix.
            mttr_estimate_minutes=15 if best_doc is not None else 30,
            generated_at=datetime.now(timezone.utc),
        )

    @staticmethod
    def _best_doc(chunks: list[KnowledgeChunk], hypothesis: Hypothesis | None) -> KnowledgeChunk | None:
        """The first doc that mentions the culprit, else the first doc."""
        if hypothesis is not None:
            for chunk in chunks:
                if names(chunk.content, hypothesis.culprit_service):
                    return chunk
        return chunks[0] if chunks else None

    @staticmethod
    def _template(
        intent: IntentResult, context: RCASynthesisInput, hypothesis: Hypothesis | None, best_doc: KnowledgeChunk | None
    ) -> tuple[str, list[str]]:
        """Plain-text summary and actions built without an LLM."""
        doc_steps = [s.strip() for s in best_doc.content.split(". ")[1:3]] if best_doc else []
        if hypothesis is not None:
            culprit, symptom = hypothesis.culprit_service, hypothesis.symptom_service
            when = hypothesis.first_seen.strftime("%H:%M:%S") if hypothesis.first_seen else "an unknown time"
            if culprit == symptom:
                summary = f'The problem started in {culprit} itself: "{hypothesis.example_error}" first appeared at {when} UTC.'
            else:
                summary = (
                    f'{culprit} is the most likely root cause: it first reported "{hypothesis.example_error}" '
                    f"at {when} UTC, and the failure then reached {symptom}."
                )
            return summary, doc_steps or [f"Check the health and recent changes of {culprit}."]
        if intent.is_quantitative:
            services = sorted({h.service for h in context.log_evidence if h.service})
            return f"Found {len(context.log_evidence)} distinct log events from: {', '.join(services)}.", []
        if not intent.logs_needed and best_doc is not None:
            return f"From {best_doc.source_uri}: {best_doc.content}", doc_steps
        return "No candidate root cause passed validation, so the cause is unclear.", [
            "Widen the time range or add the affected service name to the question."
        ]

    async def _llm_text(
        self, context: RCASynthesisInput, hypothesis: Hypothesis | None, summary: str, actions: list[str]
    ) -> tuple[str, list[str]]:
        payload = {
            "question": context.original_query,
            "root_cause": hypothesis.model_dump(mode="json") if hypothesis else None,
            "draft_summary": summary,
            "logs": [log_line(h) for h in context.log_evidence[:15]],
            "docs": [c.content[:500] for c in context.knowledge_context[:3]],
        }
        data = await ask_json(self.llm, self.model, load_prompt("synthesiser"), json.dumps(payload))  # type: ignore[arg-type]
        new_actions = [str(a) for a in data.get("actions", []) if str(a).strip()]
        return str(data.get("summary") or summary), new_actions or actions
