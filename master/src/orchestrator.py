"""Runs the six Master steps in order for one user question."""

from __future__ import annotations

import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from openai import AsyncOpenAI

from nexgen_shared.schemas import KnowledgeResult, LogRetrievalResult, RCAReport, UserQuery

from src.context import ContextAssembler
from src.executor import DAGExecutor
from src.fixtures import FixtureBackend
from src.intent import IntentClassifier, IntentResult
from src.planner import DAGPlanner
from src.reasoner import Hypothesis, ReasonerAgent
from src.session import Message, SessionManager, SessionState
from src.settings import Settings
from src.synthesiser import RCASynthesiser
from src.topology import Topology
from src.validator import ValidatorAgent

logger = logging.getLogger(__name__)

ProgressCallback = Callable[[dict[str, Any]], Awaitable[None]]


@dataclass
class RunResult:
    """The report plus the internals the benchmark and the UI want to see."""

    report: RCAReport
    intent: IntentResult
    candidates: list[Hypothesis] = field(default_factory=list)
    accepted: Hypothesis | None = None
    tokens_before: int = 0
    tokens_after: int = 0
    timings_ms: dict[str, float] = field(default_factory=dict)


def low_confidence_report(query_id: str, reason: str) -> RCAReport:
    """A report that says the analysis could not be completed, and why."""
    return RCAReport(
        query_id=query_id,
        root_cause_summary=f"Analysis could not be completed: {reason}",
        confidence=0.0,
        evidence=[],
        recommended_actions=["Check that the Query and RAG services are reachable and the question names a service."],
        reasoning_trace_summary=reason,
        mttr_estimate_minutes=0,
        generated_at=datetime.now(timezone.utc),
    )


class MasterOrchestrator:
    """intent -> plan -> fetch (parallel) -> assemble context -> reason + validate -> synthesise."""

    def __init__(self, settings: Settings | None = None) -> None:
        settings = settings or Settings()
        llm = (
            AsyncOpenAI(api_key=settings.openai_api_key, base_url=settings.openai_base_url, max_retries=5)
            if settings.openai_api_key
            else None
        )
        model = settings.openai_model_name
        self.llm_enabled = llm is not None
        self.topology = Topology.load(settings.topology_path)
        self.sessions = SessionManager(settings.redis_url, settings.session_ttl_seconds)
        self.intent = IntentClassifier(self.topology.services, llm, model)
        self.planner = DAGPlanner()
        self.executor = DAGExecutor(
            settings.query_service_url,
            settings.rag_service_url,
            fixtures=FixtureBackend() if settings.mock_services else None,
            timeout=settings.http_timeout_seconds,
        )
        self.context = ContextAssembler(settings.max_synthesis_tokens)
        self.reasoner = ReasonerAgent(self.topology.services, llm, model)
        self.validator = ValidatorAgent(self.topology)
        self.synthesiser = RCASynthesiser(llm, model)

    @staticmethod
    def _missing_reason(intent: IntentResult, logs: LogRetrievalResult | None, docs: KnowledgeResult | None) -> str:
        """Say which required evidence is missing (not an unrelated failure, e.g. RAG down when logs were empty)."""
        problems = []
        if intent.logs_needed and (logs is None or not logs.hits):
            problems.append(logs.error if logs is not None and logs.error else "no matching log lines were found")
        if intent.docs_needed and not intent.logs_needed and (docs is None or not docs.chunks):
            problems.append(docs.error if docs is not None and docs.error else "no matching documents were found")
        return "; ".join(problems)

    async def execute_query(self, query: UserQuery, progress: ProgressCallback | None = None) -> RCAReport:
        """Answer one question. Never raises: failures become a zero-confidence report."""
        try:
            return (await self.run(query, progress)).report
        except Exception as exc:
            logger.exception("Master pipeline failed")
            return low_confidence_report(query.query_id, f"internal error: {exc}")

    async def run(self, query: UserQuery, progress: ProgressCallback | None = None) -> RunResult:
        """Run every step and return the report together with intermediate results."""
        timings: dict[str, float] = {}
        start = time.perf_counter()

        def lap(name: str, since: float) -> float:
            now = time.perf_counter()
            timings[name] = round((now - since) * 1000, 1)
            return now

        async def emit(stage: str, **data: Any) -> None:
            if progress is not None:
                await progress({"stage": stage, **data})

        session = await self.sessions.get(query.session_id) or SessionState(session_id=query.session_id)
        session.query_history.append(query)
        session.messages.append(Message(role="user", content=query.raw_text))

        # 1. Intent  2. Plan
        intent = await self.intent.classify(query.raw_text)
        graph = self.planner.plan(query, intent, self.topology)
        t = lap("intent_and_plan", start)
        await emit("intent", data=intent.model_dump())
        await emit("planner", data=graph.model_dump())

        # 3. Fetch logs and docs in parallel
        logs, docs = await self.executor.execute(graph, query.raw_text)
        t = lap("fetch", t)
        await emit("executor", logs=len(logs.hits) if logs else 0, docs=len(docs.chunks) if docs else 0)

        # 4. Context
        context = self.context.assemble(query.query_id, query.raw_text, logs, docs)
        if logs is not None:  # show which log search was run, so a bad one is easy to spot
            context.reasoning_trace.append(f"log search: {logs.kql_generated or '-'} -> {len(logs.hits)} lines")
        result = RunResult(
            report=low_confidence_report(query.query_id, "not run"),
            intent=intent,
            tokens_before=self.context.count_tokens(logs.hits if logs else []),
            tokens_after=self.context.count_tokens(context.log_evidence),
        )

        if not self.context.is_context_sufficient(intent, logs, docs):
            result.report = low_confidence_report(query.query_id, self._missing_reason(intent, logs, docs))
            result.report.reasoning_trace_summary = " | ".join(context.reasoning_trace + [result.report.reasoning_trace_summary])
        else:
            # 5. Reason + validate: take the first candidate that passes all checks
            if intent.logs_needed and not intent.is_quantitative:
                named = [hint.removesuffix("-*") for hint in intent.index_hints]
                result.candidates = await self.reasoner.reason(context, named)
                for hypothesis in result.candidates:
                    verdict = self.validator.validate(hypothesis, context)
                    status = "accepted" if verdict.accepted else "rejected"
                    context.reasoning_trace.append(f"{hypothesis.culprit_service}: {status} ({verdict.reason})")
                    if verdict.accepted:
                        result.accepted = hypothesis
                        break
                t = lap("reason_and_validate", t)
                await emit("reasoner", trace=list(context.reasoning_trace))

            # 6. Synthesise
            result.report = await self.synthesiser.synthesize(query.query_id, intent, context, result.accepted)
            t = lap("synthesis", t)

        session.messages.append(Message(role="assistant", content=result.report.root_cause_summary))
        await self.sessions.put(session)
        lap("total", start)
        result.timings_ms = timings
        await emit("final", data=result.report.model_dump(mode="json"))
        return result
