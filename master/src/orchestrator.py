import logging
import os
from typing import Optional, List, Dict, Any, Callable, Awaitable
from datetime import datetime, timezone
import json
from pathlib import Path

from openai import AsyncOpenAI

from nexgen_shared.schemas import UserQuery, RCAReport, LogRetrievalResult, KnowledgeResult
from src.session import SessionManager, SessionState, Message
from src.intent import IntentClassifier
from src.planner import DAGPlanner
from src.executor import DAGExecutor
from src.context import ContextAssembler
from src.reasoner import ReasonerAgent
from src.validator import ValidatorAgent
from src.synthesiser import RCASynthesiser
from src.settings import Settings

logger = logging.getLogger(__name__)

ProgressCallback = Callable[[dict[str, Any]], Awaitable[None]]


class MasterOrchestrator:
    """Central cognitive loop that routes a UserQuery through Query and RAG."""

    def __init__(self, settings: Settings | None = None):
        """Wire session, planning, HTTP fetch, reasoning, and synthesis.

        Args:
            settings: Loaded Master settings. A default ``Settings()`` is used
                when omitted (unit tests).
        """
        self.settings = settings or Settings()
        api_key = os.getenv("OPENAI_API_KEY", "")
        base_url = os.getenv("OPENAI_BASE_URL", "http://localhost:11434/v1")
        has_real_llm = bool(api_key) and "dummy" not in api_key.lower()

        self.llm = (
            AsyncOpenAI(api_key=api_key, base_url=base_url)
            if has_real_llm
            else None
        )

        memory_only = self.settings.mock_services or not has_real_llm
        self.session_manager = SessionManager(
            redis_url=self.settings.redis_url,
            ttl_seconds=self.settings.session_ttl_seconds,
            memory_only=memory_only,
        )
        self.intent_classifier = IntentClassifier(openai_client=self.llm)
        self.planner = DAGPlanner()
        self.executor = DAGExecutor(
            query_service_url=self.settings.query_service,
            rag_service_url=self.settings.rag_service_url,
        )
        self.context_assembler = ContextAssembler()
        self.reasoner = ReasonerAgent(openai_client=self.llm)
        self.validator = ValidatorAgent(openai_client=self.llm)
        self.synthesiser = RCASynthesiser(openai_client=self.llm)
        self.traces: dict[str, list[dict[str, Any]]] = {}
        self._load_topology()

    def _load_topology(self) -> None:
        p = Path(self.settings.topology_config_path)
        if not p.is_file():
            p = Path("config/topology.json")
        if p.is_file():
            with open(p, "r", encoding="utf-8") as f:
                self.topology = json.load(f)
        else:
            self.topology = {
                "payments": {"dependencies": ["db-primary", "gateway"]},
                "gateway": {"dependencies": ["payments", "auth"]},
            }

    async def execute_query(
        self,
        query: UserQuery,
        progress_callback: ProgressCallback | None = None,
    ) -> RCAReport:
        """Execute the RCA loop: intent → DAG fetch → reason → synthesise.

        Args:
            query: Canonical inbound user question.
            progress_callback: Optional async hook invoked with stage events
                for the TUI live trace.

        Returns:
            An ``RCAReport``, including a low-confidence report on failure.
        """
        events: list[dict[str, Any]] = []
        self.traces[query.query_id] = events

        async def emit(payload: dict[str, Any]) -> None:
            events.append(payload)
            if progress_callback:
                await progress_callback(payload)

        try:
            session = await self.session_manager.get(query.session_id)
            if not session:
                session = SessionState(session_id=query.session_id)
            session.active_context_window.append(Message(role="user", content=query.raw_text))
            session.query_history.append(query)
            await self.session_manager.put(query.session_id, session)

            await emit({"stage": "session", "msg": "Session loaded and history updated."})

            intent = await self.intent_classifier.classify(query.raw_text)
            await emit({"stage": "intent", "data": intent.model_dump()})

            graph = self.planner.plan(query, intent, self.topology)
            await emit({"stage": "planner", "data": graph.model_dump()})

            datasets = await self.executor.execute(graph, query.query_id, query.raw_text)

            logs_result = None
            docs_result = None

            for key, val in datasets.items():
                if isinstance(val, LogRetrievalResult):
                    logs_result = val
                elif isinstance(val, KnowledgeResult):
                    docs_result = val

            await emit({
                "stage": "executor",
                "metrics": {
                    "logs_fetched": bool(logs_result),
                    "docs_fetched": bool(docs_result),
                    "log_hits": logs_result.hit_count if logs_result else 0,
                    "doc_chunks": len(docs_result.chunks) if docs_result else 0,
                    "kql": logs_result.kql_generated if logs_result else None,
                },
            })

            if not self.context_assembler.is_context_sufficient(intent, logs_result):
                error_report = self._build_low_confidence_report(
                    query.query_id,
                    "Insufficient logs retrieved for deterministic analysis.",
                )
                await emit({"stage": "final", "data": error_report.model_dump(mode="json")})
                return error_report

            synthesis_input = self.context_assembler.assemble(
                query.raw_text, query.query_id, logs_result, docs_result, intent
            )

            valid_hypothesis = None
            for cycle in range(self.settings.max_validator_cycles):
                hypotheses = await self.reasoner.reason(synthesis_input)

                for h in hypotheses:
                    h.is_accepted = False
                    try:
                        if await self.validator.validate(h, synthesis_input):
                            h.is_accepted = True
                            valid_hypothesis = h
                            break
                    except Exception as e:
                        if "E008" in type(e).__name__:
                            h.is_accepted = False
                            h.contradictions += 2
                            h.description += f" [Rejected: {str(e)}]"
                        else:
                            raise e

                await emit({
                    "stage": "reasoner",
                    "cycle": cycle + 1,
                    "hypotheses": [h.model_dump() for h in hypotheses],
                })

                if valid_hypothesis:
                    break

            if valid_hypothesis:
                synthesis_input.reasoning_trace.append(
                    f"Accepted Hypothesis: {valid_hypothesis.description}"
                )
            else:
                synthesis_input.reasoning_trace.append(
                    "Validation cycles exhausted with 0 secure logic paths."
                )

            report = await self.synthesiser.synthesize(
                query, logs_result, docs_result, session.active_context_window
            )

            session.active_context_window.append(
                Message(role="assistant", content=report.root_cause_summary)
            )
            await self.session_manager.put(
                session.session_id, self.session_manager.trim_context(session, query.raw_text)
            )

            await emit({"stage": "final", "data": report.model_dump(mode="json")})
            return report

        except Exception as e:
            logger.error("Master orchestrator halted: %s", e, exc_info=True)
            error_report = self._build_low_confidence_report(query.query_id, str(e))
            await emit({"stage": "final", "data": error_report.model_dump(mode="json")})
            return error_report

    def _build_low_confidence_report(self, query_id: str, reason: str) -> RCAReport:
        return RCAReport(
            query_id=query_id,
            root_cause_summary=f"Analysis halted. Reason: {reason}",
            confidence=0.0,
            evidence=[],
            recommended_actions=[
                "Review system error outputs.",
                "Check that query (8001) and rag (8002) are reachable.",
            ],
            reasoning_trace_summary="Pipeline loop terminated early.",
            mttr_estimate_minutes=0,
            generated_at=datetime.now(timezone.utc),
        )
