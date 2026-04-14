import logging
import asyncio
import os
from typing import Optional, List, Dict, Any
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

logger = logging.getLogger(__name__)

class MasterOrchestrator:
    """
    Central Cognitive Loop linking all discrete Master components into 
    the final Phase 4 Directed Acyclic Pipeline.
    """
    def __init__(self):
        # Read API Keys uniquely overriding via .env safely ensuring compat checks hold true dynamically
        api_key = os.getenv("OPENAI_API_KEY", "dummy_key_or_actual_key")
        base_url = os.getenv("OPENAI_BASE_URL", "http://localhost:11434/v1")
        is_mock = os.getenv("MOCK_SERVICES", "false").lower() == "true"
        
        # Instantiate standard async payload provider for inference mapping or fallback seamlessly
        if is_mock and (api_key == "dummy_key_or_actual_key" or "dummy" in api_key):
            self.llm = None
        else:
            self.llm = AsyncOpenAI(api_key=api_key, base_url=base_url)

        # Initialize internal state classes
        self.session_manager = SessionManager(redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
        self.intent_classifier = IntentClassifier(openai_client=self.llm)
        self.planner = DAGPlanner()
        self.executor = DAGExecutor()
        self.context_assembler = ContextAssembler()
        self.reasoner = ReasonerAgent(openai_client=self.llm)
        self.validator = ValidatorAgent(openai_client=self.llm)
        self.synthesiser = RCASynthesiser(openai_client=self.llm)
        
        self._load_topology()
        
    def _load_topology(self):
        p = Path("config/topology.json")
        if p.exists():
            with open(p, "r") as f:
                self.topology = json.load(f)
        else:
            self.topology = {}

    async def execute_query(self, query: UserQuery, progress_callback=None) -> RCAReport:
        """
        Executes the overall NexGen RCA logic loop.
        If progress_callback is provided, it calls the function with trace metrics dynamically.
        """
        try:
            # 1. State Retention Block
            session = await self.session_manager.get(query.session_id)
            if not session:
                session = SessionState(session_id=query.session_id)
            session.active_context_window.append(Message(role="user", content=query.raw_text))
            await self.session_manager.put(query.session_id, session)
            
            if progress_callback: await progress_callback({"stage": "session", "msg": "Session loaded and history updated."})

            # 2. Intent parsing intelligently
            intent = await self.intent_classifier.classify(query.raw_text)
            if progress_callback: await progress_callback({"stage": "intent", "data": intent.model_dump()})

            # 3. DAG Construction
            graph = self.planner.plan(query, intent, self.topology)
            if progress_callback: await progress_callback({"stage": "planner", "data": graph.model_dump()})

            # 4. Asynchronous Pipeline Graph execution fetching parallel resources optimally
            datasets = await self.executor.execute(graph, query.query_id, query.raw_text)
            
            logs_result = None
            docs_result = None
            
            for key, val in datasets.items():
                if isinstance(val, LogRetrievalResult):
                    logs_result = val
                elif isinstance(val, KnowledgeResult):
                    docs_result = val
                    
            if progress_callback: await progress_callback({"stage": "executor", "metrics": {"logs_fetched": bool(logs_result), "docs_fetched": bool(docs_result)}})

            # 5. Context compilation strictly validating required logs exist structurally
            if not self.context_assembler.is_context_sufficient(intent, logs_result):
                 error_report = self._build_low_confidence_report(query.query_id, "Insufficient logs retrieved for deterministic analysis.")
                 if progress_callback: await progress_callback({"stage": "final", "data": error_report.model_dump()})
                 return error_report

            synthesis_input = self.context_assembler.assemble(query.raw_text, query.query_id, logs_result, docs_result, intent)

            # 6. Mini T.O.T reasoning framework spanning maximum 3 adversarial cycles internally protecting RCA boundaries
            valid_hypothesis = None
            for cycle in range(3):
                hypotheses = await self.reasoner.reason(synthesis_input)
                if progress_callback: await progress_callback({"stage": "reasoner", "cycle": cycle + 1, "hypotheses": [h.model_dump() for h in hypotheses]})
                
                for h in hypotheses:
                    if await self.validator.validate(h, synthesis_input):
                        valid_hypothesis = h
                        break
                        
                if valid_hypothesis:
                    break
                    
            # Update trace logically
            if valid_hypothesis:
                synthesis_input.reasoning_trace.append(f"Accepted Hypothesis: {valid_hypothesis.description}")
            else:
                synthesis_input.reasoning_trace.append("Validation cycles exhausted with 0 secure logic paths anchored safely.")

            # 7. RCA Synthesis Execution explicitly injecting context states natively
            report = await self.synthesiser.synthesize(query, logs_result, docs_result, session.active_context_window)
            
            # Post Synthesis Cleanup
            session.active_context_window.append(Message(role="assistant", content=report.root_cause_summary))
            await self.session_manager.put(session.session_id, self.session_manager.trim_context(session))
            
            if progress_callback: await progress_callback({"stage": "final", "data": report.model_dump()})
            return report

        except Exception as e:
            logger.error(f"Master orchestrator halted violently: {e}", exc_info=True)
            error_report = self._build_low_confidence_report(query.query_id, str(e))
            if progress_callback: await progress_callback({"stage": "final", "data": error_report.model_dump()})
            return error_report

    def _build_low_confidence_report(self, query_id: str, reason: str) -> RCAReport:
        return RCAReport(
            query_id=query_id,
            root_cause_summary=f"Analysis halted. Reason: {reason}",
            confidence=0.0,
            evidence=[],
            recommended_actions=["Review system error outputs.", "Check LLM configurations explicitly if local."],
            reasoning_trace_summary="Pipeline loop terminated early.",
            mttr_estimate_minutes=0,
            generated_at=datetime.now(timezone.utc).isoformat()
        )
