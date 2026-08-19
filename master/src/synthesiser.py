import os
import json
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
from pydantic import ValidationError
from openai import AsyncOpenAI

from nexgen_shared.schemas import UserQuery, LogRetrievalResult, KnowledgeResult, RCAReport

logger = logging.getLogger(__name__)

class RCASynthesiser:
    def __init__(self, openai_client: Optional[AsyncOpenAI] = None):
        self.llm = openai_client
        try:
            with open("src/prompts/synthesiser.txt", "r") as f:
                self.prompt_template = f.read()
        except FileNotFoundError:
            # Fallback path if run from a different root
            try:
                with open("prompts/synthesiser.txt", "r") as f:
                    self.prompt_template = f.read()
            except FileNotFoundError:
                self.prompt_template = "You are an RCA Synthesiser."

    async def synthesize(
        self, 
        query: UserQuery, 
        logs: Optional[LogRetrievalResult], 
        docs: Optional[KnowledgeResult], 
        session_history: List[Any]
    ) -> RCAReport:
        
        context_payload = {
            "query": query.raw_text,
            "logs": logs.model_dump() if logs else None,
            "docs": docs.model_dump() if docs else None
        }
        
        user_content = json.dumps(context_payload, default=str)

        # Baseline fallback without LLM client setup
        if not self.llm:
            if logs or docs:
                return self._build_evidence_report(query, logs, docs)
            return self._build_mock_report(query.query_id)

        try:
            response = await self.llm.chat.completions.create(
                model=os.getenv("OPENAI_MODEL_NAME", "llama3.2"),
                messages=[
                    {"role": "system", "content": self.prompt_template},
                    {"role": "user", "content": user_content}
                ],
                response_format={"type": "json_object"},
                temperature=0.2
            )
            raw_response = response.choices[0].message.content.strip()
            if raw_response.startswith("```json"): raw_response = raw_response[7:]
            if raw_response.startswith("```"): raw_response = raw_response[3:]
            if raw_response.endswith("```"): raw_response = raw_response[:-3]
            report_data = json.loads(raw_response.strip())
            
            # Boundary enforcement logic exactly mapped to P3-M5 specifications
            if "confidence" in report_data:
                conf = report_data["confidence"]
                if isinstance(conf, (int, float)):
                    if conf < 0.0: report_data["confidence"] = 0.0
                    elif conf > 1.0: report_data["confidence"] = 1.0
                else:
                    report_data["confidence"] = 0.5
                    
            # Fallback mappings for required fields ensuring strictly valid output
            if "mttr_estimate_minutes" not in report_data:
                report_data["mttr_estimate_minutes"] = 0
                
            return RCAReport(**report_data)
        
        except (json.JSONDecodeError, ValidationError) as e:
            logger.error(f"Failed to parse LLM response into RCAReport: {e}")
            return self._build_error_report(query.query_id, str(e))
        except Exception as e:
            logger.error(f"LLM Synthesis failed: {e}")
            return self._build_error_report(query.query_id, str(e))

    def _build_evidence_report(
        self,
        query: UserQuery,
        logs: Optional[LogRetrievalResult],
        docs: Optional[KnowledgeResult],
    ) -> RCAReport:
        """Build an RCA from retrieved logs and knowledge without an LLM.

        Args:
            query: Original user question.
            logs: Optional log retrieval payload.
            docs: Optional knowledge payload.

        Returns:
            A confidence-scored ``RCAReport`` citing both evidence types.
        """
        from nexgen_shared.schemas import RCAEvidenceItem

        evidence = []
        log_line = None
        if logs and logs.hits:
            hit = logs.hits[0]
            log_line = hit.message
            evidence.append(
                RCAEvidenceItem(
                    type="log",
                    ref=f"trace_id:{hit.trace_id or 'unknown'}",
                    snippet=hit.message,
                )
            )
        runbook_line = None
        if docs and docs.chunks:
            chunk = docs.chunks[0]
            runbook_line = chunk.content
            evidence.append(
                RCAEvidenceItem(
                    type=chunk.source_type,
                    ref=chunk.source_uri,
                    snippet=chunk.content[:240],
                )
            )

        if log_line and runbook_line:
            summary = (
                f"{log_line}. Organisational guidance: {runbook_line}"
            )
            confidence = 0.86
        elif log_line:
            summary = f"Log evidence indicates: {log_line}"
            confidence = 0.62
        elif runbook_line:
            summary = f"Knowledge context: {runbook_line}"
            confidence = 0.55
        else:
            summary = f"No structured evidence for: {query.raw_text}"
            confidence = 0.2

        actions = []
        if docs and docs.chunks:
            actions.append(docs.chunks[0].content.split(".")[0] + ".")
        actions.append("Confirm replica lag and primary disk health before failback.")

        kql = logs.kql_generated if logs else ""
        return RCAReport(
            query_id=query.query_id,
            root_cause_summary=summary,
            confidence=confidence,
            evidence=evidence,
            recommended_actions=actions,
            reasoning_trace_summary=(
                f"Intent routed to downstream services. "
                f"KQL={kql or 'n/a'}. "
                f"Logs={logs.hit_count if logs else 0}, "
                f"chunks={len(docs.chunks) if docs else 0}."
            ),
            mttr_estimate_minutes=5,
            generated_at=datetime.now(timezone.utc),
        )

    def _build_mock_report(self, query_id: str) -> RCAReport:
        return RCAReport(
            query_id=query_id,
            root_cause_summary="Mock Generated RCA. Instantiate Synthesiser with an AsyncOpenAI client targeting llama.cpp for full generation.",
            confidence=1.0,
            evidence=[],
            recommended_actions=["Check the mock system."],
            reasoning_trace_summary="No LLM provided. Returning safely without crashing.",
            mttr_estimate_minutes=0,
            generated_at=datetime.now(timezone.utc).isoformat()
        )

    def _build_error_report(self, query_id: str, error_msg: str) -> RCAReport:
        return RCAReport(
            query_id=query_id,
            root_cause_summary=f"Synthesis failed internally: {error_msg}",
            confidence=0.0,
            evidence=[],
            recommended_actions=["Check synthesis pipeline."],
            reasoning_trace_summary="Failed to synthesize RCA.",
            mttr_estimate_minutes=-1,
            generated_at=datetime.now(timezone.utc).isoformat()
        )
