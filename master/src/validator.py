import os
import json
import logging
from typing import Optional, List
from openai import AsyncOpenAI
from pydantic import BaseModel
from pathlib import Path

from src.reasoner import AcceptedHypothesis
from src.context import RCASynthesisInput
from nexgen_shared.errors import E008TopologyVerificationRejected

logger = logging.getLogger(__name__)

class ValidatorCritique(BaseModel):
    is_valid: bool
    reason: str
    extracted_edges: List[List[str]] = []

class ValidatorAgent:
    """
    Validates logical assumptions and extracts dependencies from generated hypotheses 
    verifying if the proposed connection logically aligns within the fixed system topology graph.
    """
    def __init__(self, openai_client: Optional[AsyncOpenAI] = None, max_cycles: int = 3):
        self.llm = openai_client
        self.max_cycles = max_cycles
        self.topology = self._load_topology()
        
        try:
            with open("src/prompts/validator.txt", "r") as f:
                self.prompt_template = f.read()
        except FileNotFoundError:
            self.prompt_template = "Critique the hypothesis."
            
    def _load_topology(self) -> dict:
        p = Path("config/topology.json")
        if p.exists():
            return json.loads(p.read_text())
        return {}

    async def validate(self, hypothesis: AcceptedHypothesis, context: RCASynthesisInput) -> bool:
        # 1. Knowledge rounding check
        if hypothesis.supporting_evidence_count == 0:
            logger.info("Hypothesis REJECTED: Zero knowledge grounding support.")
            return False

        # 2. Adversarial Critique
        if not self.llm:
            return self._mock_validate(hypothesis)

        payload = {
            "hypothesis": hypothesis.description,
            "evidence_count": hypothesis.supporting_evidence_count,
            "query": context.original_query
        }
        
        try:
            res = await self.llm.chat.completions.create(
                model=os.getenv("OPENAI_MODEL_NAME", "llama3.2"),
                messages=[
                    {"role": "system", "content": self.prompt_template},
                    {"role": "user", "content": json.dumps(payload)}
                ],
                response_format={"type": "json_object"},
                temperature=0.1
            )
            raw = res.choices[0].message.content.strip()
            if raw.startswith("```json"): raw = raw[7:]
            if raw.startswith("```"): raw = raw[3:]
            if raw.endswith("```"): raw = raw[:-3]
            data = json.loads(raw.strip())
            critique = ValidatorCritique(**data)
            
            if not critique.is_valid:
                logger.info(f"Adversarial critique rejected hypothesis: {critique.reason}")
                return False
                
            # 3. Topology Edge validation (E008 verification barrier)
            for edge in critique.extracted_edges:
                if len(edge) == 2:
                    source, target = edge
                    if source in self.topology:
                        deps = self.topology[source].get("dependencies", [])
                        if target not in deps:
                            raise E008TopologyVerificationRejected(f"Edge '{source}' -> '{target}' does not exist in network topology graph.")
                    else:
                        raise E008TopologyVerificationRejected(f"Source service '{source}' does not exist in network topology graph.")
            return True
            
        except E008TopologyVerificationRejected:
            raise
        except Exception as e:
            logger.error(f"Validator logic sequence crashed internally: {e}")
            return False

    def _mock_validate(self, hypothesis: AcceptedHypothesis) -> bool:
        """Fallback mock returning boolean passes natively."""
        if "fraud" in hypothesis.description.lower() or "missing_edge" in hypothesis.description.lower():
            raise E008TopologyVerificationRejected("Mock caught a topology missing edge reject logic.")
        return True
