import os
import json
import logging
from typing import Optional, List
from pydantic import BaseModel
from openai import AsyncOpenAI

from src.context import RCASynthesisInput

logger = logging.getLogger(__name__)

class ToTNode(BaseModel):
    id: str
    depth: int
    description: str
    contradictions: int = 0
    supporting_evidence_count: int = 0
    is_accepted: bool = False

class AcceptedHypothesis(BaseModel):
    id: str
    description: str
    contradictions: int
    supporting_evidence_count: int
    is_accepted: bool

class ReasonerAgent:
    """
    Implements a miniature Tree-of-Thoughts reasoning mechanism using Best-First Search 
    to evaluate context logically up to a predefined max width and depth, pruning 
    contradictory paths heavily.
    """
    def __init__(self, openai_client: Optional[AsyncOpenAI] = None):
        self.llm = openai_client
        self.max_depth = 3
        self.max_branches = 3
        try:
            with open("src/prompts/reasoner.txt", "r") as f:
                self.prompt_template = f.read()
        except FileNotFoundError:
            self.prompt_template = "Evaluate hypothesis critically."

    async def reason(self, context: RCASynthesisInput) -> List[AcceptedHypothesis]:
        if not self.llm:
            return self._mock_reasoning_flow()
            
        # Depth 1: Generate initial branches (up to max_branches limit)
        initial_nodes = await self._generate_or_expand(context, "Initial Hypothesis Generation", depth=1)
        
        # Best First Search Queue dynamically sorts by lowest contradictions, then highest support
        queue = sorted(initial_nodes, key=lambda n: (n.contradictions, -n.supporting_evidence_count))
        accepted = []
        
        while queue:
            node = queue.pop(0)
            
            # Prune path entirely on 2+ contradictions
            if node.contradictions >= 2:
                continue
                
            # If the node is a leaf (max depth 3) or definitively evaluated by the LLM
            if node.depth == self.max_depth or node.is_accepted:
                node.is_accepted = True
                accepted.append(node)
                continue
                
            # Expand recursively if it needs refinement
            kids = await self._generate_or_expand(context, f"Refining based on: {node.description}", depth=node.depth + 1)
            for k in kids:
                queue.append(k)
                
            # Re-sort for Best-First
            queue = sorted(queue, key=lambda n: (n.contradictions, -n.supporting_evidence_count))
            # Bound dynamic explosion queue scaling
            queue = queue[:self.max_branches * self.max_depth]

        final_accepted = []
        for a in accepted[:3]: # Cap hard output limit to top 3 branches optimally
            final_accepted.append(AcceptedHypothesis(
                id=a.id, 
                description=a.description, 
                contradictions=a.contradictions, 
                supporting_evidence_count=a.supporting_evidence_count, 
                is_accepted=a.is_accepted
            ))
            
        if not final_accepted:
            return [self._default_hypothesis()]
            
        return final_accepted

    async def _generate_or_expand(self, context: RCASynthesisInput, prompt: str, depth: int) -> List[ToTNode]:
        payload = {
            "task": prompt,
            "query": context.original_query,
            "log_evidence_count": len(context.log_evidence), 
            "knowledge_evidence_count": len(context.knowledge_context)
        }
        
        try:
            response = await self.llm.chat.completions.create(
                model=os.getenv("OPENAI_MODEL_NAME", "llama3.2"),
                messages=[
                    {"role": "system", "content": self.prompt_template},
                    {"role": "user", "content": json.dumps(payload)}
                ],
                response_format={"type": "json_object"},
                temperature=0.3
            )
            raw = response.choices[0].message.content.strip()
            if raw.startswith("```json"): raw = raw[7:]
            if raw.startswith("```"): raw = raw[3:]
            if raw.endswith("```"): raw = raw[:-3]
            data = json.loads(raw.strip())
            
            nodes = []
            for i, h in enumerate(data.get("hypotheses", [])[:self.max_branches]):
                nodes.append(ToTNode(
                    id=f"tot_d{depth}_{i}",
                    depth=depth,
                    description=h.get("description", "Unknown Logic Node"),
                    contradictions=h.get("contradictions", 0),
                    supporting_evidence_count=h.get("supporting_evidence_count", 0),
                    is_accepted=h.get("is_accepted", False)
                ))
            return nodes
        except Exception as e:
            logger.error(f"ToT Expansion failed: {e}")
            return []

    def _mock_reasoning_flow(self) -> List[AcceptedHypothesis]:
        return [AcceptedHypothesis(id="m_1", description="Mock reasoned branch.", contradictions=0, supporting_evidence_count=2, is_accepted=True)]
        
    def _default_hypothesis(self) -> AcceptedHypothesis:
        return AcceptedHypothesis(id="h_def", description="Definitive hypothesis mapping fallback due to systemic reasoning drop.", contradictions=0, supporting_evidence_count=0, is_accepted=True)
