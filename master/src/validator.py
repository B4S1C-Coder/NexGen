"""Step 5b: reject candidate root causes that break simple, checkable facts."""

from __future__ import annotations

from pydantic import BaseModel

from nexgen_shared.errors import E008TopologyVerificationRejected
from nexgen_shared.schemas import RCASynthesisInput

from src.reasoner import Hypothesis, is_problem
from src.topology import Topology


class Verdict(BaseModel):
    """Result of validating one hypothesis."""

    accepted: bool
    reason: str


class ValidatorAgent:
    """
    Three deterministic checks, no LLM:
      1. Topology: the symptom service must call the culprit (directly or indirectly),
         otherwise the failure could not have reached it (error code E008).
      2. Timing: the culprit must show a problem no later than the symptom's first error.
      3. Grounding: the culprit must appear in the log evidence or the docs.
    """

    def __init__(self, topology: Topology) -> None:
        self.topology = topology

    def validate(self, hypothesis: Hypothesis, context: RCASynthesisInput) -> Verdict:
        """Run the three checks in order and return the first failure, or acceptance."""
        culprit, symptom = hypothesis.culprit_service, hypothesis.symptom_service

        if symptom in self.topology.graph and not self.topology.can_reach(symptom, culprit):
            error = E008TopologyVerificationRejected(f"{symptom} does not depend on {culprit}")
            return Verdict(accepted=False, reason=str(error))

        symptom_times = [
            h.timestamp for h in context.log_evidence if h.service == symptom and is_problem(h) and h.timestamp
        ]
        if symptom_times and hypothesis.first_seen and hypothesis.first_seen > min(symptom_times):
            return Verdict(accepted=False, reason=f"{culprit} first failed after {symptom} did")

        mentioned = any(
            culprit == h.service or culprit in (h.message or "").lower() for h in context.log_evidence
        ) or any(culprit in c.content.lower() for c in context.knowledge_context)
        if not mentioned:
            return Verdict(accepted=False, reason=f"no log line or doc mentions {culprit}")

        return Verdict(accepted=True, reason="topology, timing and grounding checks passed")
