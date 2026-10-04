from datetime import datetime, timezone

from src.reasoner import Hypothesis
from src.topology import Topology
from src.validator import ValidatorAgent


def hypothesis(culprit: str, symptom: str = "payments", minute: int = 56) -> Hypothesis:
    return Hypothesis(
        culprit_service=culprit, symptom_service=symptom,
        first_seen=datetime(2026, 9, 14, 9, minute, tzinfo=timezone.utc),
        example_error="x", supporting_logs=1,
    )


def test_topology_rejects_unrelated_service_with_e008(topology, context):
    verdict = ValidatorAgent(topology).validate(hypothesis("notifications", minute=30), context)
    assert not verdict.accepted and "[E008]" in verdict.reason


def test_timing_rejects_culprit_that_failed_after_symptom(topology, context):
    verdict = ValidatorAgent(topology).validate(hypothesis("db-primary", minute=59), context)
    assert not verdict.accepted and "after" in verdict.reason


def test_grounding_rejects_culprit_missing_from_evidence(context):
    topo = Topology({"payments": {"dependencies": ["ledger"]}})
    verdict = ValidatorAgent(topo).validate(hypothesis("ledger"), context)
    assert not verdict.accepted and "no log line or doc" in verdict.reason


def test_valid_hypothesis_is_accepted(topology, context):
    assert ValidatorAgent(topology).validate(hypothesis("db-primary"), context).accepted
