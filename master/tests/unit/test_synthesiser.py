from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

from src.intent import IntentResult
from src.reasoner import Hypothesis
from src.synthesiser import RCASynthesiser

RCA_INTENT = IntentResult(logs_needed=True, docs_needed=True)


@pytest.fixture
def db_down() -> Hypothesis:
    return Hypothesis(
        culprit_service="db-primary", symptom_service="payments",
        first_seen=datetime(2026, 9, 14, 9, 56, 58, tzinfo=timezone.utc),
        example_error="database system is shutting down", supporting_logs=2,
    )


def test_confidence_formula(context, db_down):
    # 2 of 4 problem lines mention db-primary, 1 of 1 doc does, topology passed:
    # 0.6 * 0.5 + 0.3 * 1.0 + 0.1 * 1 = 0.7
    assert RCASynthesiser.confidence(context, db_down) == 0.7


async def test_template_report_without_llm(context, db_down):
    report = await RCASynthesiser().synthesize("q1", RCA_INTENT, context, db_down)
    assert report.root_cause_summary.startswith("db-primary is the most likely root cause")
    assert report.recommended_actions == ["Promote db-replica-1", "Restart pools."]
    assert [e.type for e in report.evidence] == ["root_cause", "log", "log", "runbook"]
    assert report.evidence[0].ref == "service:db-primary"
    assert report.mttr_estimate_minutes == 15


async def test_llm_writes_text_but_not_confidence(context, db_down):
    reply = {"summary": "DB went down.", "actions": ["Fail over"], "confidence": 0.99}
    with patch("src.synthesiser.ask_json", AsyncMock(return_value=reply)):
        report = await RCASynthesiser(llm=AsyncMock(), model="m").synthesize("q1", RCA_INTENT, context, db_down)
    assert report.root_cause_summary == "DB went down."
    assert report.recommended_actions == ["Fail over"]
    assert report.confidence == 0.7


async def test_no_accepted_hypothesis_gives_unclear_report(context):
    report = await RCASynthesiser().synthesize("q1", RCA_INTENT, context, None)
    assert "unclear" in report.root_cause_summary
