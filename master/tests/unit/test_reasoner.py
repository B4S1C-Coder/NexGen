from unittest.mock import AsyncMock, patch

from src.reasoner import ReasonerAgent

SERVICES = ["db-primary", "gateway", "notifications", "payments"]


async def test_rules_rank_by_first_problem_time(context):
    ranked = await ReasonerAgent(SERVICES).reason(context, ["payments"])
    assert [h.culprit_service for h in ranked] == ["notifications", "db-primary", "payments", "gateway"]
    db = ranked[1]
    assert db.symptom_service == "payments"
    assert db.supporting_logs == 2  # its own log line + payments naming it


async def test_tie_prefers_service_that_is_not_the_symptom(hit, context):
    context.log_evidence = [hit("10:00:00", "payments", "ERROR", "Connection refused: db-primary:5432")]
    ranked = await ReasonerAgent(SERVICES).reason(context, ["payments"])
    assert ranked[0].culprit_service == "db-primary"


async def test_symptom_defaults_to_noisiest_service(hit, context):
    reasoner = ReasonerAgent(SERVICES)
    assert reasoner.pick_symptom(context, []) in SERVICES
    context.log_evidence.append(hit("09:58:00", "gateway", "ERROR", "again"))
    assert reasoner.pick_symptom(context, []) == "gateway"


async def test_llm_can_only_reorder_known_candidates(context):
    reasoner = ReasonerAgent(SERVICES, llm=AsyncMock(), model="m")
    reply = {"ranking": ["db-primary", "made-up-service", "payments"]}
    with patch("src.reasoner.ask_json", AsyncMock(return_value=reply)):
        ranked = await reasoner.reason(context, ["payments"])
    assert [h.culprit_service for h in ranked] == ["db-primary", "payments", "notifications", "gateway"]


async def test_llm_failure_keeps_rule_order(context):
    reasoner = ReasonerAgent(SERVICES, llm=AsyncMock(), model="m")
    with patch("src.reasoner.ask_json", AsyncMock(side_effect=ValueError("bad json"))):
        ranked = await reasoner.reason(context, ["payments"])
    assert ranked[0].culprit_service == "notifications"
