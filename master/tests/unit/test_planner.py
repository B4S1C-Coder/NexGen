from datetime import datetime, timezone

import pytest

from nexgen_shared.schemas import UserQuery

from src.intent import IntentResult
from src.planner import DAGPlanner


@pytest.fixture
def query() -> UserQuery:
    return UserQuery(query_id="q1", raw_text="test", session_id="s1", timestamp_utc=datetime.now(timezone.utc))


def test_both_routes_give_three_tasks(query, topology):
    graph = DAGPlanner().plan(query, IntentResult(logs_needed=True, docs_needed=True), topology)
    assert [n.action_type for n in graph.nodes] == ["FETCH_LOGS", "FETCH_DOCS", "SYNTHESIZE"]
    assert graph.nodes[-1].dependencies == ["fetch_logs", "fetch_docs"]


def test_logs_only_gives_two_tasks(query, topology):
    graph = DAGPlanner().plan(query, IntentResult(logs_needed=True, docs_needed=False), topology)
    assert [n.action_type for n in graph.nodes] == ["FETCH_LOGS", "SYNTHESIZE"]
    assert graph.nodes[-1].dependencies == ["fetch_logs"]


def test_index_hints_include_called_services(query, topology):
    intent = IntentResult(logs_needed=True, docs_needed=False, index_hints=["payments-*"])
    graph = DAGPlanner().plan(query, intent, topology)
    assert graph.nodes[0].payload["index_hints"] == ["payments-*", "db-primary-*"]


def test_troubleshooting_asks_for_all_problem_logs_downstream(query, topology):
    intent = IntentResult(logs_needed=True, docs_needed=True, index_hints=["gateway-*"])
    payload = DAGPlanner().plan(query, intent, topology).nodes[0].payload
    assert payload["index_hints"] == ["gateway-*", "payments-*", "db-primary-*"]
    assert payload["natural_language"] == "All log events with level WARN or ERROR"


def test_count_question_passes_user_wording(query, topology):
    intent = IntentResult(logs_needed=True, docs_needed=False, is_quantitative=True)
    assert DAGPlanner().plan(query, intent, topology).nodes[0].payload["natural_language"] == "test"
