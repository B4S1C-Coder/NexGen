from unittest.mock import AsyncMock, patch

import httpx
import pytest

from src.executor import DAGExecutor
from src.fixtures import FixtureBackend
from src.planner import ExecutionGraph, ExecutionNode


@pytest.fixture
def graph() -> ExecutionGraph:
    return ExecutionGraph(query_id="q1", nodes=[
        ExecutionNode(step_id="fetch_logs", action_type="FETCH_LOGS"),
        ExecutionNode(step_id="fetch_docs", action_type="FETCH_DOCS"),
        ExecutionNode(step_id="synthesize", action_type="SYNTHESIZE", dependencies=["fetch_logs", "fetch_docs"]),
    ])


async def test_fixture_mode_returns_both_results(graph):
    logs, docs = await DAGExecutor(fixtures=FixtureBackend()).execute(graph, "Why is payments returning HTTP 500 errors?")
    assert logs is not None and logs.status == "success" and logs.hits
    assert docs is not None and docs.status == "success" and docs.chunks


async def test_http_mode_parses_responses(graph):
    log_body = {"query_id": "q1", "status": "success", "kql_generated": "x", "syntax_valid": True,
                "refinement_attempts": 0, "hits": [], "hit_count": 0, "error": None}
    doc_body = {"query_id": "q1", "status": "success", "chunks": [], "total_tokens_after_compression": 0,
                "conflict_detected": False, "error": None}

    async def fake_post(url, request):
        return log_body if url.endswith("/retrieve") else doc_body

    executor = DAGExecutor()
    with patch.object(executor, "_post", side_effect=fake_post):
        logs, docs = await executor.execute(graph, "q")
    assert logs is not None and logs.kql_generated == "x"
    assert docs is not None and docs.status == "success"


async def test_http_failure_becomes_structured_error(graph):
    executor = DAGExecutor()
    with patch.object(executor, "_post", AsyncMock(side_effect=httpx.ConnectError("refused"))):
        logs, docs = await executor.execute(graph, "q")
    assert logs is not None and logs.status == "failure" and logs.error.startswith("E003")
    assert docs is not None and docs.status == "failure" and docs.error.startswith("E004")
