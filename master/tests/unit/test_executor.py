import pytest
import os
import httpx
from unittest.mock import AsyncMock, patch

from src.planner import ExecutionGraph, ExecutionNode
from src.executor import DAGExecutor
from nexgen_shared.schemas import LogRetrievalResult, KnowledgeResult

@pytest.fixture
def graph():
    node1 = ExecutionNode(step_id="n_log", action_type="FETCH_LOGS", payload={})
    node2 = ExecutionNode(step_id="n_doc", action_type="FETCH_DOCS", payload={})
    return ExecutionGraph(query_id="q123", nodes=[node1, node2])

@pytest.mark.asyncio
async def test_mock_mode_execution(graph):
    # Setup
    os.environ["MOCK_SERVICES"] = "true"
    executor = DAGExecutor()
    
    # Execute
    results = await executor.execute(graph, "q123", "test mock")
    
    # Cleanup
    del os.environ["MOCK_SERVICES"]
    
    # Assertions: the actual mocks created earlier return success immediately
    assert "n_log" in results
    assert "n_doc" in results
    assert isinstance(results["n_log"], LogRetrievalResult)
    assert isinstance(results["n_doc"], KnowledgeResult)
    assert results["n_log"].status == "success"
    assert results["n_doc"].status == "success"

@pytest.mark.asyncio
async def test_http_call_success(graph):
    # Setup non-mock execution
    os.environ["MOCK_SERVICES"] = "false"
    executor = DAGExecutor()
    executor.mock_mode = False
    
    # Prepare dummy response
    async def mock_post_call(*args, **kwargs):
        mock_resp = AsyncMock()
        mock_resp.raise_for_status = lambda: None
        if "retrieve" in args[0]:
            mock_resp.text = '{"query_id":"q123", "status":"success", "kql_generated":"test", "syntax_valid":true, "refinement_attempts":0, "hits":[], "hit_count":0, "error":null}'
        else:
            mock_resp.text = '{"query_id":"q123", "status":"success", "chunks":[], "total_tokens_after_compression":0, "conflict_detected":false, "error":null}'
        return mock_resp

    with patch("httpx.AsyncClient.post", side_effect=mock_post_call):
        results = await executor.execute(graph, "q123", "test http")
        
        assert "n_log" in results
        assert "n_doc" in results
        assert results["n_log"].query_id == "q123"
        assert results["n_doc"].query_id == "q123"

@pytest.mark.asyncio
async def test_http_call_timeout(graph):
    executor = DAGExecutor()
    executor.mock_mode = False
    
    with patch("httpx.AsyncClient.post", side_effect=httpx.RequestError("Timeout hit")):
        results = await executor.execute(graph, "q123", "timeout test")
        
        # Ensures failure bubbles gracefully to JSON response without crashing the DAG loop
        assert "error" in results["n_log"]
        assert "Timeout hit" in results["n_log"]["error"]
