import asyncio
import os
import httpx
from typing import Dict, Any

from src.planner import ExecutionGraph, ExecutionNode
from nexgen_shared.schemas import (
    LogRetrievalRequest, KnowledgeRequest, 
    LogRetrievalResult, KnowledgeResult, 
    TimeRange, KnowledgeTimeWindow, SchemaContextPayload
)

# Load optional mock packages safely
try:
    from src.mock_query.pipeline import MockQueryPipeline
    from src.mock_rag.pipeline import MockRAGPipeline
except ImportError:
    MockQueryPipeline = None
    MockRAGPipeline = None

class DAGExecutor:
    """
    Engine that traverses an ExecutionGraph and parallelizes external fetches using asyncio 
    or internal mock networks based on local environment configurations.
    """
    def __init__(self, query_service_url: str = "http://localhost:8001", rag_service_url: str = "http://localhost:8002"):
        self.query_service_url = query_service_url
        self.rag_service_url = rag_service_url
        self.mock_mode = os.getenv("MOCK_SERVICES", "false").lower() == "true"
        self.timeout = 10.0
        
        if self.mock_mode:
            self.mock_query = MockQueryPipeline() if MockQueryPipeline else None
            self.mock_rag = MockRAGPipeline() if MockRAGPipeline else None

    async def execute(self, graph: ExecutionGraph, query_id: str, natural_language: str) -> Dict[str, Any]:
        results = {}
        fetch_tasks = []
        
        # 1. Identify leaf nodes capable of running parallelly (zero dependencies)
        for node in graph.nodes:
            if not node.dependencies and node.action_type in ["FETCH_LOGS", "FETCH_DOCS"]:
                fetch_tasks.append(self._execute_node(node, query_id, natural_language))
                
        # 2. Gather IO requests asynchronously natively mapped correctly back to the node graph id
        completed = await asyncio.gather(*fetch_tasks, return_exceptions=True)
        
        for res in completed:
            if isinstance(res, dict):
                results.update(res)
            elif isinstance(res, Exception):
                results["executor_error"] = str(res)
                
        return results

    async def _execute_node(self, node: ExecutionNode, query_id: str, natural_language: str) -> Dict[str, Any]:
        if node.action_type == "FETCH_LOGS":
            req = LogRetrievalRequest(
                query_id=query_id,
                natural_language=natural_language,
                index_hints=node.payload.get("index_hints", []),
                time_range=TimeRange(**node.payload.get("time_range", {"from": "now-30m", "to": "now"})) if node.payload.get("time_range") else TimeRange(**{"from": "now-30m", "to": "now"}),
                max_results=node.payload.get("max_results", 50),
                schema_context=SchemaContextPayload(known_fields=[], value_samples={})
            )
            
            if self.mock_mode and self.mock_query:
                # Direct Python pass-through avoiding HTTP
                result = await self.mock_query.retrieve(req)
                return {node.step_id: result}
            else:
                return await self._http_call(f"{self.query_service_url}/retrieve", req.model_dump(), node.step_id, LogRetrievalResult)

        elif node.action_type == "FETCH_DOCS":
            req = KnowledgeRequest(
                query_id=query_id,
                semantic_query=natural_language,
                source_filters=[],
                time_window=KnowledgeTimeWindow(not_after="2026-04-14T00:00:00Z"),
                max_chunks=10,
                compression_budget_tokens=2000
            )

            if self.mock_mode and self.mock_rag:
                result = await self.mock_rag.retrieve_knowledge(req)
                return {node.step_id: result}
            else:
                return await self._http_call(f"{self.rag_service_url}/knowledge", req.model_dump(), node.step_id, KnowledgeResult)

        return {}

    async def _http_call(self, url: str, payload: dict, step_id: str, model_cls) -> Dict[str, Any]:
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            try:
                resp = await client.post(url, json=payload)
                resp.raise_for_status()
                result = model_cls.model_validate_json(resp.text)
                return {step_id: result}
            except httpx.HTTPStatusError as e:
                # Fallbacks gracefully into Pydantic models containing explicitly defined schemas without crashing execution
                return {step_id: {"error": f"HTTP {e.response.status_code}"}}
            except httpx.RequestError as e:
                return {step_id: {"error": f"Request failed: {str(e)}"}}
