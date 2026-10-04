"""Step 2: turn the routing decision into a small task graph (DAG)."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from nexgen_shared.schemas import UserQuery

from src.intent import IntentResult
from src.topology import Topology


class ExecutionNode(BaseModel):
    """One task: FETCH_LOGS, FETCH_DOCS or SYNTHESIZE."""

    step_id: str
    action_type: str
    dependencies: list[str] = []
    payload: dict[str, Any] = {}


class ExecutionGraph(BaseModel):
    """All tasks for one query. Fetch tasks have no dependencies, so they run in parallel."""

    query_id: str
    nodes: list[ExecutionNode]


class DAGPlanner:
    """Builds the task graph for a query."""

    def plan(self, query: UserQuery, intent: IntentResult, topology: Topology) -> ExecutionGraph:
        """
        Create FETCH_LOGS and/or FETCH_DOCS tasks, plus a SYNTHESIZE task that depends on them.

        For a troubleshooting question the log task asks for every WARN/ERROR event, not the
        question itself (a literal filter like "payments 500s" would hide the culprit's logs).
        Index hints are widened to all services the named service depends on, because the
        root cause is often downstream (payments-* also searches db-primary-*, auth-service-*...).
        For a count/list question the user's own wording is passed through.
        """
        hints = list(intent.index_hints)
        for hint in intent.index_hints:
            for dep in topology.reachable(hint.removesuffix("-*")):
                if f"{dep}-*" not in hints:
                    hints.append(f"{dep}-*")
        log_question = query.raw_text if intent.is_quantitative else "All log events with level WARN or ERROR"

        nodes: list[ExecutionNode] = []
        if intent.logs_needed:
            nodes.append(ExecutionNode(
                step_id="fetch_logs", action_type="FETCH_LOGS",
                payload={"index_hints": hints, "natural_language": log_question},
            ))
        if intent.docs_needed:
            nodes.append(ExecutionNode(step_id="fetch_docs", action_type="FETCH_DOCS"))
        nodes.append(ExecutionNode(
            step_id="synthesize", action_type="SYNTHESIZE",
            dependencies=[n.step_id for n in nodes], payload={"original_query": query.raw_text},
        ))
        return ExecutionGraph(query_id=query.query_id, nodes=nodes)
