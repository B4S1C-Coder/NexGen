import uuid
from typing import List, Dict, Any, Optional
from pydantic import BaseModel
from nexgen_shared.schemas import UserQuery
from src.intent import IntentResult

class ExecutionNode(BaseModel):
    step_id: str
    action_type: str
    dependencies: List[str] = []
    payload: Dict[str, Any]

class ExecutionGraph(BaseModel):
    query_id: str
    nodes: List[ExecutionNode]

class DAGPlanner:
    """
    Translates an IntentResult and Topology graph into a directed acyclic graph 
    (DAG) of execution steps for the orchestrator.
    """
    def plan(self, query: UserQuery, intent: IntentResult, topology: dict) -> ExecutionGraph:
        nodes = []
        synthesize_deps = []
        
        # 1. Topology Injection: Expand index_hints
        expanded_hints = set(intent.index_hints)
        if topology:
            for hint in intent.index_hints:
                # Strip wildcards to match topology keys (e.g., 'payments-*' -> 'payments')
                service_name = hint.replace("-*", "")
                if service_name in topology:
                    deps = topology[service_name].get("dependencies", [])
                    for d in deps:
                        expanded_hints.add(f"{d}-*")
        
        final_hints = list(expanded_hints)
        
        # 2. Add FETCH_LOGS node if intended
        if intent.logs_needed:
            log_node_id = f"fetch_logs_{uuid.uuid4().hex[:8]}"
            nodes.append(ExecutionNode(
                step_id=log_node_id,
                action_type="FETCH_LOGS",
                dependencies=[],
                payload={
                    "index_hints": final_hints, 
                    "time_range": intent.time_range
                }
            ))
            synthesize_deps.append(log_node_id)
            
        # 3. Add FETCH_DOCS node if intended
        if intent.docs_needed:
            doc_node_id = f"fetch_docs_{uuid.uuid4().hex[:8]}"
            nodes.append(ExecutionNode(
                step_id=doc_node_id,
                action_type="FETCH_DOCS",
                dependencies=[],
                payload={}
            ))
            synthesize_deps.append(doc_node_id)
            
        # 4. Add final SYNTHESIZE node
        nodes.append(ExecutionNode(
            step_id=f"synthesize_{uuid.uuid4().hex[:8]}",
            action_type="SYNTHESIZE",
            dependencies=synthesize_deps,
            payload={
                "original_query": query.raw_text
            }
        ))
        
        return ExecutionGraph(query_id=query.query_id, nodes=nodes)
