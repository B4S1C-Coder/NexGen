import pytest
from datetime import datetime
from nexgen_shared.schemas import UserQuery
from src.intent import IntentResult
from src.planner import DAGPlanner

@pytest.fixture
def query():
    return UserQuery(
        query_id="q123", 
        raw_text="test query", 
        session_id="s1", 
        timestamp_utc=datetime.now()
    )

def test_dag_both_logs_and_docs(query):
    intent = IntentResult(
        logs_needed=True, 
        docs_needed=True, 
        is_quantitative=False, 
        is_qualitative=True
    )
    planner = DAGPlanner()
    graph = planner.plan(query, intent, topology={})
    
    assert len(graph.nodes) == 3
    actions = [n.action_type for n in graph.nodes]
    assert "FETCH_LOGS" in actions
    assert "FETCH_DOCS" in actions
    assert "SYNTHESIZE" in actions
    
    # Verify SYNTHESIZE node depends on the other two
    synth_node = next(n for n in graph.nodes if n.action_type == "SYNTHESIZE")
    assert len(synth_node.dependencies) == 2

def test_dag_logs_only(query):
    intent = IntentResult(
        logs_needed=True, 
        docs_needed=False, 
        is_quantitative=True, 
        is_qualitative=False
    )
    planner = DAGPlanner()
    graph = planner.plan(query, intent, topology={})
    
    assert len(graph.nodes) == 2
    actions = [n.action_type for n in graph.nodes]
    assert "FETCH_LOGS" in actions
    assert "FETCH_DOCS" not in actions
    
    # SYNTHESIZE node should only depend on FETCH_LOGS
    synth_node = next(n for n in graph.nodes if n.action_type == "SYNTHESIZE")
    assert len(synth_node.dependencies) == 1

def test_dag_topology_expansion(query):
    intent = IntentResult(
        logs_needed=True, 
        docs_needed=False, 
        is_quantitative=True, 
        is_qualitative=False, 
        index_hints=["payments-*"]
    )
    # Mock topology definition where 'payments' depends on 'db-primary'
    topology = {
        "payments": {"dependencies": ["db-primary"]}
    }
    planner = DAGPlanner()
    graph = planner.plan(query, intent, topology=topology)
    
    log_node = next(n for n in graph.nodes if n.action_type == "FETCH_LOGS")
    assert "payments-*" in log_node.payload["index_hints"]
    assert "db-primary-*" in log_node.payload["index_hints"]
