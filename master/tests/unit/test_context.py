import pytest
from datetime import datetime, timezone

from src.context import ContextAssembler
from src.intent import IntentResult
from nexgen_shared.schemas import LogRetrievalResult, LogHit

@pytest.fixture
def assembler():
    return ContextAssembler(max_tokens=200)

def test_is_context_sufficient_false(assembler):
    intent = IntentResult(
        logs_needed=True, 
        docs_needed=False, 
        is_quantitative=False, 
        is_qualitative=False
    )
    
    # Missing hits breaks sufficiency when logs are mandatory
    logs = LogRetrievalResult(
        query_id="q1", status="success", kql_generated="", syntax_valid=True,
        refinement_attempts=0, hits=[], hit_count=0, error=None
    )
    
    assert assembler.is_context_sufficient(intent, logs) is False

def test_pruning_and_token_budget(assembler):
    intent = IntentResult(
        logs_needed=True, 
        docs_needed=False, 
        is_quantitative=False, 
        is_qualitative=False
    )
    
    hits = []
    for i in range(200):
        hits.append(LogHit(
            timestamp=datetime.now(timezone.utc),
            service="test",
            level="ERROR",
            message=f"Long padding message index {i} repeated " * 5,
            trace_id=f"t{i}"
        ))
        
    logs = LogRetrievalResult(
        query_id="q1", status="success", kql_generated="", syntax_valid=True,
        refinement_attempts=0, hits=hits, hit_count=200, error=None
    )
    
    output = assembler.assemble("test query", "q1", logs, None, intent)
    
    # We explicitly capped tokenizer at 200 raw tokens in fixture. 200 log messages will breach.
    assert len(output.log_evidence) < 200
    assert len(output.log_evidence) > 0 
