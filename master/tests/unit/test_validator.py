import pytest
import json
from unittest.mock import AsyncMock

from src.validator import ValidatorAgent
from src.reasoner import AcceptedHypothesis
from src.context import RCASynthesisInput
from nexgen_shared.errors import E008TopologyVerificationRejected

@pytest.fixture
def dummy_context():
    return RCASynthesisInput(
        query_id="q1", 
        original_query="Test timeout payload", 
        log_evidence=[], 
        knowledge_context=[], 
        reasoning_trace=[]
    )

@pytest.mark.asyncio
async def test_validator_knowledge_grounding_reject():
    agent = ValidatorAgent(openai_client=None)
    # Zero evidence support should outright reject immediately without LLM logic
    hyp = AcceptedHypothesis(
        id="h1", 
        description="Hypothetical logic.", 
        contradictions=0, 
        supporting_evidence_count=0, 
        is_accepted=True
    )
    is_valid = await agent.validate(hyp, None)
    assert not is_valid

@pytest.mark.asyncio
async def test_validator_topology_edge_failure(dummy_context, tmp_path):
    # Setup agent with mock graph topology isolating 'serviceA' natively
    agent = ValidatorAgent(openai_client=AsyncMock())
    agent.topology = {"serviceA": {"dependencies": ["serviceB"]}}
    
    # LLM extracts the assumption that Service A connected to isolated Service C 
    payload = {
        "is_valid": True,
        "reason": "Logically sound.",
        "extracted_edges": [["serviceA", "serviceC"]]
    }
    
    mock_message = AsyncMock()
    mock_message.content = json.dumps(payload)
    agent.llm.chat.completions.create.return_value = AsyncMock(choices=[AsyncMock(message=mock_message)])
    
    hyp = AcceptedHypothesis(
        id="h2", 
        description="A logic assumes A -> C", 
        contradictions=0, 
        supporting_evidence_count=1, 
        is_accepted=True
    )
    
    # Assert pipeline stops cleanly and securely with deterministic Pydantic Error Type
    with pytest.raises(E008TopologyVerificationRejected) as exc_info:
        await agent.validate(hyp, dummy_context)
        
    assert "serviceA" in str(exc_info.value)
