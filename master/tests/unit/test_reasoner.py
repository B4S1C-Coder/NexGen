import pytest
import json
from unittest.mock import AsyncMock

from src.reasoner import ReasonerAgent, AcceptedHypothesis
from src.context import RCASynthesisInput

@pytest.fixture
def dummy_context():
    return RCASynthesisInput(
        query_id="q1",
        original_query="Test failure query",
        log_evidence=[],
        knowledge_context=[],
        reasoning_trace=[]
    )

@pytest.mark.asyncio
async def test_reasoner_mock_fallback(dummy_context):
    reasoner = ReasonerAgent(openai_client=None)
    results = await reasoner.reason(dummy_context)
    
    assert len(results) == 1
    assert "Mock reasoned branch." in results[0].description

@pytest.mark.asyncio
async def test_tot_bfs_pruning_contradictions(dummy_context):
    mock_client = AsyncMock()
    
    # We provide an LLM json response that mimics a set of hypotheses generated at depth 1.
    # The BFS logic ensures contradictions >= 2 are dynamically pruned out.
    payload = {
        "hypotheses": [
            {
                "description": "Flawed Network hypothesis",
                "contradictions": 3,
                "supporting_evidence_count": 0,
                "is_accepted": False
            },
            {
                "description": "Clear DB hypothesis",
                "contradictions": 0,
                "supporting_evidence_count": 2,
                "is_accepted": True
            }
        ]
    }
    
    mock_message = AsyncMock()
    mock_message.content = json.dumps(payload)
    mock_choice = AsyncMock()
    mock_choice.message = mock_message
    mock_client.chat.completions.create.return_value = AsyncMock(choices=[mock_choice])
    
    reasoner = ReasonerAgent(openai_client=mock_client)
    accepted = await reasoner.reason(dummy_context)
    
    # Assert correct pruning mechanism
    assert len(accepted) == 1
    assert accepted[0].description == "Clear DB hypothesis"
    assert accepted[0].contradictions == 0
