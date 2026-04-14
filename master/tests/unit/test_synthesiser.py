import pytest
import json
from unittest.mock import AsyncMock
from datetime import datetime, timezone
from src.synthesiser import RCASynthesiser
from nexgen_shared.schemas import UserQuery

@pytest.fixture
def dummy_query():
    return UserQuery(
        query_id="q_123",
        raw_text="The internal db is timing out",
        session_id="s_1",
        timestamp_utc=datetime.now(timezone.utc).isoformat()
    )

@pytest.mark.asyncio
async def test_mock_synthesiser(dummy_query):
    synth = RCASynthesiser(openai_client=None)
    report = await synth.synthesize(dummy_query, logs=None, docs=None, session_history=[])
    
    assert report.query_id == "q_123"
    assert report.confidence == 1.0
    assert "Mock Generated" in report.root_cause_summary

@pytest.mark.asyncio
async def test_llm_synthesiser_success_parsing(dummy_query):
    mock_client = AsyncMock()
    valid_payload = {
        "query_id": "q_123",
        "root_cause_summary": "Database connectivity loss",
        "confidence": 0.85,
        "evidence": [],
        "recommended_actions": ["Restart proxy"],
        "reasoning_trace_summary": "Db logs show TCP timeouts",
        "mttr_estimate_minutes": 10,
        "generated_at": datetime.now(timezone.utc).isoformat()
    }
    
    # Mock LLM API Structure
    mock_message = AsyncMock()
    mock_message.content = json.dumps(valid_payload)
    mock_choice = AsyncMock()
    mock_choice.message = mock_message
    mock_client.chat.completions.create.return_value = AsyncMock(choices=[mock_choice])
    
    synth = RCASynthesiser(openai_client=mock_client)
    report = await synth.synthesize(dummy_query, logs=None, docs=None, session_history=[])
    
    assert report.root_cause_summary == "Database connectivity loss"
    assert report.confidence == 0.85

@pytest.mark.asyncio
async def test_llm_confidence_boundaries_capping(dummy_query):
    mock_client = AsyncMock()
    payload = {
        "query_id": "q_123",
        "root_cause_summary": "Testing Boundaries",
        "confidence": 1.5, # Out of bounds
        "evidence": [],
        "recommended_actions": [],
        "reasoning_trace_summary": "Trace",
        "mttr_estimate_minutes": 5,
        "generated_at": datetime.now(timezone.utc).isoformat()
    }
    
    mock_message = AsyncMock()
    mock_message.content = json.dumps(payload)
    mock_client.chat.completions.create.return_value = AsyncMock(choices=[AsyncMock(message=mock_message)])
    
    synth = RCASynthesiser(openai_client=mock_client)
    report = await synth.synthesize(dummy_query, logs=None, docs=None, session_history=[])
    
    # Assert confidence capped to 1.0
    assert report.confidence == 1.0
