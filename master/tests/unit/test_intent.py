import pytest
from src.intent import IntentClassifier

@pytest.fixture
def classifier():
    # Instantiate without LLM or Qdrant so we strictly test the fast path
    return IntentClassifier(openai_client=None, qdrant_client=None)

@pytest.mark.asyncio
async def test_fast_path_quantitative(classifier):
    result = await classifier.classify("count HTTP 500s from payments")
    
    assert result.is_quantitative is True
    assert result.logs_needed is True
    assert result.docs_needed is False
    assert result.is_qualitative is False

@pytest.mark.asyncio
async def test_fast_path_qualitative(classifier):
    result = await classifier.classify("what is the best practice for node sizing?")
    
    assert result.is_qualitative is True
    assert result.docs_needed is True
    assert result.logs_needed is False
    assert result.is_quantitative is False
