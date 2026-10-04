from unittest.mock import AsyncMock, patch

import pytest

from src.intent import IntentClassifier

SERVICES = ["auth-service", "db-primary", "gateway", "payments"]


@pytest.fixture
def classifier() -> IntentClassifier:
    return IntentClassifier(SERVICES)


async def test_count_question_needs_only_logs(classifier):
    result = await classifier.classify("count HTTP 500s from payments")
    assert (result.logs_needed, result.docs_needed, result.is_quantitative) == (True, False, True)


async def test_how_to_question_needs_only_docs(classifier):
    result = await classifier.classify("what is the best practice for node sizing?")
    assert (result.logs_needed, result.docs_needed) == (False, True)


async def test_why_question_needs_both(classifier):
    result = await classifier.classify("Why did payments fail?")
    assert (result.logs_needed, result.docs_needed) == (True, True)


async def test_services_found_in_order_with_short_names(classifier):
    assert classifier.find_services("why can't auth reach the db from the payment page") == [
        "auth-service", "db-primary", "payments",
    ]


async def test_unclear_question_without_llm_fetches_both(classifier):
    result = await classifier.classify("hello there")
    assert (result.logs_needed, result.docs_needed, result.used_llm) == (True, True, False)


async def test_unclear_question_asks_llm():
    classifier = IntentClassifier(SERVICES, llm=AsyncMock(), model="m")
    reply = {"logs_needed": False, "docs_needed": True, "is_quantitative": False}
    with patch("src.intent.ask_json", AsyncMock(return_value=reply)):
        result = await classifier.classify("hello there")
    assert (result.logs_needed, result.docs_needed, result.used_llm) == (False, True, True)
