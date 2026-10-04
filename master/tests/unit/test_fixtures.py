from datetime import datetime, timezone

from nexgen_shared.schemas import (
    KnowledgeRequest,
    KnowledgeTimeWindow,
    LogRetrievalRequest,
    SchemaContextPayload,
    TimeRange,
)

from src.fixtures import FixtureBackend


def test_exact_question_finds_its_scenario():
    backend = FixtureBackend()
    assert backend.find("Why is payments returning HTTP 500 errors?")["id"] == "db-01"


def test_other_question_finds_closest_scenario():
    assert FixtureBackend().find("gateway rejecting requests with 429")["id"] == "rate-02"


def test_free_form_question_gets_best_matching_docs():
    request = KnowledgeRequest(
        query_id="q1", semantic_query="How do I raise the token quota for a client?", source_filters=[],
        time_window=KnowledgeTimeWindow(not_after=datetime.now(timezone.utc)),
        max_chunks=10, compression_budget_tokens=2000,
    )
    assert FixtureBackend().knowledge(request).chunks[0].chunk_id == "rb-rate-limit"


def test_logs_and_knowledge_use_shared_schemas():
    backend = FixtureBackend()
    question = "Why is payments returning HTTP 500 errors?"
    logs = backend.logs(LogRetrievalRequest(
        query_id="q1", natural_language=question, index_hints=[],
        time_range=TimeRange.model_validate({"from": "now-30m", "to": "now"}),
        max_results=100, schema_context=SchemaContextPayload(known_fields=[]),
    ), question)
    docs = backend.knowledge(KnowledgeRequest(
        query_id="q1", semantic_query=question, source_filters=[],
        time_window=KnowledgeTimeWindow(not_after=datetime.now(timezone.utc)),
        max_chunks=10, compression_budget_tokens=2000,
    ))
    assert logs.status == "success" and logs.hit_count == len(logs.hits) > 0
    assert logs.hits[0].timestamp is not None and logs.hits[0].timestamp.tzinfo is not None
    assert docs.status == "success" and docs.chunks[0].source_uri.startswith("confluence://")
