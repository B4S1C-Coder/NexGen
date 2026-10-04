from nexgen_shared.schemas import KnowledgeResult, LogRetrievalResult

from src.context import ContextAssembler
from src.intent import IntentResult


def logs_result(hits):
    return LogRetrievalResult(query_id="q1", status="success", kql_generated="", syntax_valid=True,
                              refinement_attempts=0, hits=hits, hit_count=len(hits), error=None)


def docs_result(chunks):
    return KnowledgeResult(query_id="q1", status="success", chunks=chunks,
                           total_tokens_after_compression=0, conflict_detected=False, error=None)


def test_logs_required_but_empty_is_insufficient():
    intent = IntentResult(logs_needed=True, docs_needed=True)
    assert not ContextAssembler().is_context_sufficient(intent, logs_result([]), None)


def test_docs_only_question_needs_docs(chunk):
    intent = IntentResult(logs_needed=False, docs_needed=True)
    assembler = ContextAssembler()
    assert not assembler.is_context_sufficient(intent, None, docs_result([]))
    assert assembler.is_context_sufficient(intent, None, docs_result([chunk("x")]))


def test_duplicates_removed_and_sorted_by_time(hit):
    hits = [
        hit("09:57:02", "payments", "ERROR", "Connection refused"),
        hit("09:57:01", "payments", "ERROR", "Connection refused"),
        hit("09:56:00", "db-primary", "ERROR", "shutting down"),
    ]
    context = ContextAssembler().assemble("q1", "why?", logs_result(hits), None)
    assert [h.message for h in context.log_evidence] == ["shutting down", "Connection refused"]
    assert context.log_evidence[1].trace_id == "payments-09:57:01"


def test_token_budget_is_respected(hit):
    hits = [hit(f"10:{i // 60:02d}:{i % 60:02d}", "svc", "ERROR", f"unique message number {i}") for i in range(200)]
    assembler = ContextAssembler(max_tokens=200)
    context = assembler.assemble("q1", "why?", logs_result(hits), None)
    assert 0 < len(context.log_evidence) < 200
    assert assembler.count_tokens(context.log_evidence) <= 200
