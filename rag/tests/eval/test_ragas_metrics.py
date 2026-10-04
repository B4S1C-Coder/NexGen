"""Unit tests for the evaluation metric functions (no services or LLM needed)."""

from ragas_metrics import claim_share, context_precision, context_recall, doc_of


def test_doc_of_strips_chunk_suffix():
    assert doc_of("runbook-payments-db-failover-chunk-2") == "runbook-payments-db-failover"


def test_context_precision_rewards_relevant_chunks_ranked_first():
    assert context_precision(["a", "b", "x"], {"a", "b"}) == 1.0
    # relevant doc at rank 2 only: precision@2 = 0.5
    assert context_precision(["x", "a"], {"a"}) == 0.5
    assert context_precision(["x", "y"], {"a"}) == 0.0


def test_context_recall_counts_relevant_docs_found():
    assert context_recall(["a", "x", "a"], {"a", "b"}) == 0.5
    assert context_recall(["a", "b"], {"a", "b"}) == 1.0


def test_claim_share():
    assert claim_share([{"supported": True}, {"supported": False}], "supported") == 0.5
    assert claim_share([], "supported") is None
