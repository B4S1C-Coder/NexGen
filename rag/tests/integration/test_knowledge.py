"""Integration tests for the POST /knowledge endpoint.

Tests cover:
- Basic pipeline returning docs (no conflicts).
- Conflict detection with contradictory docs producing conflict_detected=True.
- total_tokens_after_compression is populated.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi.testclient import TestClient

from src.compactor import LLMLingua2Compactor
from src.conflict import ConflictDetector, ConflictPair
from src.debate import MultiAgentDebate
from src.id_preservation import TechnicalIDPreservationLayer
from src.main import app
from src.preprocessor import ChunkMetadata, RankedChunk


def _make_chunk(
    chunk_id: str,
    content: str,
    source_type: str = "runbook",
    authority_tier: str = "A",
    score: float = 0.9,
) -> RankedChunk:
    """Create a RankedChunk with sensible defaults for testing."""
    meta = ChunkMetadata(
        chunk_id=chunk_id,
        doc_id=f"doc-{chunk_id}",
        source_type=source_type,
        source_uri=f"uri://{chunk_id}",
        authority_tier=authority_tier,
        created_at=datetime.now(UTC),
        resolution_status="resolved",
        is_accepted_answer=True,
        recency_score=1.0,
    )
    return RankedChunk(
        chunk_id=chunk_id,
        content=content,
        metadata=meta,
        score=score,
    )


KNOWLEDGE_REQUEST = {
    "query_id": "q-123",
    "semantic_query": "payments database connection refused",
    "source_filters": ["runbooks"],
    "time_window": {"not_after": datetime.now(UTC).isoformat()},
    "max_chunks": 12,
    "compression_budget_tokens": 2000,
}


@patch("src.main.LLMLingua2Compactor")
@patch("src.main.ConflictDetector")
@patch("src.main.CrossEncoderReranker")
@patch("src.main.SparseRetriever")
@patch("src.main.DenseRetriever")
def test_knowledge_pipeline_returns_docs(mock_dense, mock_sparse, mock_reranker, mock_conflict, mock_compactor):
    """Basic pipeline returns docs with no conflicts detected."""

    chunk = _make_chunk(
        "test-chunk",
        "payments database connection refused",
    )

    with TestClient(app) as client:
        # Replace state with mocks after lifespan has run
        client.app.state.dense_retriever = AsyncMock()
        client.app.state.dense_retriever.retrieve.return_value = [chunk]
        client.app.state.sparse_retriever = AsyncMock()
        client.app.state.sparse_retriever.retrieve.return_value = []
        client.app.state.reranker = MagicMock()
        client.app.state.reranker.rerank.side_effect = lambda q, chunks: chunks
        # Use a no-op conflict detector (returns no conflicts)
        client.app.state.conflict_detector = MagicMock()
        client.app.state.conflict_detector.detect_conflicts.return_value = []
        # Use extractive-fallback compactor (no model)
        from src.compactor import LLMLingua2Compactor as RealCompactor
        client.app.state.compactor = RealCompactor(
            model_name="__nonexistent_for_tests__"
        )
        client.app.state.id_preservation = TechnicalIDPreservationLayer()

        response = client.post("/knowledge", json=KNOWLEDGE_REQUEST)

    assert response.status_code == 200
    data = response.json()
    assert data["query_id"] == "q-123"
    assert data["status"] == "success"
    assert len(data["chunks"]) == 1
    assert data["chunks"][0]["chunk_id"] == "test-chunk"
    assert data["chunks"][0]["content"] == "payments database connection refused"
    assert data["conflict_detected"] is False
    assert data["total_tokens_after_compression"] > 0


@patch("src.main.LLMLingua2Compactor")
@patch("src.main.ConflictDetector")
@patch("src.main.CrossEncoderReranker")
@patch("src.main.SparseRetriever")
@patch("src.main.DenseRetriever")
def test_knowledge_pipeline_detects_conflicts(mock_dense, mock_sparse, mock_reranker, mock_conflict, mock_compactor):
    """Pipeline with contradictory chunks returns conflict_detected=True.

    The conflict detector is mocked to return a ConflictPair. The debate
    module is mocked to return the winner chunk. The pipeline should
    remove the losing chunk and set conflict_detected=True.
    """

    chunk_a = _make_chunk(
        "chunk-a",
        "The database should be restarted to fix the issue.",
        score=0.95,
    )
    chunk_b = _make_chunk(
        "chunk-b",
        "Do NOT restart the database as it causes data loss.",
        authority_tier="B",
        score=0.85,
    )

    conflict_pair = ConflictPair(
        chunk_i=chunk_a,
        chunk_j=chunk_b,
        confidence=0.92,
    )

    with TestClient(app) as client:
        client.app.state.dense_retriever = AsyncMock()
        client.app.state.dense_retriever.retrieve.return_value = [chunk_a, chunk_b]
        client.app.state.sparse_retriever = AsyncMock()
        client.app.state.sparse_retriever.retrieve.return_value = []
        client.app.state.reranker = MagicMock()
        client.app.state.reranker.rerank.side_effect = lambda q, chunks: chunks

        # Conflict detector returns one conflict pair
        client.app.state.conflict_detector = MagicMock()
        client.app.state.conflict_detector.detect_conflicts.return_value = [
            conflict_pair
        ]

        # Debate module returns chunk_a as winner
        mock_debate = AsyncMock()
        mock_debate.resolve.return_value = chunk_a
        client.app.state.debate = mock_debate

        from src.compactor import LLMLingua2Compactor as RealCompactor
        client.app.state.compactor = RealCompactor(
            model_name="__nonexistent_for_tests__"
        )
        client.app.state.id_preservation = TechnicalIDPreservationLayer()

        response = client.post("/knowledge", json=KNOWLEDGE_REQUEST)

    assert response.status_code == 200
    data = response.json()
    assert data["conflict_detected"] is True
    # Only the winning chunk should remain
    assert len(data["chunks"]) == 1
    assert data["chunks"][0]["chunk_id"] == "chunk-a"
    assert data["total_tokens_after_compression"] > 0


@patch("src.main.LLMLingua2Compactor")
@patch("src.main.ConflictDetector")
@patch("src.main.CrossEncoderReranker")
@patch("src.main.SparseRetriever")
@patch("src.main.DenseRetriever")
def test_knowledge_pipeline_with_merged_conflict(mock_dense, mock_sparse, mock_reranker, mock_conflict, mock_compactor):
    """When debate produces a MERGE result, both originals are replaced."""

    chunk_a = _make_chunk("chunk-a", "Approach A for fixing the issue.")
    chunk_b = _make_chunk("chunk-b", "Approach B for fixing the issue.")

    conflict_pair = ConflictPair(
        chunk_i=chunk_a, chunk_j=chunk_b, confidence=0.88
    )
    merged_chunk = _make_chunk(
        "chunk-a-merged",
        "Both approaches have merit. Use A first, then B if A fails.",
    )

    with TestClient(app) as client:
        client.app.state.dense_retriever = AsyncMock()
        client.app.state.dense_retriever.retrieve.return_value = [chunk_a, chunk_b]
        client.app.state.sparse_retriever = AsyncMock()
        client.app.state.sparse_retriever.retrieve.return_value = []
        client.app.state.reranker = MagicMock()
        client.app.state.reranker.rerank.side_effect = lambda q, chunks: chunks

        client.app.state.conflict_detector = MagicMock()
        client.app.state.conflict_detector.detect_conflicts.return_value = [
            conflict_pair
        ]

        mock_debate = AsyncMock()
        mock_debate.resolve.return_value = merged_chunk
        client.app.state.debate = mock_debate

        from src.compactor import LLMLingua2Compactor as RealCompactor
        client.app.state.compactor = RealCompactor(
            model_name="__nonexistent_for_tests__"
        )
        client.app.state.id_preservation = TechnicalIDPreservationLayer()

        response = client.post("/knowledge", json=KNOWLEDGE_REQUEST)

    assert response.status_code == 200
    data = response.json()
    assert data["conflict_detected"] is True
    # Both originals replaced with merged
    assert len(data["chunks"]) == 1
    assert data["chunks"][0]["chunk_id"] == "chunk-a-merged"
    assert data["total_tokens_after_compression"] > 0


@patch("src.main.LLMLingua2Compactor")
@patch("src.main.ConflictDetector")
@patch("src.main.CrossEncoderReranker")
@patch("src.main.SparseRetriever")
@patch("src.main.DenseRetriever")
def test_knowledge_pipeline_compression_populates_token_count(mock_dense, mock_sparse, mock_reranker, mock_conflict, mock_compactor):
    """Verify total_tokens_after_compression reflects actual compression."""

    # Create chunks with enough content to test compression (punctuated for sentence splitting)
    long_content = " ".join(["This is a short sentence."] * 10)
    chunk = _make_chunk("long-chunk", long_content)

    with TestClient(app) as client:
        client.app.state.dense_retriever = AsyncMock()
        client.app.state.dense_retriever.retrieve.return_value = [chunk]
        client.app.state.sparse_retriever = AsyncMock()
        client.app.state.sparse_retriever.retrieve.return_value = []
        client.app.state.reranker = MagicMock()
        client.app.state.reranker.rerank.side_effect = lambda q, chunks: chunks
        client.app.state.conflict_detector = MagicMock()
        client.app.state.conflict_detector.detect_conflicts.return_value = []
        from src.compactor import LLMLingua2Compactor as RealCompactor
        client.app.state.compactor = RealCompactor(
            model_name="__nonexistent_for_tests__"
        )
        client.app.state.id_preservation = TechnicalIDPreservationLayer()

        # Use a small budget to force compression
        request = {**KNOWLEDGE_REQUEST, "compression_budget_tokens": 20}
        response = client.post("/knowledge", json=request)

    assert response.status_code == 200
    data = response.json()
    # Token count should be populated and within budget + 5%
    assert data["total_tokens_after_compression"] > 0
    assert data["total_tokens_after_compression"] <= 22  # 20 + ~5%
