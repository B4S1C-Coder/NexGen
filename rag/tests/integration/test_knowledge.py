from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from src.main import app
from src.preprocessor import ChunkMetadata, RankedChunk


@patch("src.main.CrossEncoderReranker")
@patch("src.main.SparseRetriever")
@patch("src.main.DenseRetriever")
def test_knowledge_pipeline_returns_docs(mock_dense, mock_sparse, mock_reranker):
    # Setup mock retrievers
    mock_dense_instance = AsyncMock()
    mock_sparse_instance = AsyncMock()
    mock_dense.return_value = mock_dense_instance
    mock_sparse.return_value = mock_sparse_instance

    meta = ChunkMetadata(
        chunk_id="test-chunk",
        doc_id="test-doc",
        source_type="runbooks",
        source_uri="uri",
        authority_tier="A",
        created_at=datetime.now(UTC),
        resolution_status="resolved",
        is_accepted_answer=True,
        recency_score=1.0,
    )
    mock_dense_instance.retrieve.return_value = [
        RankedChunk(chunk_id="test-chunk", content="payments database connection refused", metadata=meta, score=0.9)
    ]
    mock_sparse_instance.retrieve.return_value = []

    # Setup mock reranker
    mock_reranker_instance = mock_reranker.return_value
    mock_reranker_instance.rerank.side_effect = lambda query, chunks: chunks

    with TestClient(app) as client:
        # We need to replace the state instances with our mocks
        client.app.state.dense_retriever = mock_dense_instance
        client.app.state.sparse_retriever = mock_sparse_instance
        client.app.state.reranker = mock_reranker_instance

        response = client.post(
            "/knowledge",
            json={
                "query_id": "q-123",
                "semantic_query": "payments database connection refused",
                "source_filters": ["runbooks"],
                "time_window": {"not_after": datetime.now(UTC).isoformat()},
                "max_chunks": 12,
                "compression_budget_tokens": 2000,
            },
        )

    assert response.status_code == 200
    data = response.json()
    assert data["query_id"] == "q-123"
    assert data["status"] == "success"
    assert len(data["chunks"]) == 1
    assert data["chunks"][0]["chunk_id"] == "test-chunk"
    assert data["chunks"][0]["content"] == "payments database connection refused"
    assert data["conflict_detected"] is False
