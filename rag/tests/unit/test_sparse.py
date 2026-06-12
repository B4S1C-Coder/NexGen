import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock

from qdrant_client import AsyncQdrantClient
from qdrant_client.http.models import NamedSparseVector, ScoredPoint

from nexgen_shared.schemas import KnowledgeRequest, KnowledgeTimeWindow
from src.qdrant_setup import SPARSE_VECTOR_NAME
from src.settings import Settings
from src.sparse import SparseRetriever


def test_retrieve_sparse():
    # Setup test data
    settings = Settings(
        sparse_collection="test_sparse_collection"
    )

    # Use a query with a specific technical identifier
    query_text = "how to fix ERR_CONN_RESET?"
    
    request = KnowledgeRequest(
        query_id="q-2",
        semantic_query=query_text,
        source_filters=[],
        time_window=KnowledgeTimeWindow(not_after=datetime.now(timezone.utc)),
        max_chunks=2,
        compression_budget_tokens=1000
    )

    # Mock Qdrant client
    mock_qdrant_client = AsyncMock()
    mock_point = ScoredPoint(
        id="point-2",
        version=1,
        score=12.4,
        payload={
            "chunk_id": "test-chunk-err",
            "doc_id": "doc-err",
            "content": "To fix ERR_CONN_RESET, restart the proxy.",
            "source_type": "runbook",
            "created_at": "2026-05-01T10:00:00+00:00",
            "authority_tier": "A"
        }
    )
    mock_qdrant_client.search.return_value = [mock_point]

    # Initialize retriever
    retriever = SparseRetriever(
        qdrant_client=mock_qdrant_client,
        settings=settings
    )

    # Execute
    chunks = asyncio.run(retriever.retrieve(request))

    # Assertions
    assert len(chunks) == 1
    assert chunks[0].chunk_id == "test-chunk-err"
    assert chunks[0].score == 12.4
    assert "ERR_CONN_RESET" in chunks[0].content
    
    # Verify Qdrant search call
    mock_qdrant_client.search.assert_called_once()
    call_kwargs = mock_qdrant_client.search.call_args.kwargs
    
    # Assert correct collection name and limit
    assert call_kwargs["collection_name"] == settings.sparse_collection
    assert call_kwargs["limit"] == 6  # max_chunks (2) * 3
    
    # Assert query_vector is a NamedSparseVector matching our text encoding
    query_vector = call_kwargs["query_vector"]
    assert isinstance(query_vector, NamedSparseVector)
    assert query_vector.name == SPARSE_VECTOR_NAME
    assert len(query_vector.vector.indices) > 0
    assert len(query_vector.vector.values) > 0
    assert call_kwargs["query_filter"] is not None
