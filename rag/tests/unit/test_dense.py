import asyncio
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime, timezone

from qdrant_client import AsyncQdrantClient
from qdrant_client.http.models import ScoredPoint
import httpx

from nexgen_shared.schemas import KnowledgeRequest, KnowledgeTimeWindow
from src.dense import DenseRetriever
from src.settings import Settings


def test_retrieve_dense():
    # Setup test data
    settings = Settings(
        llamacpp_embed_server_url="http://mock-embed:8082",
        dense_collection="test_dense"
    )
    
    request = KnowledgeRequest(
        query_id="q-1",
        semantic_query="test query",
        source_filters=[],
        time_window=KnowledgeTimeWindow(not_after=datetime.now(timezone.utc)),
        max_chunks=4,
        compression_budget_tokens=1000
    )

    # Mock HTTP client
    mock_http_response = MagicMock()
    mock_http_response.raise_for_status = MagicMock()
    mock_http_response.json.return_value = {"embedding": [0.1, 0.2, 0.3]}
    
    mock_http_client = AsyncMock()
    mock_http_client.post.return_value = mock_http_response

    # Mock Qdrant client
    mock_qdrant_client = AsyncMock()
    mock_point = ScoredPoint(
        id="point-1",
        version=1,
        score=0.95,
        payload={
            "chunk_id": "test-chunk-1",
            "doc_id": "doc-1",
            "content": "This is a test document.",
            "source_type": "runbook",
            "created_at": "2026-05-01T10:00:00+00:00",
            "authority_tier": "A"
        }
    )
    mock_qdrant_client.search.return_value = [mock_point]

    # Initialize retriever
    retriever = DenseRetriever(
        qdrant_client=mock_qdrant_client,
        http_client=mock_http_client,
        settings=settings
    )

    # Execute
    chunks = asyncio.run(retriever.retrieve(request))

    # Assertions
    assert len(chunks) == 1
    assert chunks[0].chunk_id == "test-chunk-1"
    assert chunks[0].score == 0.95
    assert chunks[0].content == "This is a test document."
    
    # Verify HTTP call
    mock_http_client.post.assert_called_once_with(
        f"{settings.llamacpp_embed_server_url.rstrip('/')}/embedding",
        json={"content": "test query"}
    )
    
    # Verify Qdrant search call
    mock_qdrant_client.search.assert_called_once()
    call_kwargs = mock_qdrant_client.search.call_args.kwargs
    assert call_kwargs["collection_name"] == settings.dense_collection
    assert call_kwargs["query_vector"] == [0.1, 0.2, 0.3]
    assert call_kwargs["limit"] == 12  # max_chunks (4) * 3
    assert call_kwargs["query_filter"] is not None
