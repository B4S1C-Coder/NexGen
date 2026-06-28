from unittest.mock import AsyncMock, MagicMock

import pytest
import httpx

from nexgen_shared.errors import E007KnowledgeConflictUnresolved
from src.conflict import ConflictPair
from src.debate import MultiAgentDebate
from src.preprocessor import ChunkMetadata, RankedChunk
from src.settings import Settings


@pytest.fixture
def mock_settings():
    return Settings(
        llamacpp_generate_server_url="http://fake-url",
        max_debate_rounds=3
    )


@pytest.fixture
def mock_http_client():
    return AsyncMock(spec=httpx.AsyncClient)


@pytest.fixture
def sample_conflict():
    meta1 = ChunkMetadata(
        chunk_id="chunk1",
        doc_id="doc1",
        source_type="slack",
        source_uri="uri1",
        authority_tier="B",
        created_at="2026-06-01T00:00:00Z",
        resolution_status="unknown",
        is_accepted_answer=False,
        recency_score=1.0,
    )
    meta2 = ChunkMetadata(
        chunk_id="chunk2",
        doc_id="doc2",
        source_type="slack",
        source_uri="uri2",
        authority_tier="A",
        created_at="2026-06-01T00:00:00Z",
        resolution_status="unknown",
        is_accepted_answer=False,
        recency_score=1.0,
    )
    chunk_a = RankedChunk(chunk_id="chunk1", content="A", metadata=meta1, score=1.0)
    chunk_b = RankedChunk(chunk_id="chunk2", content="B", metadata=meta2, score=0.9)
    return ConflictPair(chunk_a=chunk_a, chunk_b=chunk_b, confidence=0.9)


@pytest.mark.anyio
async def test_resolve_winner_1(mock_settings, mock_http_client, sample_conflict):
    mock_response = MagicMock()
    # 3 calls: Agent1, Agent2, Aggregator
    mock_response.json.side_effect = [
        {"content": "Argument 1"},
        {"content": "Argument 2"},
        {"content": "WINNER: 1\nChunk 1 is better."}
    ]
    mock_http_client.post.return_value = mock_response

    debate = MultiAgentDebate(mock_settings, mock_http_client)
    result = await debate.resolve(sample_conflict)

    assert result.chunk_id == "chunk1"
    assert mock_http_client.post.call_count == 3


@pytest.mark.anyio
async def test_resolve_winner_merge(mock_settings, mock_http_client, sample_conflict):
    mock_response = MagicMock()
    mock_response.json.side_effect = [
        {"content": "Argument 1"},
        {"content": "Argument 2"},
        {"content": "WINNER: MERGE\nThis is the merged summary."}
    ]
    mock_http_client.post.return_value = mock_response

    debate = MultiAgentDebate(mock_settings, mock_http_client)
    result = await debate.resolve(sample_conflict)

    assert result.chunk_id.startswith("merged-")
    assert result.content == "This is the merged summary."


@pytest.mark.anyio
async def test_resolve_exhausts_rounds(mock_settings, mock_http_client, sample_conflict):
    mock_response = MagicMock()
    # 3 calls per round * 3 rounds = 9 calls. None output WINNER.
    mock_response.json.return_value = {"content": "I am confused."}
    mock_http_client.post.return_value = mock_response

    debate = MultiAgentDebate(mock_settings, mock_http_client)

    with pytest.raises(E007KnowledgeConflictUnresolved):
        await debate.resolve(sample_conflict)
