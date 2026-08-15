"""Unit tests for the MultiAgentDebate module (P3-R2).

All tests mock the LLM calls via httpx so no Ollama server is needed.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from nexgen_shared.errors import E007KnowledgeConflictUnresolved

from src.conflict import ConflictPair
from src.debate import MultiAgentDebate
from src.preprocessor import ChunkMetadata, RankedChunk
from src.settings import Settings


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_metadata(
    chunk_id: str = "c-001",
    tier: str = "A",
    created_at: datetime | None = None,
) -> ChunkMetadata:
    """Create a minimal ChunkMetadata fixture."""
    return ChunkMetadata(
        chunk_id=chunk_id,
        doc_id="doc-001",
        source_type="runbook",
        source_uri="confluence://runbooks/test",
        authority_tier=tier,
        created_at=created_at or datetime.now(timezone.utc),
        resolution_status="resolved",
        is_accepted_answer=False,
        recency_score=1.0,
    )


def _make_chunk(chunk_id: str, content: str, tier: str = "A") -> RankedChunk:
    """Create a RankedChunk with the given content."""
    return RankedChunk(
        chunk_id=chunk_id,
        content=content,
        metadata=_make_metadata(chunk_id, tier=tier),
        score=0.9,
    )


def _make_conflict() -> ConflictPair:
    """Create a fixture ConflictPair."""
    return ConflictPair(
        chunk_i=_make_chunk("c-001", "The service uses PostgreSQL.", tier="A"),
        chunk_j=_make_chunk("c-002", "The service uses MySQL.", tier="B"),
        confidence=0.95,
    )


def _mock_response(content: str) -> MagicMock:
    """Build a mock httpx response with given content."""
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.json.return_value = {
        "choices": [{"message": {"content": content}}]
    }
    return resp


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_winner_selected() -> None:
    """Aggregator returns WINNER: 1 → returns chunk_i."""
    settings = Settings()
    debate = MultiAgentDebate(settings)
    conflict = _make_conflict()

    call_count = 0

    async def mock_post(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            # Agent 1 and Agent 2 arguments
            return _mock_response("This document is more accurate because...")
        else:
            # Aggregator decision
            return _mock_response("WINNER: 1\nDocument 1 is more authoritative as a runbook.")

    with patch("src.debate.httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.post = mock_post
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client_cls.return_value = mock_client

        result = await debate.resolve(conflict)

    assert result is conflict.chunk_i
    assert result.chunk_id == "c-001"


@pytest.mark.asyncio
async def test_winner_2_selected() -> None:
    """Aggregator returns WINNER: 2 → returns chunk_j."""
    settings = Settings()
    debate = MultiAgentDebate(settings)
    conflict = _make_conflict()

    call_count = 0

    async def mock_post(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            return _mock_response("My document is correct because...")
        else:
            return _mock_response("WINNER: 2\nDocument 2 has more recent information.")

    with patch("src.debate.httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.post = mock_post
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client_cls.return_value = mock_client

        result = await debate.resolve(conflict)

    assert result is conflict.chunk_j
    assert result.chunk_id == "c-002"


@pytest.mark.asyncio
async def test_merge_outcome() -> None:
    """Aggregator returns WINNER: MERGE → merged chunk with ≤200 tokens."""
    settings = Settings()
    debate = MultiAgentDebate(settings)
    conflict = _make_conflict()

    merged_text = "Both databases may be in use. PostgreSQL is the primary and MySQL is used for read replicas."

    call_count = 0

    async def mock_post(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            return _mock_response("Argument for my document...")
        else:
            return _mock_response(
                f"WINNER: MERGE\nBoth have valid points.\nMERGED_SUMMARY: {merged_text}"
            )

    with patch("src.debate.httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.post = mock_post
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client_cls.return_value = mock_client

        result = await debate.resolve(conflict)

    assert result.chunk_id == "c-001-merged"
    assert result.content == merged_text
    # Verify token count ≤ 200
    assert len(result.content.split()) <= 200
    # Merged chunk inherits metadata
    assert result.metadata.resolution_status == "resolved"


@pytest.mark.asyncio
async def test_max_rounds_raises_e007() -> None:
    """Aggregator returns no WINNER: token for max rounds → raises E007."""
    settings = Settings()
    debate = MultiAgentDebate(settings)
    conflict = _make_conflict()

    async def mock_post(*args, **kwargs):
        # Never return a valid WINNER token
        return _mock_response("I cannot determine which document is better. More context needed.")

    with patch("src.debate.httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.post = mock_post
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client_cls.return_value = mock_client

        with pytest.raises(E007KnowledgeConflictUnresolved) as exc_info:
            await debate.resolve(conflict)

    assert "E007" in str(exc_info.value)
    assert "c-001" in str(exc_info.value)
    assert "c-002" in str(exc_info.value)


def test_aggregator_output_contains_winner_token() -> None:
    """parse_winner correctly extracts WINNER token from various outputs."""
    # WINNER: 1
    winner, merged = MultiAgentDebate.parse_winner("WINNER: 1\nDoc 1 is better.")
    assert winner == "1"
    assert merged is None

    # WINNER: 2
    winner, merged = MultiAgentDebate.parse_winner("WINNER: 2\nDoc 2 wins.")
    assert winner == "2"
    assert merged is None

    # WINNER: MERGE with summary
    winner, merged = MultiAgentDebate.parse_winner(
        "WINNER: MERGE\nBoth valid.\nMERGED_SUMMARY: Use PostgreSQL primary, MySQL replica."
    )
    assert winner == "MERGE"
    assert merged is not None
    assert "PostgreSQL" in merged

    # No winner token
    winner, merged = MultiAgentDebate.parse_winner("I don't know which is right.")
    assert winner == ""
    assert merged is None


def test_parse_winner_case_insensitive() -> None:
    """parse_winner handles case-insensitive WINNER token."""
    winner, _ = MultiAgentDebate.parse_winner("Winner: 1\nSome justification.")
    assert winner == "1"

    winner, _ = MultiAgentDebate.parse_winner("winner: merge\nMERGED_SUMMARY: combined info.")
    assert winner == "MERGE"
