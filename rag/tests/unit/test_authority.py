from datetime import datetime, timezone

from src.authority import AuthorityScorer
from src.preprocessor import ChunkMetadata, RankedChunk


def _mock_metadata(tier: str, status: str, accepted: bool) -> ChunkMetadata:
    return ChunkMetadata(
        chunk_id="",
        doc_id="",
        source_type="slack",
        source_uri="",
        authority_tier=tier,
        created_at=datetime.now(timezone.utc),
        resolution_status=status,
        is_accepted_answer=accepted,
        recency_score=1.0,
    )


def test_authority_scoring():
    scorer = AuthorityScorer()

    # Create chunks with identical base scores
    base_score = 10.0

    chunk_b = RankedChunk(
        chunk_id="tier-b",
        content="",
        metadata=_mock_metadata("B", "open", False),
        score=base_score,
    )

    chunk_a = RankedChunk(
        chunk_id="tier-a",
        content="",
        metadata=_mock_metadata("A", "resolved", False),
        score=base_score,
    )

    chunk_accepted = RankedChunk(
        chunk_id="tier-b-accepted",
        content="",
        metadata=_mock_metadata("B", "resolved", True),
        score=base_score,
    )

    chunk_deprecated = RankedChunk(
        chunk_id="deprecated",
        content="",
        metadata=_mock_metadata("A", "deprecated", False),
        score=base_score,
    )

    chunks = [chunk_deprecated, chunk_b, chunk_a, chunk_accepted]
    scored = scorer.score(chunks)

    # Calculate expected logic:
    # chunk_a: 10 * 1.25 = 12.5
    # chunk_accepted: 10 * 1.15 = 11.5
    # chunk_b: 10 * 1.0 = 10.0
    # chunk_deprecated: 10 * 1.25 * 0.3 = 3.75

    assert len(scored) == 4
    
    # Check ordering
    assert scored[0].chunk_id == "tier-a"
    assert scored[1].chunk_id == "tier-b-accepted"
    assert scored[2].chunk_id == "tier-b"
    assert scored[3].chunk_id == "deprecated"

    # Check precise math
    assert scored[0].score == 12.5
    assert scored[1].score == 11.5
    assert scored[2].score == 10.0
    assert scored[3].score == 3.75
