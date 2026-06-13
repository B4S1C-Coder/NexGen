from datetime import datetime, timedelta, timezone

from qdrant_client.http import models

from nexgen_shared.schemas import KnowledgeRequest, KnowledgeTimeWindow
from src.temporal import TemporalFilter
from src.preprocessor import RankedChunk, ChunkMetadata


def test_build_qdrant_filter():
    """Test that build_qdrant_filter correctly uses not_after to create a Qdrant filter."""
    not_after_time = datetime(2026, 4, 6, 10, 0, 0, tzinfo=timezone.utc)
    request = KnowledgeRequest(
        query_id="test-123",
        semantic_query="test",
        source_filters=[],
        time_window=KnowledgeTimeWindow(not_after=not_after_time),
        max_chunks=5,
        compression_budget_tokens=1000
    )
    
    filter_mod = TemporalFilter()
    qdrant_filter = filter_mod.build_qdrant_filter(request)
    
    assert isinstance(qdrant_filter, models.Filter)
    assert len(qdrant_filter.must) == 1
    condition = qdrant_filter.must[0]
    assert condition.key == "created_at"
    assert condition.range.lte == not_after_time


def test_apply_recency_decay():
    """Test that recency decay reduces score for a 50-day-old doc to ~37% of original."""
    now_utc = datetime.now(timezone.utc)
    # A chunk from today
    chunk_new = RankedChunk(
        chunk_id="1",
        content="new content",
        metadata=ChunkMetadata(
            chunk_id="1", doc_id="d1", source_type="runbook", source_uri="uri1",
            authority_tier="A", created_at=now_utc, resolution_status="open",
            is_accepted_answer=False, recency_score=1.0
        ),
        score=1.0
    )
    
    # A chunk from 50 days ago
    chunk_old = RankedChunk(
        chunk_id="2",
        content="old content",
        metadata=ChunkMetadata(
            chunk_id="2", doc_id="d2", source_type="runbook", source_uri="uri2",
            authority_tier="A", created_at=now_utc - timedelta(days=50), resolution_status="open",
            is_accepted_answer=False, recency_score=1.0
        ),
        score=1.0
    )
    
    filter_mod = TemporalFilter()
    chunks = filter_mod.apply_recency_decay([chunk_new, chunk_old], lambda_=0.02)
    
    assert len(chunks) == 2
    # New chunk should have minimal to no decay
    assert chunks[0].score >= 0.99
    # 50 days old chunk should have decay: exp(-0.02 * 50) = exp(-1.0) ~= 0.3678
    assert 0.36 < chunks[1].score < 0.38
