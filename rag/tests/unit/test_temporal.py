from datetime import datetime, timezone

from qdrant_client.http import models

from nexgen_shared.schemas import KnowledgeRequest, KnowledgeTimeWindow
from src.temporal import TemporalFilter


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
