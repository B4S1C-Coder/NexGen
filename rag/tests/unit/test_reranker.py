from datetime import datetime
from unittest.mock import MagicMock, patch

from src.preprocessor import ChunkMetadata, RankedChunk
from src.reranker import CrossEncoderReranker
from src.settings import Settings


@patch("src.reranker.CrossEncoder")
def test_rerank(mock_cross_encoder_class):
    # Setup mock
    mock_model = MagicMock()
    # Let's say chunk 2 is highly relevant (score=10.0), chunk 1 is low (score=-2.0)
    mock_model.predict.return_value = [-2.0, 10.0]
    mock_cross_encoder_class.return_value = mock_model

    reranker = CrossEncoderReranker(Settings(cross_encoder_model="dummy-model"))

    meta = ChunkMetadata(
        chunk_id="", doc_id="", source_type="runbook", source_uri="",
        authority_tier="A", created_at=datetime.utcnow(), resolution_status="resolved",
        is_accepted_answer=True, recency_score=1.0,
    )

    chunks = [
        RankedChunk(chunk_id="c1", content="off-topic chunk", metadata=meta, score=0.5),
        RankedChunk(chunk_id="c2", content="highly relevant exact query terms", metadata=meta, score=0.4),
    ]

    reranked = reranker.rerank("exact query terms", chunks)

    assert len(reranked) == 2
    
    # Highest cross_encoder_score should be first
    assert reranked[0].chunk_id == "c2"
    assert reranked[0].cross_encoder_score == 10.0
    
    assert reranked[1].chunk_id == "c1"
    assert reranked[1].cross_encoder_score == -2.0

    # Ensure predict was called with correct pairs
    mock_model.predict.assert_called_once()
    pairs = mock_model.predict.call_args[0][0]
    assert pairs == [
        ["exact query terms", "off-topic chunk"],
        ["exact query terms", "highly relevant exact query terms"],
    ]
