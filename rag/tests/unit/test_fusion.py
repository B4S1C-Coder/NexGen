from datetime import datetime

from src.fusion import WRRFFusion
from src.preprocessor import ChunkMetadata, RankedChunk
from src.settings import Settings


def test_classify_query():
    fusion = WRRFFusion(Settings(wrrf_k=60))

    # Natural language
    w_d, w_s = fusion.classify_query("how do I configure the payments service?")
    assert w_d == 0.7
    assert w_s == 0.3

    # Error code
    w_d, w_s = fusion.classify_query("why am I getting ERR_PAYMENT_FAILED?")
    assert w_d == 0.3
    assert w_s == 0.7

    # Mixed or unclassified (just few words)
    w_d, w_s = fusion.classify_query("payments fail")
    assert w_d == 0.5
    assert w_s == 0.5


def test_fuse():
    fusion = WRRFFusion(Settings(wrrf_k=60))

    # Dummy metadata
    meta = ChunkMetadata(
        chunk_id="",
        doc_id="",
        source_type="runbook",
        source_uri="",
        authority_tier="A",
        created_at=datetime.utcnow(),
        resolution_status="resolved",
        is_accepted_answer=True,
        recency_score=1.0,
    )

    # Create dummy chunks
    dense = [
        RankedChunk(chunk_id="c1", content="chunk 1", metadata=meta, score=1.0),
        RankedChunk(chunk_id="c2", content="chunk 2", metadata=meta, score=0.8),
    ]

    sparse = [
        RankedChunk(chunk_id="c2", content="chunk 2", metadata=meta, score=5.0),
        RankedChunk(chunk_id="c3", content="chunk 3", metadata=meta, score=3.0),
    ]

    # Weights for a mixed query
    fused = fusion.fuse(dense, sparse, w_dense=0.5, w_sparse=0.5)

    # Expect 3 unique chunks
    assert len(fused) == 3

    # Check WRRF scores
    # c1: dense_rank=1, sparse_rank=None => 0.5 * (1/61) = 0.008196
    # c2: dense_rank=2, sparse_rank=1 => 0.5 * (1/62) + 0.5 * (1/61) = 0.008064 + 0.008196 = 0.01626
    # c3: dense_rank=None, sparse_rank=2 => 0.5 * (1/62) = 0.008064

    # c2 should be ranked highest
    assert fused[0].chunk_id == "c2"
    assert fused[1].chunk_id == "c1"
    assert fused[2].chunk_id == "c3"
