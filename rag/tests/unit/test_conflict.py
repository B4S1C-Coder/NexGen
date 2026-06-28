from unittest.mock import MagicMock, patch

import numpy as np

from src.conflict import ConflictDetector
from src.preprocessor import ChunkMetadata, RankedChunk
from src.settings import Settings


@patch("src.conflict.CrossEncoder")
def test_detect_conflicts_finds_contradiction(mock_cross_encoder_class):
    # Mock settings
    settings = Settings(nli_model="fake-model", conflict_confidence_threshold=0.8)

    # Setup mock cross-encoder
    mock_model = MagicMock()
    mock_cross_encoder_class.return_value = mock_model
    
    # Simulate id2label mapping
    mock_model.config.id2label = {0: "CONTRADICTION", 1: "ENTAILMENT", 2: "NEUTRAL"}
    
    # Setup predict to return high probability for contradiction for the single pair
    # Scores before softmax. Let's make index 0 much larger than others.
    # Softmax([5.0, 0.0, 0.0]) -> [0.986, 0.006, 0.006]
    mock_model.predict.return_value = np.array([[5.0, 0.0, 0.0]])

    detector = ConflictDetector(settings)

    meta = ChunkMetadata(
        chunk_id="chunk1",
        doc_id="doc1",
        source_type="slack",
        source_uri="slack://thread",
        authority_tier="B",
        created_at="2026-06-01T00:00:00Z",
        resolution_status="unknown",
        is_accepted_answer=False,
        recency_score=1.0,
    )
    
    chunk_a = RankedChunk(
        chunk_id="chunk1",
        content="The server crashed because of a memory leak in the payments module.",
        metadata=meta,
        score=0.9
    )
    
    chunk_b = RankedChunk(
        chunk_id="chunk2",
        content="The payments module has no memory leaks and is working perfectly.",
        metadata=meta,
        score=0.85
    )

    pairs = detector.detect_conflicts([chunk_a, chunk_b])

    assert len(pairs) == 1
    assert pairs[0].chunk_a == chunk_a
    assert pairs[0].chunk_b == chunk_b
    assert pairs[0].confidence > 0.8
    
    # Assert model predict was called with correct arguments
    mock_model.predict.assert_called_once_with([(chunk_a.content, chunk_b.content)])


@patch("src.conflict.CrossEncoder")
def test_detect_conflicts_ignores_low_confidence(mock_cross_encoder_class):
    settings = Settings(nli_model="fake-model", conflict_confidence_threshold=0.8)
    mock_model = MagicMock()
    mock_cross_encoder_class.return_value = mock_model
    mock_model.config.id2label = {0: "CONTRADICTION", 1: "ENTAILMENT", 2: "NEUTRAL"}
    
    # Softmax([0.5, 0.4, 0.4]) -> [0.35, 0.32, 0.32], below 0.8 threshold
    mock_model.predict.return_value = np.array([[0.5, 0.4, 0.4]])

    detector = ConflictDetector(settings)

    chunk_a = RankedChunk(chunk_id="1", content="A", metadata=MagicMock(), score=1.0)
    chunk_b = RankedChunk(chunk_id="2", content="B", metadata=MagicMock(), score=0.9)

    pairs = detector.detect_conflicts([chunk_a, chunk_b])
    
    assert len(pairs) == 0


@patch("src.conflict.CrossEncoder")
def test_detect_conflicts_ignores_non_contradiction(mock_cross_encoder_class):
    settings = Settings(nli_model="fake-model", conflict_confidence_threshold=0.8)
    mock_model = MagicMock()
    mock_cross_encoder_class.return_value = mock_model
    mock_model.config.id2label = {0: "CONTRADICTION", 1: "ENTAILMENT", 2: "NEUTRAL"}
    
    # Softmax([0.0, 5.0, 0.0]) -> [0.006, 0.986, 0.006], entailment
    mock_model.predict.return_value = np.array([[0.0, 5.0, 0.0]])

    detector = ConflictDetector(settings)

    chunk_a = RankedChunk(chunk_id="1", content="A", metadata=MagicMock(), score=1.0)
    chunk_b = RankedChunk(chunk_id="2", content="B", metadata=MagicMock(), score=0.9)

    pairs = detector.detect_conflicts([chunk_a, chunk_b])
    
    assert len(pairs) == 0
