"""Unit tests for the ConflictDetector (P3-R1).

All tests mock the CrossEncoder model so no model download or GPU is needed.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from src.conflict import ConflictDetector, ConflictPair, _CONTRADICTION_LABEL
from src.preprocessor import ChunkMetadata, RankedChunk
from src.settings import Settings


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_metadata(chunk_id: str = "c-001") -> ChunkMetadata:
    """Create a minimal ChunkMetadata fixture."""
    return ChunkMetadata(
        chunk_id=chunk_id,
        doc_id="doc-001",
        source_type="runbook",
        source_uri="confluence://runbooks/test",
        authority_tier="A",
        created_at=datetime.now(timezone.utc),
        resolution_status="resolved",
        is_accepted_answer=False,
        recency_score=1.0,
    )


def _make_chunk(chunk_id: str, content: str) -> RankedChunk:
    """Create a RankedChunk with the given content."""
    return RankedChunk(
        chunk_id=chunk_id,
        content=content,
        metadata=_make_metadata(chunk_id),
        score=0.9,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@patch("src.conflict.CrossEncoder")
def test_contradiction_detected(mock_cross_encoder_cls: MagicMock) -> None:
    """Two chunks with contradictory content should produce ≥1 ConflictPair."""
    # Set up mock model: return high contradiction confidence
    mock_model = MagicMock()
    mock_cross_encoder_cls.return_value = mock_model

    # Scores: [contradiction, entailment, neutral]
    mock_model.predict.return_value = np.array([[0.95, 0.03, 0.02]])

    settings = Settings()
    detector = ConflictDetector(settings)

    chunk_a = _make_chunk("c-001", "The payments service uses PostgreSQL as its primary database.")
    chunk_b = _make_chunk("c-002", "The payments service uses MySQL as its primary database.")

    conflicts = detector.detect_conflicts([chunk_a, chunk_b])

    assert len(conflicts) >= 1
    assert isinstance(conflicts[0], ConflictPair)
    assert conflicts[0].confidence > settings.conflict_confidence_threshold
    assert conflicts[0].label == "contradiction"
    assert conflicts[0].chunk_i is chunk_a
    assert conflicts[0].chunk_j is chunk_b

    # Verify predict was called with the right sentence pairs
    mock_model.predict.assert_called_once()
    call_args = mock_model.predict.call_args
    sentence_pairs = call_args[0][0]
    assert len(sentence_pairs) == 1
    assert sentence_pairs[0][0] == chunk_a.content
    assert sentence_pairs[0][1] == chunk_b.content


@patch("src.conflict.CrossEncoder")
def test_no_conflict_for_agreeing_chunks(mock_cross_encoder_cls: MagicMock) -> None:
    """Two chunks with agreeing content should produce 0 ConflictPairs."""
    mock_model = MagicMock()
    mock_cross_encoder_cls.return_value = mock_model

    # Scores: high entailment, low contradiction
    mock_model.predict.return_value = np.array([[0.05, 0.90, 0.05]])

    settings = Settings()
    detector = ConflictDetector(settings)

    chunk_a = _make_chunk("c-001", "The service runs on port 8080.")
    chunk_b = _make_chunk("c-002", "The application listens on port 8080.")

    conflicts = detector.detect_conflicts([chunk_a, chunk_b])

    assert len(conflicts) == 0


@patch("src.conflict.CrossEncoder")
def test_empty_chunks_returns_empty(mock_cross_encoder_cls: MagicMock) -> None:
    """An empty or single-element chunk list should return no conflicts."""
    mock_model = MagicMock()
    mock_cross_encoder_cls.return_value = mock_model

    settings = Settings()
    detector = ConflictDetector(settings)

    # Empty list
    assert detector.detect_conflicts([]) == []

    # Single chunk — no pairs possible
    chunk = _make_chunk("c-001", "Some content")
    assert detector.detect_conflicts([chunk]) == []

    # predict should never be called
    mock_model.predict.assert_not_called()


@patch("src.conflict.CrossEncoder")
def test_threshold_boundary(mock_cross_encoder_cls: MagicMock) -> None:
    """Contradiction confidence at exactly the threshold should NOT be included (> not >=)."""
    mock_model = MagicMock()
    mock_cross_encoder_cls.return_value = mock_model

    # Confidence exactly at threshold (0.8)
    mock_model.predict.return_value = np.array([[0.80, 0.10, 0.10]])

    settings = Settings()
    detector = ConflictDetector(settings)

    chunk_a = _make_chunk("c-001", "Service A depends on Service B.")
    chunk_b = _make_chunk("c-002", "Service A does not depend on Service B.")

    conflicts = detector.detect_conflicts([chunk_a, chunk_b])
    # Exactly at threshold — should NOT be a conflict (strict >)
    assert len(conflicts) == 0


@patch("src.conflict.CrossEncoder")
def test_multiple_pairs_partial_conflicts(mock_cross_encoder_cls: MagicMock) -> None:
    """With 3 chunks (3 pairs), only the contradictory pair should be flagged."""
    mock_model = MagicMock()
    mock_cross_encoder_cls.return_value = mock_model

    # 3 chunks → 3 pairs: (A,B), (A,C), (B,C)
    # Only pair (A,B) is contradictory
    mock_model.predict.return_value = np.array([
        [0.92, 0.04, 0.04],  # (A,B) — contradiction
        [0.10, 0.80, 0.10],  # (A,C) — entailment
        [0.15, 0.70, 0.15],  # (B,C) — entailment
    ])

    settings = Settings()
    detector = ConflictDetector(settings)

    chunk_a = _make_chunk("c-001", "Database is PostgreSQL.")
    chunk_b = _make_chunk("c-002", "Database is MySQL.")
    chunk_c = _make_chunk("c-003", "Database runs on port 5432.")

    conflicts = detector.detect_conflicts([chunk_a, chunk_b, chunk_c])

    assert len(conflicts) == 1
    assert conflicts[0].chunk_i is chunk_a
    assert conflicts[0].chunk_j is chunk_b
    assert conflicts[0].confidence == pytest.approx(0.92)
