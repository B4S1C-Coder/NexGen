"""Unit tests for FewShotSelector (few_shot.py).

All tests use mocked Qdrant and Ollama — no real infrastructure needed.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.few_shot import (
    FewShotExample,
    FewShotSelector,
    SIMILARITY_THRESHOLD,
    MIN_QDRANT_RESULTS,
    _load_fallback_examples,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_qdrant_hit(nl: str, kql: str, score: float) -> MagicMock:
    """Build a mock Qdrant search result hit."""
    hit = MagicMock()
    hit.payload = {"nl": nl, "kql": kql}
    hit.score = score
    return hit


def make_selector_with_mock_client(hits: list) -> FewShotSelector:
    """Build a FewShotSelector with a mocked Qdrant client."""
    selector = FewShotSelector()
    mock_client = MagicMock()
    mock_client.search.return_value = hits
    selector._client = mock_client
    selector._fallback = [
        FewShotExample(nl="fallback question", kql='service.name: "fallback"')
    ]
    return selector


# ---------------------------------------------------------------------------
# Tests for _load_fallback_examples
# ---------------------------------------------------------------------------

class TestLoadFallbackExamples:
    """Tests for the fallback JSONL loader."""

    def test_loads_real_fallback_file(self) -> None:
        """Must load the actual fallback_examples.jsonl file."""
        path = Path(__file__).parent.parent.parent / "data" / "fallback_examples.jsonl"
        examples = _load_fallback_examples(path)
        assert len(examples) == 10

    def test_each_example_has_nl_and_kql(self) -> None:
        """Every loaded example must have non-empty nl and kql fields."""
        path = Path(__file__).parent.parent.parent / "data" / "fallback_examples.jsonl"
        examples = _load_fallback_examples(path)
        for ex in examples:
            assert ex.nl
            assert ex.kql

    def test_missing_file_returns_empty_list(self, tmp_path: Path) -> None:
        """Missing file must return empty list without raising."""
        result = _load_fallback_examples(tmp_path / "nonexistent.jsonl")
        assert result == []

    def test_fallback_examples_have_no_score(self) -> None:
        """Fallback examples must have score=None (not from Qdrant)."""
        path = Path(__file__).parent.parent.parent / "data" / "fallback_examples.jsonl"
        examples = _load_fallback_examples(path)
        assert all(ex.score is None for ex in examples)


# ---------------------------------------------------------------------------
# Tests for FewShotSelector.select()
# ---------------------------------------------------------------------------

class TestFewShotSelectorSelect:
    """Tests for the select() method with mocked dependencies."""

    @pytest.mark.asyncio
    async def test_returns_qdrant_results_when_above_threshold(self) -> None:
        """Must return Qdrant results when enough are above threshold."""
        hits = [
            make_qdrant_hit("show payment errors", 'service.name: "payments"', 0.92),
            make_qdrant_hit("show auth errors", 'service.name: "auth"', 0.88),
        ]
        selector = make_selector_with_mock_client(hits)

        results = await selector.select("show me payment errors")

        assert len(results) == 2
        assert results[0].score == 0.92
        assert results[0].kql == 'service.name: "payments"'

    @pytest.mark.asyncio
    async def test_falls_back_when_qdrant_returns_zero(self) -> None:
        """Must use fallback when Qdrant returns no results."""
        selector = make_selector_with_mock_client(hits=[])

        results = await selector.select("completely unknown query")

        assert len(results) >= 1
        assert results[0].nl == "fallback question"

    @pytest.mark.asyncio
    async def test_falls_back_when_qdrant_returns_one_result(self) -> None:
        """Must use fallback when Qdrant returns fewer than MIN_QDRANT_RESULTS."""
        hits = [make_qdrant_hit("show errors", 'log.level: "ERROR"', 0.75)]
        selector = make_selector_with_mock_client(hits)

        results = await selector.select("show errors from service")

        # One result is below MIN_QDRANT_RESULTS=2 so must fall back
        assert results[0].nl == "fallback question"

    @pytest.mark.asyncio
    async def test_falls_back_when_no_client(self) -> None:
        """Must use fallback when startup() was never called."""
        selector = FewShotSelector()
        selector._fallback = [
            FewShotExample(nl="fallback", kql='log.level: "ERROR"')
        ]
        # _client is None — startup() not called

        results = await selector.select("any query")

        assert len(results) >= 1

    @pytest.mark.asyncio
    async def test_qdrant_exception_falls_back(self) -> None:
        """Must use fallback when Qdrant search raises an exception."""
        selector = FewShotSelector()
        mock_client = MagicMock()
        mock_client.search.side_effect = Exception("Qdrant unreachable")
        selector._client = mock_client
        selector._fallback = [
            FewShotExample(nl="fallback", kql='log.level: "ERROR"')
        ]

        results = await selector.select("any query")

        assert results[0].nl == "fallback"

    @pytest.mark.asyncio
    async def test_result_is_list_of_few_shot_examples(self) -> None:
        """select() must always return a list of FewShotExample instances."""
        hits = [
            make_qdrant_hit("q1", "kql1", 0.91),
            make_qdrant_hit("q2", "kql2", 0.85),
        ]
        selector = make_selector_with_mock_client(hits)

        results = await selector.select("test query")

        assert isinstance(results, list)
        assert all(isinstance(r, FewShotExample) for r in results)
    
    @pytest.mark.asyncio
    async def test_embedding_failure_falls_back(self) -> None:
        """Must use fallback when embedding raises an exception."""
        selector = FewShotSelector()
        mock_client = MagicMock()
        selector._client = mock_client
        selector._fallback = [
            FewShotExample(nl="fallback", kql='log.level: "ERROR"')
        ]

        # Patch _embed to raise an exception
        with patch("src.few_shot._embed", side_effect=RuntimeError("Ollama down")):
            results = await selector.select("any query")

        assert len(results) >= 1
        assert results[0].nl == "fallback"

    async def test_empty_fallback_and_no_client_returns_empty(self) -> None:
        """Must return empty list when both Qdrant and fallback are unavailable."""
        selector = FewShotSelector()
        selector._fallback = []
        selector._client = None

        results = await selector.select("any query")

        assert results == []

# ---------------------------------------------------------------------------
# Tests for constants
# ---------------------------------------------------------------------------

class TestConstants:
    """Tests that configuration constants have sensible values."""

    def test_similarity_threshold_is_reasonable(self) -> None:
        """Similarity threshold must be between 0.5 and 0.95."""
        assert 0.5 <= SIMILARITY_THRESHOLD <= 0.95

    def test_min_qdrant_results_is_at_least_two(self) -> None:
        """MIN_QDRANT_RESULTS must be at least 2."""
        assert MIN_QDRANT_RESULTS >= 2

