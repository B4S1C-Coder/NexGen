"""Unit tests for the LLMLingua2Compactor module (P3-R3).

Uses fallback mode for tests without mocking if LLMLingua is not available,
and provides a test that explicitly mocks LLMLingua to verify its integration.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.compactor import LLMLingua2Compactor
from src.settings import Settings


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_long_text() -> str:
    return (
        "This is the first sentence. It has some context that is not very important. "
        "Here is the <TRACE_ID:abc-123> which is critical. "
        "Another filler sentence goes here. We need to reach the budget limit eventually. "
        "The <IP_ADDR:192.168.1.1> is also very important. "
        "The end of the text contains more filler content."
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@patch("src.compactor.HAS_LLMLINGUA", False)
def test_empty_input() -> None:
    """Empty chunks list returns empty string."""
    settings = Settings()
    compactor = LLMLingua2Compactor(settings)
    assert compactor.compress([]) == ""


@patch("src.compactor.HAS_LLMLINGUA", False)
def test_output_within_budget_fallback() -> None:
    """Fallback compression truncates to stay within budget."""
    settings = Settings()
    compactor = LLMLingua2Compactor(settings)

    text = " ".join([f"Word{i}" for i in range(100)])
    budget = 10
    
    compressed = compactor.compress([text], budget_tokens=budget)
    
    # Should not exceed budget tokens
    assert len(compressed.split()) <= budget


@patch("src.compactor.HAS_LLMLINGUA", False)
def test_technical_tags_preserved_fallback() -> None:
    """Fallback compression prioritizes sentences with technical tags."""
    settings = Settings()
    compactor = LLMLingua2Compactor(settings)

    # Budget of 15 words is enough for the two tag sentences, but not the whole text
    budget = 15
    compressed = compactor.compress([_make_long_text()], budget_tokens=budget)

    assert "<TRACE_ID:abc-123>" in compressed
    assert "<IP_ADDR:192.168.1.1>" in compressed


@patch("src.compactor.PromptCompressor")
def test_llmlingua_integration(mock_compressor_cls: MagicMock) -> None:
    """Verifies that the LLMLingua PromptCompressor API is called correctly."""
    # Setup mock
    mock_instance = MagicMock()
    mock_compressor_cls.return_value = mock_instance
    mock_instance.compress_prompt.return_value = {
        "compressed_prompt": "Critical <TRACE_ID:abc-123> and <IP_ADDR:192.168.1.1>."
    }

    settings = Settings()
    compactor = LLMLingua2Compactor(settings)
    
    # Force HAS_LLMLINGUA = True and set the mock compressor
    compactor._compressor = mock_instance

    budget = 20
    chunks = [_make_long_text()]
    
    compressed = compactor.compress(chunks, budget_tokens=budget)

    # Verify return value
    assert "Critical" in compressed
    
    # Verify compress_prompt was called with correct force_tokens
    mock_instance.compress_prompt.assert_called_once()
    kwargs = mock_instance.compress_prompt.call_args.kwargs
    
    assert "force_tokens" in kwargs
    force_tokens = kwargs["force_tokens"]
    assert "<TRACE_ID:abc-123>" in force_tokens
    assert "<IP_ADDR:192.168.1.1>" in force_tokens
    
    # Verify rate calculation is present
    assert "rate" in kwargs


@patch("src.compactor.HAS_LLMLINGUA", False)
def test_under_budget_returns_full_text() -> None:
    """If the original text is under the token budget, it is returned unaltered."""
    settings = Settings()
    compactor = LLMLingua2Compactor(settings)

    chunks = ["Short text.", "Another short one."]
    budget = 1000  # High budget

    compressed = compactor.compress(chunks, budget_tokens=budget)

    assert compressed == "Short text.\n\nAnother short one."
