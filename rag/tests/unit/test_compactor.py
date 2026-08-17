"""Unit tests for the LLMLingua-2 compactor module.

Validates that:
- Output token count ≤ budget + 5 %.
- All ``<TRACE_ID:*>`` tags present in output.
- Empty input returns empty string.
- Input already within budget is returned unchanged.
- Multiple tag types are preserved.
"""Unit tests for the LLMLingua2Compactor module (P3-R3).

Uses fallback mode for tests without mocking if LLMLingua is not available,
and provides a test that explicitly mocks LLMLingua to verify its integration.
"""

from __future__ import annotations

import pytest

from src.compactor import LLMLingua2Compactor, _TAG_PATTERN, _mark_tag_tokens, _tokenise


# -----------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------


@pytest.fixture()
def compactor() -> LLMLingua2Compactor:
    """Return a compactor instance using the extractive fallback (no model).

    We pass a non-existent model name to guarantee that ``_load_model``
    fails and the compactor uses the extractive fallback path.
    """
    return LLMLingua2Compactor(model_name="__nonexistent_model_for_tests__")


LONG_CHUNKS = [
    (
        "The payments service lost connectivity to <IP_ADDR:192.168.1.100> "
        "at 09:57 UTC. Error log shows <TRACE_ID:abc123def456> with status "
        "code 500. The database primary <IP_ADDR:10.0.0.5> was unreachable. "
        "Multiple retries failed over a three-minute window causing cascading "
        "failures across the gateway and auth services."
    ),
    (
        "Runbook procedure: When <ERROR_CODE:ERR_CONN_RESET> occurs trigger "
        "manual failover to db-replica-1 via ops-console. Verify health with "
        "systemctl status postgresql and confirm replication lag below two seconds. "
        "Cross-reference with <TRACE_ID:trace789xyz> in the distributed tracing "
        "dashboard before closing the incident ticket."
    ),
    (
        "Slack discussion thread regarding outage on 2026-04-06. User A reports "
        "seeing connection refused errors in payments service logs. User B confirms "
        "<HASH:d8c05cb> commit deployed thirty minutes prior introduced a config "
        "regression. Path <PATH:/var/log/payments/app.log> shows the exact failure "
        "timestamp. Resolution: revert commit and restart the service pod."
    ),
]


# -----------------------------------------------------------------------
# Tests: output token count within budget
# -----------------------------------------------------------------------


class TestTokenBudget:
    """Verify the compactor respects the token budget."""

    def test_output_within_budget(self, compactor: LLMLingua2Compactor) -> None:
        """Compressed output must have ≤ budget + 5 % tokens."""
        budget = 30
        result = compactor.compress(LONG_CHUNKS, budget)
        token_count = len(result.split())
        max_allowed = int(budget * 1.05) + 1  # allow rounding
        assert token_count <= max_allowed, (
            f"Token count {token_count} exceeds budget {budget} + 5 % "
            f"(max {max_allowed})"
        )

    def test_tight_budget(self, compactor: LLMLingua2Compactor) -> None:
        """Even a very tight budget should not exceed the ceiling."""
        budget = 10
        result = compactor.compress(LONG_CHUNKS, budget)
        token_count = len(result.split())
        max_allowed = int(budget * 1.05) + 1
        assert token_count <= max_allowed

    def test_generous_budget_returns_all_content(
        self, compactor: LLMLingua2Compactor
    ) -> None:
        """When budget exceeds input length, all content is preserved."""
        budget = 5000
        result = compactor.compress(LONG_CHUNKS, budget)
        full = "\n\n".join(LONG_CHUNKS)
        # All words from the original should appear (order-independent)
        original_words = set(full.split())
        result_words = set(result.split())
        assert original_words == result_words


# -----------------------------------------------------------------------
# Tests: technical-ID tag preservation
# -----------------------------------------------------------------------


class TestTagPreservation:
    """Verify that <TAG:value> technical identifiers survive compression."""

    def test_trace_ids_preserved(self, compactor: LLMLingua2Compactor) -> None:
        """All <TRACE_ID:*> tags must appear in the compressed output."""
        budget = 30
        result = compactor.compress(LONG_CHUNKS, budget)
        assert "<TRACE_ID:abc123def456>" in result
        assert "<TRACE_ID:trace789xyz>" in result

    def test_all_tag_types_preserved(
        self, compactor: LLMLingua2Compactor
    ) -> None:
        """IP, HASH, PATH, and ERROR_CODE tags must also survive."""
        budget = 40
        result = compactor.compress(LONG_CHUNKS, budget)

        expected_tags = [
            "<IP_ADDR:192.168.1.100>",
            "<IP_ADDR:10.0.0.5>",
            "<ERROR_CODE:ERR_CONN_RESET>",
            "<HASH:d8c05cb>",
            "<PATH:/var/log/payments/app.log>",
            "<TRACE_ID:abc123def456>",
            "<TRACE_ID:trace789xyz>",
        ]
        for tag in expected_tags:
            assert tag in result, f"Missing tag in output: {tag}"


# -----------------------------------------------------------------------
# Tests: edge cases
# -----------------------------------------------------------------------


class TestEdgeCases:
    """Edge-case coverage for the compactor."""

    def test_empty_chunks(self, compactor: LLMLingua2Compactor) -> None:
        """Empty chunk list returns empty string."""
        assert compactor.compress([], budget_tokens=100) == ""

    def test_single_empty_chunk(self, compactor: LLMLingua2Compactor) -> None:
        """A list containing one empty string returns empty string."""
        assert compactor.compress([""], budget_tokens=100) == ""

    def test_no_tags_still_compresses(
        self, compactor: LLMLingua2Compactor
    ) -> None:
        """Chunks without any tags can still be compressed."""
        chunks = ["This is a plain sentence without any special identifiers."]
        result = compactor.compress(chunks, budget_tokens=5)
        assert len(result.split()) <= 6  # budget + tolerance


# -----------------------------------------------------------------------
# Tests: internal helpers
# -----------------------------------------------------------------------


class TestHelpers:
    """Validate internal helper functions."""

    def test_mark_tag_tokens(self) -> None:
        """Tokens containing tags are flagged with probability 1.0."""
        tokens = _tokenise("hello <TRACE_ID:abc123> world")
        _mark_tag_tokens(tokens)
        tag_token = tokens[1]
        assert tag_token.is_tag_token is True
        assert tag_token.preserve_probability == 1.0
        assert tokens[0].is_tag_token is False
        assert tokens[2].is_tag_token is False

    def test_tag_regex_matches(self) -> None:
        """The TAG_PATTERN regex matches all expected tag formats."""
        examples = [
            "<IP_ADDR:192.168.1.1>",
            "<TRACE_ID:abc123>",
            "<HASH:d8c05cb>",
            "<PATH:/var/log/app.log>",
            "<ERROR_CODE:ERR_CONN_RESET>",
        ]
        for tag in examples:
            assert _TAG_PATTERN.search(tag), f"TAG_PATTERN did not match {tag}"
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
