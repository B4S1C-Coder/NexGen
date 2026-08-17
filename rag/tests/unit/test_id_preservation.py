"""Unit tests for the TechnicalIDPreservationLayer module.

Validates that:
- A deliberately stripped ``<TRACE_ID:*>`` is re-injected in the output.
- The ``nexgen_rag_id_reinjected_total`` counter increments on re-injection.
- No re-injection occurs when all tags are already present.
- Multiple missing tags are all re-injected.
- Empty inputs are handled gracefully.
"""

from __future__ import annotations

import pytest

from src.id_preservation import (
    TechnicalIDPreservationLayer,
    PreservationResult,
    id_reinjected_total,
    _find_best_sentence_index,
)


# -----------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------


@pytest.fixture()
def layer() -> TechnicalIDPreservationLayer:
    """Return a fresh preservation layer instance."""
    return TechnicalIDPreservationLayer()


@pytest.fixture(autouse=True)
def _reset_counter() -> None:
    """Reset the Prometheus counter before each test."""
    id_reinjected_total.reset()


ORIGINAL_CHUNKS = [
    (
        "The payments service lost connectivity to <IP_ADDR:192.168.1.100> "
        "at 09:57 UTC. Error log shows <TRACE_ID:abc123def456> with status "
        "code 500."
    ),
    (
        "Runbook procedure: When <ERROR_CODE:ERR_CONN_RESET> occurs trigger "
        "manual failover. Cross-reference with <TRACE_ID:trace789xyz> in the "
        "distributed tracing dashboard."
    ),
]


# -----------------------------------------------------------------------
# Tests: re-injection of stripped tags
# -----------------------------------------------------------------------


class TestReinjection:
    """Verify that missing tags are re-injected into compressed text."""

    def test_stripped_trace_id_reinjected(
        self, layer: TechnicalIDPreservationLayer
    ) -> None:
        """A deliberately removed <TRACE_ID:*> must appear in the output."""
        # Compressed text with <TRACE_ID:abc123def456> deliberately removed
        compressed = (
            "The payments service lost connectivity to <IP_ADDR:192.168.1.100> "
            "at 09:57 UTC. Runbook procedure: When <ERROR_CODE:ERR_CONN_RESET> "
            "occurs trigger manual failover. Cross-reference with "
            "<TRACE_ID:trace789xyz> in the distributed tracing dashboard."
        )

        result = layer.verify_and_reinject(ORIGINAL_CHUNKS, compressed)

        assert "<TRACE_ID:abc123def456>" in result.text
        assert result.total_reinjections == 1
        assert "<TRACE_ID:abc123def456>" in result.reinjected_tags

    def test_multiple_stripped_tags_reinjected(
        self, layer: TechnicalIDPreservationLayer
    ) -> None:
        """Multiple missing tags must all be re-injected."""
        # Remove both TRACE_IDs
        compressed = (
            "The payments service lost connectivity to <IP_ADDR:192.168.1.100> "
            "at 09:57 UTC. Runbook procedure: When <ERROR_CODE:ERR_CONN_RESET> "
            "occurs trigger manual failover."
        )

        result = layer.verify_and_reinject(ORIGINAL_CHUNKS, compressed)

        assert "<TRACE_ID:abc123def456>" in result.text
        assert "<TRACE_ID:trace789xyz>" in result.text
        assert result.total_reinjections == 2

    def test_all_tags_present_no_reinjection(
        self, layer: TechnicalIDPreservationLayer
    ) -> None:
        """When all tags survive compression, no re-injection occurs."""
        compressed = (
            "The payments service <IP_ADDR:192.168.1.100> "
            "<TRACE_ID:abc123def456> <ERROR_CODE:ERR_CONN_RESET> "
            "<TRACE_ID:trace789xyz> dashboard."
        )

        result = layer.verify_and_reinject(ORIGINAL_CHUNKS, compressed)

        assert result.total_reinjections == 0
        assert result.reinjected_tags == []
        assert result.text == compressed


# -----------------------------------------------------------------------
# Tests: Prometheus counter
# -----------------------------------------------------------------------


class TestPrometheusCounter:
    """Verify the nexgen_rag_id_reinjected_total counter behaviour."""

    def test_counter_increments_on_reinjection(
        self, layer: TechnicalIDPreservationLayer
    ) -> None:
        """Counter must increment by the number of re-injected tags."""
        compressed = "Some compressed text without any tags."

        assert id_reinjected_total.value == 0
        layer.verify_and_reinject(ORIGINAL_CHUNKS, compressed)
        # 4 tags total: 2 TRACE_IDs + 1 IP_ADDR + 1 ERROR_CODE
        assert id_reinjected_total.value == 4

    def test_counter_does_not_increment_when_no_reinjection(
        self, layer: TechnicalIDPreservationLayer
    ) -> None:
        """Counter must stay at 0 when no tags are missing."""
        compressed = (
            "<IP_ADDR:192.168.1.100> <TRACE_ID:abc123def456> "
            "<ERROR_CODE:ERR_CONN_RESET> <TRACE_ID:trace789xyz>"
        )

        layer.verify_and_reinject(ORIGINAL_CHUNKS, compressed)
        assert id_reinjected_total.value == 0

    def test_counter_accumulates_across_calls(
        self, layer: TechnicalIDPreservationLayer
    ) -> None:
        """Counter accumulates across multiple verify_and_reinject calls."""
        compressed1 = "No tags here."
        compressed2 = "Also no tags here."

        layer.verify_and_reinject(ORIGINAL_CHUNKS, compressed1)
        first_value = id_reinjected_total.value
        assert first_value == 4

        layer.verify_and_reinject(ORIGINAL_CHUNKS, compressed2)
        assert id_reinjected_total.value == 8


# -----------------------------------------------------------------------
# Tests: edge cases
# -----------------------------------------------------------------------


class TestEdgeCases:
    """Edge-case coverage for the preservation layer."""

    def test_empty_compressed_text(
        self, layer: TechnicalIDPreservationLayer
    ) -> None:
        """Empty compressed text returns empty result."""
        result = layer.verify_and_reinject(ORIGINAL_CHUNKS, "")
        assert result.text == ""
        assert result.total_reinjections == 0

    def test_empty_original_chunks(
        self, layer: TechnicalIDPreservationLayer
    ) -> None:
        """No original chunks means no tags to verify."""
        result = layer.verify_and_reinject([], "Some compressed text.")
        assert result.text == "Some compressed text."
        assert result.total_reinjections == 0

    def test_chunks_without_tags(
        self, layer: TechnicalIDPreservationLayer
    ) -> None:
        """Original chunks with no tags require no preservation."""
        plain_chunks = ["Just a plain document.", "No special identifiers."]
        result = layer.verify_and_reinject(plain_chunks, "Compressed plain text.")
        assert result.text == "Compressed plain text."
        assert result.total_reinjections == 0


# -----------------------------------------------------------------------
# Tests: internal helpers
# -----------------------------------------------------------------------


class TestHelpers:
    """Validate internal helper functions."""

    def test_find_best_sentence_for_trace_id(self) -> None:
        """Tag should be placed near the sentence mentioning trace."""
        sentences = [
            "The payments service failed.",
            "Check the trace logs in the dashboard.",
            "Restart the pod.",
        ]
        idx = _find_best_sentence_index(sentences, "<TRACE_ID:abc123>")
        # Sentence 1 contains "trace" which matches keyword from TRACE_ID
        assert idx == 1

    def test_find_best_sentence_fallback_to_last(self) -> None:
        """When no keyword matches, fall back to the last sentence."""
        sentences = [
            "Nothing related here.",
            "Also unrelated content.",
        ]
        idx = _find_best_sentence_index(sentences, "<HASH:d8c05cb>")
        # No keyword overlap; should fall back to last sentence
        assert idx == len(sentences) - 1
