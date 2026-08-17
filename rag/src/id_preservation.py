"""Post-compression Technical ID preservation layer (rag.md §6.2).

Verifies that all ``<TAG:value>`` technical identifiers from the original
uncompressed chunks are present in the compressed output. Any missing tags
are re-injected at the nearest semantically similar sentence boundary.
Every re-injection is logged as a warning and counted via a Prometheus
counter (``nexgen_rag_id_reinjected_total``).
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Regex for all <TAG:value> technical identifiers (same as compactor)
# ---------------------------------------------------------------------------
_TAG_PATTERN = re.compile(r"<(?:IP_ADDR|TRACE_ID|HASH|PATH|ERROR_CODE):[^>]+>")

# ---------------------------------------------------------------------------
# Prometheus-style counter (lightweight in-process counter for metrics)
# ---------------------------------------------------------------------------


class _Counter:
    """Minimal in-process counter for Prometheus-style metrics.

    Parameters:
        name: Metric name following Prometheus conventions.
        description: Human-readable description of the metric.
    """

    def __init__(self, name: str, description: str = "") -> None:
        self.name = name
        self.description = description
        self._value: int = 0

    def inc(self, amount: int = 1) -> None:
        """Increment the counter.

        Parameters:
            amount: Value to add to the counter.
        """
        self._value += amount

    @property
    def value(self) -> int:
        """Return the current counter value."""
        return self._value

    def reset(self) -> None:
        """Reset the counter to zero (primarily for testing)."""
        self._value = 0


# Module-level metric instance
id_reinjected_total = _Counter(
    name="nexgen_rag_id_reinjected_total",
    description="Total number of technical IDs re-injected after compression.",
)


@dataclass(slots=True)
class PreservationResult:
    """Outcome of the Technical ID preservation pass.

    Parameters:
        text: The output text with all tags guaranteed present.
        reinjected_tags: List of tags that were missing and re-injected.
        total_reinjections: Count of re-injected tags.
    """

    text: str
    reinjected_tags: list[str] = field(default_factory=list)
    total_reinjections: int = 0


class TechnicalIDPreservationLayer:
    """Post-compression verification and re-injection of technical IDs.

    After the ``LLMLingua2Compactor`` compresses the context, this layer
    verifies that every ``<TAG:value>`` pattern from the original
    uncompressed chunks survives in the compressed output.  For any
    missing tag, the layer re-injects it at the nearest semantically
    appropriate sentence boundary.

    Every re-injection increments the ``nexgen_rag_id_reinjected_total``
    Prometheus counter and emits a ``WARNING``-level log entry.
    """

    def verify_and_reinject(
        self,
        original_chunks: list[str],
        compressed_text: str,
    ) -> PreservationResult:
        """Verify tag presence and re-inject any missing tags.

        Parameters:
            original_chunks: The original uncompressed chunk texts.
            compressed_text: The text produced by the compactor.

        Returns:
            A ``PreservationResult`` with the corrected text and metadata
            about any re-injections performed.
        """
        if not compressed_text:
            return PreservationResult(text=compressed_text)

        # 1. Extract all <TAG:value> patterns from original chunks
        original_tags = self._extract_tags(original_chunks)

        if not original_tags:
            return PreservationResult(text=compressed_text)

        # 2. Check which tags are missing from the compressed output
        missing_tags = [
            tag for tag in original_tags if tag not in compressed_text
        ]

        if not missing_tags:
            return PreservationResult(text=compressed_text)

        # 3. Re-inject missing tags at the best sentence boundary
        result_text = self._reinject_tags(compressed_text, missing_tags)

        # 4. Emit metrics and logs
        count = len(missing_tags)
        id_reinjected_total.inc(count)

        for tag in missing_tags:
            logger.warning(
                "Technical ID re-injected after compression: %s", tag
            )

        return PreservationResult(
            text=result_text,
            reinjected_tags=missing_tags,
            total_reinjections=count,
        )

    @staticmethod
    def _extract_tags(chunks: list[str]) -> list[str]:
        """Extract all unique ``<TAG:value>`` patterns from chunk texts.

        Parameters:
            chunks: List of text chunks to scan.

        Returns:
            Deduplicated list of tag strings, preserving discovery order.
        """
        seen: set[str] = set()
        tags: list[str] = []
        for chunk in chunks:
            for match in _TAG_PATTERN.finditer(chunk):
                tag = match.group()
                if tag not in seen:
                    seen.add(tag)
                    tags.append(tag)
        return tags

    @staticmethod
    def _reinject_tags(text: str, missing_tags: list[str]) -> str:
        """Re-inject missing tags at the nearest sentence boundary.

        For each missing tag, the layer inserts it after the sentence
        that is most likely related — determined by simple keyword
        overlap between the tag's type/value and the sentence content.
        If no good match is found, the tag is appended at the end.

        Parameters:
            text: The compressed text to augment.
            missing_tags: Tags that need to be re-injected.

        Returns:
            The text with all missing tags inserted.
        """
        # Split into sentence-like segments preserving delimiters
        sentences = re.split(r"(?<=[.!?\n])\s+", text)
        if not sentences:
            return text + " " + " ".join(missing_tags)

        for tag in missing_tags:
            best_idx = _find_best_sentence_index(sentences, tag)
            # Insert the tag immediately after the best-matching sentence
            sentences[best_idx] = sentences[best_idx].rstrip() + " " + tag

        return " ".join(s for s in sentences if s.strip())


def _find_best_sentence_index(sentences: list[str], tag: str) -> int:
    """Find the sentence index most semantically related to a tag.

    Uses a simple keyword-overlap heuristic: extracts the tag value and
    type, then scores each sentence by how many of those keywords appear.
    Falls back to the last sentence if no match is found.

    Parameters:
        sentences: List of sentence strings from the compressed text.
        tag: The ``<TAG:value>`` string to place.

    Returns:
        Zero-based index of the best matching sentence.
    """
    # Extract tag type and value  e.g. "<TRACE_ID:abc123>" → ("TRACE_ID", "abc123")
    inner_match = re.match(r"<(\w+):(.+)>", tag)
    if not inner_match:
        return len(sentences) - 1

    tag_type = inner_match.group(1).lower()
    tag_value = inner_match.group(2).lower()

    # Build search keywords from the tag
    keywords: set[str] = set()
    # Split tag_type on underscore: e.g. TRACE_ID → {"trace", "id"}
    keywords.update(tag_type.split("_"))
    # Add value fragments (split on common delimiters)
    keywords.update(re.split(r"[.:/_\-]", tag_value))
    keywords.discard("")

    best_idx = len(sentences) - 1
    best_score = 0

    for idx, sentence in enumerate(sentences):
        s_lower = sentence.lower()
        score = sum(1 for kw in keywords if kw in s_lower)
        if score > best_score:
            best_score = score
            best_idx = idx

    return best_idx
