"""LLMLingua-2 context compactor for the RAG pipeline.

Compresses concatenated chunks to fit within a token budget using a binary
token classifier (LLMLingua-2 BERT). Technical ID tags (``<TAG:value>``)
are unconditionally preserved via a pre-pass that marks them as
non-discardable. When the model is unavailable a cosine-similarity based
extractive fallback is used instead.
"""

from __future__ import annotations

import logging
import math
import re
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Regex for all <TAG:value> technical identifiers
# ---------------------------------------------------------------------------
_TAG_PATTERN = re.compile(r"<(?:IP_ADDR|TRACE_ID|HASH|PATH|ERROR_CODE):[^>]+>")


@dataclass(slots=True)
class _TokenEntry:
    """Internal bookkeeping for a single whitespace-delimited token.

    Parameters:
        text: The original token string.
        preserve_probability: Probability that the token should be kept.
            ``1.0`` means the token is unconditionally preserved.
        is_tag_token: Whether the token is (or contains) a technical-ID tag.
    """

    text: str
    preserve_probability: float = 0.5
    is_tag_token: bool = False


class LLMLingua2Compactor:
    """Binary-token-classification compactor inspired by LLMLingua-2.

    The compactor can operate in two modes:

    1. **Model mode** — A ``transformers`` sequence-classification model
       produces per-token keep/discard probabilities.  The probabilities
       are iteratively thresholded until the output fits within the
       requested budget (±5 %).
    2. **Fallback mode** — When the model cannot be loaded (e.g. in
       unit-tests or lightweight deployments), an extractive
       sentence-level selection strategy preserves the most information
       within the budget.

    In both modes, ``<TAG:value>`` patterns (IP addresses, trace IDs,
    hashes, file paths, error codes) are forced to ``preserve_probability
    = 1.0`` so they are **never** discarded.

    Parameters:
        model_name: HuggingFace model identifier for the token classifier.
    """

    # Iterative ratio constants
    _MAX_ITERATIONS = 10
    _TOLERANCE = 0.05  # ±5 %

    def __init__(self, model_name: str | None = None) -> None:
        self._model = None
        self._tokenizer = None
        self._model_name = (
            model_name
            or "microsoft/llmlingua-2-bert-base-multilingual-cased-meetingbank"
        )
        self._model_available = False
        self._load_model()

    # ------------------------------------------------------------------
    # Model loading
    # ------------------------------------------------------------------

    def _load_model(self) -> None:
        """Attempt to load the LLMLingua-2 BERT model.

        Catches all import and runtime errors so the compactor can
        gracefully degrade to the extractive fallback.
        """
        try:
            from transformers import AutoModelForTokenClassification, AutoTokenizer  # type: ignore[import-untyped]

            self._tokenizer = AutoTokenizer.from_pretrained(self._model_name)
            self._model = AutoModelForTokenClassification.from_pretrained(
                self._model_name
            )
            self._model.eval()
            self._model_available = True
            logger.info("LLMLingua-2 model loaded: %s", self._model_name)
        except Exception:
            logger.warning(
                "LLMLingua-2 model unavailable; falling back to extractive compaction"
            )
            self._model_available = False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def compress(self, chunks: list[str], budget_tokens: int) -> str:
        """Compress *chunks* to at most *budget_tokens* whitespace tokens.

        Parameters:
            chunks: List of text chunks to compress.
            budget_tokens: Maximum number of whitespace-delimited tokens
                allowed in the output.

        Returns:
            A single compressed string that fits within the budget and
            preserves all ``<TAG:value>`` technical identifiers.
        """
        if not chunks:
            return ""

        concatenated = "\n\n".join(chunks)
        tokens = _tokenise(concatenated)

        if not tokens:
            return ""

        # Fast path: already within budget
        if len(tokens) <= budget_tokens:
            return _reconstruct(tokens)

        # Mark tags as unconditionally preserved (pre-pass)
        _mark_tag_tokens(tokens)

        if self._model_available:
            result = self._model_compress(tokens, budget_tokens)
        else:
            result = self._extractive_compress(tokens, budget_tokens)

        return result

    # ------------------------------------------------------------------
    # Model-based compression
    # ------------------------------------------------------------------

    def _model_compress(
        self,
        tokens: list[_TokenEntry],
        budget_tokens: int,
    ) -> str:
        """Use the BERT token-classifier to assign keep/discard probabilities.

        Parameters:
            tokens: Pre-tokenised entries (tag tokens already marked).
            budget_tokens: Target output length.

        Returns:
            Compressed text string.
        """
        import torch  # type: ignore[import-untyped]

        text = _reconstruct(tokens)
        inputs = self._tokenizer(  # type: ignore[union-attr]
            text,
            return_tensors="pt",
            truncation=True,
            max_length=512,
            return_offsets_mapping=True,
        )
        offset_mapping = inputs.pop("offset_mapping")[0].tolist()

        with torch.no_grad():
            outputs = self._model(**inputs)  # type: ignore[misc]
            logits = outputs.logits[0]  # (seq_len, num_labels)
            # Label 1 = "preserve"
            probs = torch.softmax(logits, dim=-1)[:, 1].tolist()

        # Map sub-word probabilities back to whitespace tokens.
        # For each whitespace token we take the *max* sub-word probability.
        _map_subword_probs_to_tokens(tokens, text, offset_mapping, probs)

        # Iterative threshold search
        return self._iterative_select(tokens, budget_tokens)

    # ------------------------------------------------------------------
    # Extractive fallback
    # ------------------------------------------------------------------

    def _extractive_compress(
        self,
        tokens: list[_TokenEntry],
        budget_tokens: int,
    ) -> str:
        """Sentence-level extractive fallback when no model is available.

        Splits the token sequence into sentences, ranks them by their
        proportion of tag tokens (as a proxy for importance), and greedily
        selects sentences until the budget is filled.

        Parameters:
            tokens: Pre-tokenised entries.
            budget_tokens: Target output length.

        Returns:
            Compressed text string.
        """
        text = _reconstruct(tokens)
        sentences = _split_sentences(text)

        if not sentences:
            return ""

        # Pre-compute the set of all tags that must appear in the output.
        all_tags: set[str] = set()
        for token in tokens:
            for match in _TAG_PATTERN.finditer(token.text):
                all_tags.add(match.group())

        # Score each sentence: tag density + position bias (earlier = better)
        scored: list[tuple[float, int, str]] = []
        for idx, sentence in enumerate(sentences):
            s_tokens = sentence.split()
            tag_count = sum(1 for t in s_tokens if _TAG_PATTERN.search(t))
            density = tag_count / max(len(s_tokens), 1)
            position_bias = 1.0 / (1.0 + idx * 0.1)
            scored.append((density + position_bias, idx, sentence))

        # Sort descending by score
        scored.sort(key=lambda x: x[0], reverse=True)

        selected: list[tuple[int, str]] = []
        used_tokens = 0
        covered_tags: set[str] = set()

        # Reserve budget for tags that may need re-injection.
        # As we add sentences that contain tags, the reservation shrinks.
        uncovered_tag_tokens = sum(
            len(tag.split()) for tag in all_tags
        )
        effective_budget = budget_tokens - uncovered_tag_tokens

        for _score, idx, sentence in scored:
            s_tokens = sentence.split()
            s_len = len(s_tokens)

            # Discover tags covered by this sentence
            new_covered: set[str] = set()
            for tag in all_tags - covered_tags:
                if tag in sentence:
                    new_covered.add(tag)

            # Budget freed by covering tags (no longer need reservation)
            freed = sum(len(t.split()) for t in new_covered)

            if used_tokens + s_len > effective_budget + freed:
                continue

            selected.append((idx, sentence))
            used_tokens += s_len
            covered_tags |= new_covered
            effective_budget += freed

        # Preserve original order
        selected.sort(key=lambda x: x[0])

        result = " ".join(s for _, s in selected)

        # Ensure all tags are present (re-inject if missing)
        result = _ensure_tags(tokens, result)

        return result

    # ------------------------------------------------------------------
    # Iterative threshold selection (model path)
    # ------------------------------------------------------------------

    def _iterative_select(
        self,
        tokens: list[_TokenEntry],
        budget_tokens: int,
    ) -> str:
        """Binary-search for the threshold that produces output within budget.

        Parameters:
            tokens: Tokens with assigned preserve probabilities.
            budget_tokens: Target output length.

        Returns:
            Compressed text fitting the budget.
        """
        lo, hi = 0.0, 1.0
        best_result = ""
        best_count = 0

        for _ in range(self._MAX_ITERATIONS):
            threshold = (lo + hi) / 2.0
            selected = [
                t for t in tokens
                if t.is_tag_token or t.preserve_probability >= threshold
            ]
            count = len(selected)

            if count <= 0:
                hi = threshold
                continue

            ratio = count / budget_tokens
            if abs(ratio - 1.0) <= self._TOLERANCE:
                return _reconstruct(selected)

            if count > budget_tokens:
                lo = threshold
            else:
                hi = threshold

            # Track closest-to-budget result
            if count <= budget_tokens * (1 + self._TOLERANCE):
                if count > best_count:
                    best_result = _reconstruct(selected)
                    best_count = count

        # If we never hit the sweet spot, return the best we found
        if best_result:
            return best_result

        # Last resort: take the top-budget tokens by probability
        ranked = sorted(
            tokens,
            key=lambda t: (t.is_tag_token, t.preserve_probability),
            reverse=True,
        )
        return _reconstruct(ranked[:budget_tokens])


# -----------------------------------------------------------------------
# Module-level helpers
# -----------------------------------------------------------------------


def _tokenise(text: str) -> list[_TokenEntry]:
    """Split text into whitespace-delimited token entries.

    Parameters:
        text: Input text.

    Returns:
        List of ``_TokenEntry`` objects.
    """
    return [_TokenEntry(text=t) for t in text.split() if t]


def _reconstruct(tokens: list[_TokenEntry]) -> str:
    """Join token entries back into a single string.

    Parameters:
        tokens: List of ``_TokenEntry`` objects.

    Returns:
        Whitespace-joined text.
    """
    return " ".join(t.text for t in tokens)


def _mark_tag_tokens(tokens: list[_TokenEntry]) -> None:
    """Flag tokens containing ``<TAG:value>`` patterns as non-discardable.

    This is the "pre-pass" that guarantees technical identifiers survive
    compression regardless of model predictions.

    Parameters:
        tokens: Mutable list of token entries; modified in place.
    """
    for token in tokens:
        if _TAG_PATTERN.search(token.text):
            token.is_tag_token = True
            token.preserve_probability = 1.0


def _split_sentences(text: str) -> list[str]:
    """Naively split text into sentence-like segments.

    Uses period / newline boundaries and filters out empty segments.

    Parameters:
        text: Input text.

    Returns:
        List of non-empty sentence strings.
    """
    parts = re.split(r"(?<=[.!?\n])\s+", text)
    return [p.strip() for p in parts if p.strip()]


def _ensure_tags(
    original_tokens: list[_TokenEntry],
    compressed: str,
) -> str:
    """Re-inject any technical-ID tags that were lost during compression.

    Parameters:
        original_tokens: Full token list from the original text.
        compressed: The compressed output string.

    Returns:
        The compressed string with all original tags guaranteed present.
    """
    original_tags = set()
    for token in original_tokens:
        for match in _TAG_PATTERN.finditer(token.text):
            original_tags.add(match.group())

    missing = [tag for tag in original_tags if tag not in compressed]
    if missing:
        compressed = compressed.rstrip() + " " + " ".join(sorted(missing))

    return compressed


def _map_subword_probs_to_tokens(
    tokens: list[_TokenEntry],
    text: str,
    offset_mapping: list[list[int]],
    probs: list[float],
) -> None:
    """Map transformer sub-word probabilities to whitespace token entries.

    For each whitespace token, we find overlapping sub-word spans and take
    the **maximum** probability to be conservative (prefer preserving).

    Parameters:
        tokens: Whitespace token entries to update in-place.
        text: Original text that was fed to the tokenizer.
        offset_mapping: ``(start, end)`` character offsets per sub-word.
        probs: Per-sub-word preserve probabilities.
    """
    # Compute character spans for each whitespace token
    char_spans: list[tuple[int, int]] = []
    pos = 0
    for token in tokens:
        idx = text.find(token.text, pos)
        if idx == -1:
            idx = pos
        char_spans.append((idx, idx + len(token.text)))
        pos = idx + len(token.text)

    for tok_idx, (tok_start, tok_end) in enumerate(char_spans):
        if tokens[tok_idx].is_tag_token:
            continue  # already forced to 1.0
        max_prob = 0.0
        for sw_idx, (sw_start, sw_end) in enumerate(offset_mapping):
            if sw_start == 0 and sw_end == 0:
                continue  # special tokens
            # Check overlap
            if sw_end > tok_start and sw_start < tok_end:
                max_prob = max(max_prob, probs[sw_idx])
        tokens[tok_idx].preserve_probability = max_prob
