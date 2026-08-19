"""LLMLingua-2 style context compactor for the RAG pipeline."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any

try:
    from llmlingua import PromptCompressor
    HAS_LLMLINGUA = True
except ImportError:  # pragma: no cover
    PromptCompressor = None  # type: ignore[assignment]
    HAS_LLMLINGUA = False

from .settings import Settings

logger = logging.getLogger(__name__)
_TAG_PATTERN = re.compile(r"<(?:IP_ADDR|TRACE_ID|HASH|PATH|ERROR_CODE):[^>]+>")


@dataclass(slots=True)
class _TokenEntry:
    text: str
    preserve_probability: float = 0.5
    is_tag_token: bool = False


def _tokenise(text: str) -> list[_TokenEntry]:
    """Tokenise text on whitespace while preserving token text verbatim."""
    return [_TokenEntry(text=token) for token in text.split() if token]


def _reconstruct(tokens: list[_TokenEntry]) -> str:
    """Rebuild a token sequence as a single space-separated string."""
    return " ".join(token.text for token in tokens)


def _mark_tag_tokens(tokens: list[_TokenEntry]) -> None:
    """Force technical-ID tokens to stay in the compressed output."""
    for token in tokens:
        if _TAG_PATTERN.search(token.text):
            token.is_tag_token = True
            token.preserve_probability = 1.0


class LLMLingua2Compactor:
    """Compress chunks into a bounded prompt while preserving IDs.

    Args:
        settings: Optional RAG settings object. When omitted, defaults are read
            from environment.
        model_name: Optional explicit LLMLingua model identifier.
    """

    def __init__(self, settings: Settings | None = None, model_name: str | None = None) -> None:
        self._settings = settings or Settings()
        self._model_name = model_name or self._settings.llmlingua2_model
        self._budget = self._settings.default_compression_budget_tokens
        self._compressor: Any = None
        if HAS_LLMLINGUA:
            try:
                self._compressor = PromptCompressor(
                    model_name=self._model_name,
                    use_llmlingua2=True,
                )
            except Exception as exc:  # pragma: no cover
                logger.warning("Failed to load LLMLingua-2 model: %s", exc)
                self._compressor = None

    def compress(self, chunks: list[str], budget_tokens: int | None = None) -> str:
        """Compress chunk text within a token budget."""
        if not chunks or not any(chunk.strip() for chunk in chunks):
            return ""
        budget = budget_tokens or self._budget
        full_text = "\n\n".join(chunks)
        full_tokens = _tokenise(full_text)
        if len(full_tokens) <= budget:
            return full_text
        if self._compressor is not None:
            tags = _collect_tags(chunks)
            rate = min(1.0, max(0.05, budget / max(len(full_tokens), 1)))
            payload = self._compressor.compress_prompt(
                full_text,
                rate=rate,
                force_tokens=tags,
            )
            compressed = payload.get("compressed_prompt", "")
            if compressed:
                return compressed
        return self._extractive_fallback(chunks, budget)

    def _extractive_fallback(self, chunks: list[str], budget_tokens: int) -> str:
        """Select tag-rich sentences first, then fill remaining space."""
        sentences: list[str] = []
        for chunk in chunks:
            parts = [
                part.strip()
                for part in re.split(r"(?<=[.!?])\s+|\n+", chunk)
                if part.strip()
            ]
            sentences.extend(parts)
        if not sentences:
            return ""

        selected: list[str] = []
        used = 0
        seen: set[str] = set()

        def add_sentence(sentence: str) -> None:
            nonlocal used
            if sentence in seen:
                return
            words = sentence.split()
            if used + len(words) <= budget_tokens:
                selected.append(sentence)
                seen.add(sentence)
                used += len(words)

        for sentence in sentences:
            if _TAG_PATTERN.search(sentence):
                add_sentence(sentence)

        for sentence in sentences:
            add_sentence(sentence)
            if used >= budget_tokens:
                break

        text = " ".join(selected).strip()
        if not text:
            words = " ".join(chunks).split()[:budget_tokens]
            text = " ".join(words)
        words = text.split()
        if len(words) > budget_tokens:
            text = " ".join(words[:budget_tokens])
        return text


def _collect_tags(chunks: list[str]) -> list[str]:
    """Extract unique technical-ID tags from the original chunk text."""
    seen: set[str] = set()
    tags: list[str] = []
    for chunk in chunks:
        for match in _TAG_PATTERN.finditer(chunk):
            tag = match.group(0)
            if tag not in seen:
                seen.add(tag)
                tags.append(tag)
    return tags
