"""LLMLingua-2 Compactor for context compression (rag.md §5.3).

Compresses retrieved knowledge chunks into a token-budgeted prompt, preserving
essential information. Implements a fallback mode (extractive sentence selection)
when the LLMLingua-2 model is unavailable, to allow tests to run without
downloading large weights.
"""

from __future__ import annotations

import logging
import re
from typing import Any

# Optional import to allow fallback mode without model download in tests
try:
    from llmlingua import PromptCompressor
    HAS_LLMLINGUA = True
except ImportError:
    HAS_LLMLINGUA = False


from .settings import Settings

logger = logging.getLogger(__name__)


class LLMLingua2Compactor:
    """Compresses a list of chunks into a budgeted text string.

    Uses microsoft/llmlingua-2-bert-base-multilingual-cased-meetingbank by
    default. Extracts technical tags (`<TAG:value>`) as always-preserve targets
    during compression.

    If LLMLingua is missing or fails to load, falls back to extractive compression.

    Parameters:
        settings: RAG service settings providing model name and budget.
    """

    def __init__(self, settings: Settings) -> None:
        self._model_name = settings.llmlingua2_model
        self._budget = settings.default_compression_budget_tokens
        self._compressor: Any = None
        self._tag_pattern = re.compile(r"<([A-Z_]+):([^>]+)>")

        if HAS_LLMLINGUA:
            try:
                # LLMLingua-2 uses model_name for the underlying model
                # and use_llmlingua2=True to activate the newer algorithm
                self._compressor = PromptCompressor(
                    model_name=self._model_name,
                    use_llmlingua2=True,
                )
            except Exception as exc:
                logger.warning(
                    f"Failed to load LLMLingua-2 model ({exc}). "
                    "Falling back to extractive mode."
                )

    def compress(self, chunks: list[str], budget_tokens: int | None = None) -> str:
        """Compress chunk content into a budgeted string.

        Parameters:
            chunks: A list of raw chunk texts to compress.
            budget_tokens: Optional token budget (overrides default).

        Returns:
            The compressed text string.
        """
        if not chunks:
            return ""

        target_budget = budget_tokens or self._budget
        full_text = "\n\n".join(chunks)
        
        # Approximate token count (split by whitespace)
        word_count = len(full_text.split())
        if word_count <= target_budget:
            return full_text

        # 1. Pre-pass: Identify technical tags to preserve
        # LLMLingua allows passing a list of strings to preserve via `force_tokens`
        preserve_tokens: list[str] = []
        for match in self._tag_pattern.finditer(full_text):
            preserve_tokens.append(match.group(0))

        # Deduplicate
        preserve_tokens = list(set(preserve_tokens))

        if self._compressor is not None:
            try:
                # Calculate required compression ratio
                ratio = target_budget / max(1.0, float(word_count))
                
                # LLMLingua-2 compress_prompt API
                result = self._compressor.compress_prompt(
                    full_text,
                    rate=ratio,
                    force_tokens=preserve_tokens,
                )
                return result.get("compressed_prompt", "")
            except Exception as exc:
                logger.error(f"LLMLingua compression failed ({exc}). Using fallback.")
                return self._fallback_compress(full_text, target_budget, preserve_tokens)
        else:
            return self._fallback_compress(full_text, target_budget, preserve_tokens)

    def _fallback_compress(self, text: str, budget: int, preserve: list[str]) -> str:
        """Extractive fallback: keep sentences with tags, then take head until budget."""
        sentences = [s.strip() for s in text.replace("\n", ". ").split(". ") if s.strip()]
        
        selected: list[str] = []
        current_tokens = 0
        
        # Priority 1: Sentences containing preserved tags
        for sentence in sentences:
            if any(token in sentence for token in preserve):
                tok_len = len(sentence.split())
                if current_tokens + tok_len <= budget:
                    selected.append(sentence)
                    current_tokens += tok_len
                    
        # Priority 2: Fill remaining budget with starting sentences
        for sentence in sentences:
            if sentence not in selected:
                tok_len = len(sentence.split())
                if current_tokens + tok_len <= budget:
                    selected.append(sentence)
                    current_tokens += tok_len
                else:
                    break
                    
        return ". ".join(selected) + ("." if selected else "")
