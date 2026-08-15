"""NLI-based conflict detection for retrieved knowledge chunks (rag.md §5.1).

Uses a cross-encoder NLI model to perform pairwise contradiction detection
across the top-k chunks returned by the authority scorer. When a pair of
chunks is classified as CONTRADICTION with confidence above the configured
threshold, a ConflictPair is emitted for downstream resolution by the
MultiAgentDebate module.
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass

from sentence_transformers import CrossEncoder

from .preprocessor import RankedChunk
from .settings import Settings


# Label indices emitted by NLI cross-encoder models following the standard
# ordering: 0 = contradiction, 1 = entailment, 2 = neutral.
_CONTRADICTION_LABEL = 0


@dataclass(slots=True)
class ConflictPair:
    """A pair of chunks whose content is semantically contradictory.

    Parameters:
        chunk_i: The first chunk in the conflicting pair.
        chunk_j: The second chunk in the conflicting pair.
        confidence: Model confidence that the pair is contradictory (0–1).
        label: Human-readable NLI label (always ``"contradiction"``).
    """

    chunk_i: RankedChunk
    chunk_j: RankedChunk
    confidence: float
    label: str = "contradiction"


class ConflictDetector:
    """Detects semantic contradictions between retrieved chunks using NLI.

    Loads ``cross-encoder/nli-deberta-v3-small`` (configurable via settings)
    and classifies every unique pair of chunks. Pairs labelled CONTRADICTION
    with confidence above ``conflict_confidence_threshold`` are returned as
    ``ConflictPair`` instances.

    Parameters:
        settings: RAG service settings providing model name and threshold.
    """

    def __init__(self, settings: Settings) -> None:
        self._model = CrossEncoder(settings.nli_model, max_length=512)
        self._threshold = settings.conflict_confidence_threshold

    def detect_conflicts(self, chunks: list[RankedChunk]) -> list[ConflictPair]:
        """Run pairwise NLI over chunks and collect contradictions.

        Parameters:
            chunks: Ranked chunks from the authority scorer (top-k).

        Returns:
            A list of ``ConflictPair`` instances for every pair classified as
            CONTRADICTION with confidence above the configured threshold.
            Returns an empty list when fewer than 2 chunks are provided.
        """
        if len(chunks) < 2:
            return []

        pairs: list[tuple[RankedChunk, RankedChunk]] = list(
            itertools.combinations(chunks, 2)
        )

        # Build sentence pairs for batch prediction
        sentence_pairs: list[list[str]] = [
            [pair[0].content, pair[1].content] for pair in pairs
        ]

        # Predict returns an array of shape (n_pairs, 3) with softmax scores
        # for [contradiction, entailment, neutral].
        scores = self._model.predict(sentence_pairs, apply_softmax=True)

        conflicts: list[ConflictPair] = []
        for (chunk_i, chunk_j), score_row in zip(pairs, scores, strict=True):
            contradiction_confidence = float(score_row[_CONTRADICTION_LABEL])
            if contradiction_confidence > self._threshold:
                conflicts.append(
                    ConflictPair(
                        chunk_i=chunk_i,
                        chunk_j=chunk_j,
                        confidence=contradiction_confidence,
                    )
                )

        return conflicts
