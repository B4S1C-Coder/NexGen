from __future__ import annotations

import math
from datetime import datetime, timezone

from qdrant_client.http import models

from nexgen_shared.schemas import KnowledgeRequest
from .preprocessor import RankedChunk


class TemporalFilter:
    """Filter to enforce as-of correctness and recency decay for retrieved knowledge."""

    def build_qdrant_filter(self, request: KnowledgeRequest) -> models.Filter:
        """Construct a Qdrant filter to exclude documents newer than the query time window.

        Parameters:
            request: The inbound KnowledgeRequest containing the temporal bound.

        Returns:
            A Qdrant Filter enforcing the not_after constraint.
        """
        # Qdrant datetime payloads are RFC3339 strings, we can filter using DatetimeRange
        return models.Filter(
            must=[
                models.FieldCondition(
                    key="created_at",
                    range=models.DatetimeRange(
                        lte=request.time_window.not_after.isoformat()
                    ),
                )
            ]
        )

    def apply_recency_decay(self, chunks: list[RankedChunk], lambda_: float = 0.02) -> list[RankedChunk]:
        """Apply exponential recency decay to raw chunk scores in place.

        Parameters:
            chunks: A list of RankedChunks.
            lambda_: The decay constant, defaults to 0.02 (soft decay).

        Returns:
            The mutated list of RankedChunks.
        """
        now_utc = datetime.now(timezone.utc)
        for chunk in chunks:
            # Calculate difference in days. Ensure chunk.metadata.created_at is timezone-aware.
            created_at = chunk.metadata.created_at
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=timezone.utc)
            
            delta = now_utc - created_at
            delta_days = max(0.0, delta.total_seconds() / 86400.0)
            
            # Apply decay: score_weighted = raw_score * exp(-λ * Δdays)
            chunk.score = chunk.score * math.exp(-lambda_ * delta_days)
            
        return chunks
