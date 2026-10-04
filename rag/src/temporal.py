from __future__ import annotations


from qdrant_client.http import models

from nexgen_shared.schemas import KnowledgeRequest


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
