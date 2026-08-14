from src.preprocessor import RankedChunk


class AuthorityScorer:
    """Applies business-logic boosts and penalties to semantic search results."""

    def score(self, chunks: list[RankedChunk]) -> list[RankedChunk]:
        """Apply tier boosts, resolution boosts, and recency to the base scores.

        Parameters:
            chunks: List of chunks previously ranked by fusion/cross-encoder.

        Returns:
            List of chunks with updated `score` attributes, sorted descending.
        """
        scored_chunks: list[RankedChunk] = []

        for chunk in chunks:
            # Base score from Cross-Encoder if available, otherwise fallback to standard WRRF score
            base = chunk.cross_encoder_score if chunk.cross_encoder_score is not None else chunk.score

            tier_boost = 1.25 if chunk.metadata.authority_tier == "A" else 1.0
            resolution_boost = 1.15 if chunk.metadata.is_accepted_answer else 1.0
            deprecated_penalty = 0.3 if chunk.metadata.resolution_status == "deprecated" else 1.0
            recency = chunk.metadata.recency_score

            final_score = base * tier_boost * resolution_boost * deprecated_penalty * recency

            # Create a new RankedChunk to avoid mutation
            new_chunk = RankedChunk(
                chunk_id=chunk.chunk_id,
                content=chunk.content,
                metadata=chunk.metadata,
                score=final_score,
                cross_encoder_score=chunk.cross_encoder_score,
            )
            scored_chunks.append(new_chunk)

        # Sort descending by the new final score
        scored_chunks.sort(key=lambda c: c.score, reverse=True)
        return scored_chunks
