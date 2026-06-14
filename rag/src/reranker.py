from sentence_transformers import CrossEncoder

from src.preprocessor import RankedChunk
from src.settings import Settings


class CrossEncoderReranker:
    """Reranks retrieved chunks using a cross-encoder model for semantic relevance."""

    def __init__(self, settings: Settings) -> None:
        # Load the model specified in the configuration
        self.model = CrossEncoder(settings.cross_encoder_model, max_length=512)

    def rerank(self, query: str, chunks: list[RankedChunk]) -> list[RankedChunk]:
        """Score each chunk against the query using the cross-encoder.

        Parameters:
            query: The original semantic query.
            chunks: A list of RankedChunk objects from the fusion step.

        Returns:
            A new list of RankedChunk objects sorted descending by cross_encoder_score.
        """
        if not chunks:
            return []

        # Prepare pairs: [(query, chunk1), (query, chunk2), ...]
        pairs = [[query, chunk.content] for chunk in chunks]

        # Predict scores
        scores = self.model.predict(pairs)

        reranked_chunks: list[RankedChunk] = []
        for chunk, score in zip(chunks, scores, strict=True):
            # Create a new RankedChunk to avoid mutating the original
            reranked_chunk = RankedChunk(
                chunk_id=chunk.chunk_id,
                content=chunk.content,
                metadata=chunk.metadata,
                score=chunk.score,  # Retain WRRF score
                cross_encoder_score=float(score),
            )
            reranked_chunks.append(reranked_chunk)

        # Sort descending by cross-encoder score
        reranked_chunks.sort(key=lambda c: c.cross_encoder_score or 0.0, reverse=True)
        return reranked_chunks
