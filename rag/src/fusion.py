import re

from src.preprocessor import RankedChunk
from src.settings import Settings


class WRRFFusion:
    """Performs Weighted Reciprocal Rank Fusion (WRRF) on dense and sparse retrieval results."""

    def __init__(self, settings: Settings) -> None:
        self.k = settings.wrrf_k

    def classify_query(self, query: str) -> tuple[float, float]:
        """Classify query to determine optimal dense vs. sparse weights.

        Parameters:
            query: Semantic query text.

        Returns:
            Tuple of (w_dense, w_sparse).
        """
        # Rule-based detection for technical queries (e.g. error codes, trace IDs)
        has_error_code = bool(re.search(r"\b(?:ERR|E)[A-Z0-9_]{2,}\b", query, re.IGNORECASE))
        has_trace_id = bool(re.search(r"\b[0-9a-f]{8,}\b", query, re.IGNORECASE))
        has_path = bool(re.search(r"(?<!<)(?:/[^\s<>]+)+", query))
        
        # Determine classification
        if has_error_code or has_trace_id or has_path:
            return 0.3, 0.7  # Technical/Error-heavy query
        
        # Check if mostly natural language
        words = query.split()
        if len(words) > 3 and not any(char in query for char in "{}[]_\\"):
            return 0.7, 0.3  # Mostly natural language
            
        return 0.5, 0.5  # Mixed

    def fuse(
        self,
        dense: list[RankedChunk],
        sparse: list[RankedChunk],
        w_dense: float,
        w_sparse: float,
    ) -> list[RankedChunk]:
        """Fuse dense and sparse chunks using WRRF.

        Parameters:
            dense: Chunks retrieved from dense index.
            sparse: Chunks retrieved from sparse index.
            w_dense: Weight assigned to dense ranking.
            w_sparse: Weight assigned to sparse ranking.

        Returns:
            A deduplicated list of RankedChunk objects sorted descending by WRRF score.
        """
        chunk_map: dict[str, RankedChunk] = {}
        dense_ranks: dict[str, int] = {}
        sparse_ranks: dict[str, int] = {}

        # 1. Register ranks and collect unique chunks
        for idx, chunk in enumerate(dense):
            chunk_map[chunk.chunk_id] = chunk
            dense_ranks[chunk.chunk_id] = idx + 1
            
        for idx, chunk in enumerate(sparse):
            if chunk.chunk_id not in chunk_map:
                chunk_map[chunk.chunk_id] = chunk
            sparse_ranks[chunk.chunk_id] = idx + 1

        # 2. Compute WRRF for each chunk
        fused_chunks: list[RankedChunk] = []
        for chunk_id, chunk in chunk_map.items():
            dense_rank = dense_ranks.get(chunk_id)
            sparse_rank = sparse_ranks.get(chunk_id)

            dense_score = w_dense * (1.0 / (self.k + dense_rank)) if dense_rank else 0.0
            sparse_score = w_sparse * (1.0 / (self.k + sparse_rank)) if sparse_rank else 0.0

            wrrf_score = dense_score + sparse_score
            
            # Create a new RankedChunk with updated score to avoid mutating original objects
            fused_chunk = RankedChunk(
                chunk_id=chunk.chunk_id,
                content=chunk.content,
                metadata=chunk.metadata,
                score=wrrf_score,
                cross_encoder_score=chunk.cross_encoder_score,
            )
            fused_chunks.append(fused_chunk)

        # 3. Sort descending by new WRRF score
        fused_chunks.sort(key=lambda c: c.score, reverse=True)
        return fused_chunks
