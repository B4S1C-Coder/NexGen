from __future__ import annotations

from qdrant_client import AsyncQdrantClient
from qdrant_client.http import models

from nexgen_shared.schemas import KnowledgeRequest
from src.ingest_service import SparseEncoder
from src.preprocessor import ChunkMetadata, RankedChunk
from src.qdrant_setup import SPARSE_VECTOR_NAME
from src.settings import Settings
from src.temporal import TemporalFilter


class SparseRetriever:
    """Retrieves context chunks using sparse BM25 vectors via Qdrant."""

    def __init__(self, qdrant_client: AsyncQdrantClient, settings: Settings) -> None:
        self.qdrant_client = qdrant_client
        self.settings = settings
        self.sparse_encoder = SparseEncoder()
        self.temporal_filter = TemporalFilter()

    async def retrieve(self, request: KnowledgeRequest) -> list[RankedChunk]:
        """Encodes the query into a sparse vector and fetches matching chunks from Qdrant.

        Parameters:
            request: The inbound KnowledgeRequest with semantic_query and constraints.

        Returns:
            A list of RankedChunk objects representing the search hits.
        """
        # 1. Encode query to sparse vector
        sparse_vector = self.sparse_encoder.encode(request.semantic_query)
        if not sparse_vector.indices:
            # Empty sparse vector means no alphanumeric tokens
            return []

        # 2. Query nexgen_bm25_terms with Qdrant filter from TemporalFilter
        qdrant_filter = self.temporal_filter.build_qdrant_filter(request)
        limit = request.max_chunks * 3

        search_results = await self.qdrant_client.search(
            collection_name=self.settings.sparse_collection,
            query_vector=models.NamedSparseVector(
                name=SPARSE_VECTOR_NAME,
                vector=sparse_vector,
            ),
            query_filter=qdrant_filter,
            limit=limit,
            with_payload=True,
        )

        # 3. Process into RankedChunk list
        chunks: list[RankedChunk] = []
        for point in search_results:
            payload_data = point.payload or {}
            
            # Reconstruct ChunkMetadata from payload
            metadata = ChunkMetadata(
                chunk_id=payload_data.get("chunk_id", ""),
                doc_id=payload_data.get("doc_id", ""),
                source_type=payload_data.get("source_type", ""),
                source_uri=payload_data.get("source_uri", ""),
                authority_tier=payload_data.get("authority_tier", "B"),
                created_at=payload_data.get("created_at"),
                resolution_status=payload_data.get("resolution_status", "unknown"),
                is_accepted_answer=payload_data.get("is_accepted_answer", False),
                recency_score=payload_data.get("recency_score", 1.0),
            )

            # Ensure created_at is a datetime object
            from datetime import datetime
            if isinstance(metadata.created_at, str):
                try:
                    metadata.created_at = datetime.fromisoformat(metadata.created_at)
                except ValueError:
                    pass

            chunk = RankedChunk(
                chunk_id=metadata.chunk_id,
                content=payload_data.get("content", ""),
                metadata=metadata,
                score=point.score,
            )
            chunks.append(chunk)

        return chunks
