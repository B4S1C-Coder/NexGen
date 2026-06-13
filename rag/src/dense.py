from __future__ import annotations

from typing import Any

import httpx
from qdrant_client import AsyncQdrantClient

from nexgen_shared.schemas import KnowledgeRequest
from src.preprocessor import RankedChunk, ChunkMetadata
from src.settings import Settings
from src.temporal import TemporalFilter


class DenseRetriever:
    """Retrieves context chunks using dense vector embeddings via Qdrant."""

    def __init__(
        self,
        qdrant_client: AsyncQdrantClient,
        http_client: httpx.AsyncClient,
        settings: Settings,
    ) -> None:
        self.qdrant_client = qdrant_client
        self.http_client = http_client
        self.settings = settings
        self.temporal_filter = TemporalFilter()

    async def retrieve(self, request: KnowledgeRequest) -> list[RankedChunk]:
        """Embeds the query and fetches top semantically matching chunks from Qdrant.

        Parameters:
            request: The inbound KnowledgeRequest with semantic_query and constraints.

        Returns:
            A list of RankedChunk objects representing the search hits.
        """
        # 1. Embed query via Ollama/llama.cpp
        embed_url = f"{self.settings.llamacpp_embed_server_url.rstrip('/')}/embedding"
        payload = {"content": request.semantic_query}
        response = await self.http_client.post(embed_url, json=payload)
        response.raise_for_status()
        
        # llama.cpp server typically returns {"embedding": [float, ...]}
        resp_data = response.json()
        query_vector = resp_data.get("embedding", [])
        if not query_vector:
            # Fallback or error handling if empty
            return []

        # 2. Query nexgen_dense with Qdrant filter from TemporalFilter
        qdrant_filter = self.temporal_filter.build_qdrant_filter(request)
        limit = request.max_chunks * 3

        search_results = await self.qdrant_client.search(
            collection_name=self.settings.dense_collection,
            query_vector=query_vector,
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
                created_at=payload_data.get("created_at"),  # This is usually parsed or needs to be a datetime object
                resolution_status=payload_data.get("resolution_status", "unknown"),
                is_accepted_answer=payload_data.get("is_accepted_answer", False),
                recency_score=payload_data.get("recency_score", 1.0),
            )
            
            # Note: Depending on Qdrant, created_at might be a string, if so, parse it
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
