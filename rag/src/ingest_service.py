from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from uuid import NAMESPACE_URL, uuid5

import httpx
from pydantic import BaseModel, ConfigDict
from qdrant_client import QdrantClient, models

from .connectors.base import BaseConnector, ensure_utc
from .preprocessor import Preprocessor
from .qdrant_setup import DENSE_VECTOR_SIZE, SPARSE_VECTOR_NAME, ensure_qdrant_collections


class IngestRequest(BaseModel):
    """Request body for the RAG ingestion endpoint."""

    model_config = ConfigDict(extra="forbid")

    source_type: str
    full_reindex: bool = False


class IngestResponse(BaseModel):
    """Summary of one ingestion run."""

    model_config = ConfigDict(extra="forbid")

    status: str
    source_type: str
    documents_indexed: int
    chunks_indexed: int


class Embedder(Protocol):
    """Protocol implemented by embedding backends used during ingestion."""

    async def embed_texts(self, texts: list[str]) -> list[list[float]]:
        """Return one dense vector per input text."""


class UnsupportedSourceError(ValueError):
    """Raised when an ingest request references an unknown connector."""


class OllamaEmbedder:
    """Embed text using an Ollama-compatible embeddings endpoint."""

    def __init__(self, base_url: str, model: str) -> None:
        """Create an embedder for the configured model endpoint.

        Parameters:
            base_url: Base URL of the embeddings service.
            model: Embedding model identifier.
        """

        self._base_url = base_url.rstrip("/")
        self._model = model

    async def embed_texts(self, texts: list[str]) -> list[list[float]]:
        """Embed all texts sequentially using the configured HTTP endpoint.

        Parameters:
            texts: Input strings to embed.

        Returns:
            Dense vectors aligned with ``texts``.
        """

        async with httpx.AsyncClient(timeout=30.0) as client:
            vectors: list[list[float]] = []
            for text in texts:
                response = await client.post(
                    f"{self._base_url}/api/embeddings",
                    json={"model": self._model, "prompt": text},
                )
                response.raise_for_status()
                payload = response.json()
                if "embedding" in payload:
                    vectors.append(payload["embedding"])
                else:
                    vectors.append(payload["data"][0]["embedding"])
        return vectors


@dataclass(slots=True)
class SparseEncoder:
    """Generate simple sparse term vectors for BM25-style retrieval."""

    token_pattern: re.Pattern[str] = re.compile(r"[A-Za-z0-9_./:-]+")

    def encode(self, text: str) -> models.SparseVector:
        """Convert text into a deterministic sparse vector.

        Parameters:
            text: Input text to tokenize.

        Returns:
            A Qdrant sparse vector keyed by hashed token indices.
        """

        weights: dict[int, float] = {}
        tokens = [token.lower() for token in self.token_pattern.findall(text)]
        if not tokens:
            return models.SparseVector(indices=[], values=[])

        max_frequency = 1
        counts: dict[str, int] = {}
        for token in tokens:
            counts[token] = counts.get(token, 0) + 1
            max_frequency = max(max_frequency, counts[token])

        for token, count in counts.items():
            index = int.from_bytes(hashlib.sha1(token.encode("utf-8")).digest()[:4], "big")
            weights[index] = count / max_frequency

        indices = sorted(weights)
        return models.SparseVector(indices=indices, values=[weights[index] for index in indices])


class IngestService:
    """Coordinate connector fetching, preprocessing, embedding, and Qdrant upserts."""

    def __init__(
        self,
        *,
        qdrant_client: QdrantClient,
        connectors: dict[str, BaseConnector],
        preprocessor: Preprocessor,
        embedder: Embedder,
        dense_collection: str,
        sparse_collection: str,
    ) -> None:
        """Create an ingestion coordinator for the configured connectors.

        Parameters:
            qdrant_client: Qdrant client used for storage.
            connectors: Connector registry keyed by request source type.
            preprocessor: Preprocessor used to chunk and annotate documents.
            embedder: Dense embedding backend.
            dense_collection: Qdrant collection name for dense vectors.
            sparse_collection: Qdrant collection name for sparse vectors.
        """

        self._qdrant_client = qdrant_client
        self._connectors = connectors
        self._preprocessor = preprocessor
        self._embedder = embedder
        self._dense_collection = dense_collection
        self._sparse_collection = sparse_collection
        self._sparse_encoder = SparseEncoder()
        self._last_indexed_at: dict[str, datetime] = {}

    async def ingest(self, request: IngestRequest) -> IngestResponse:
        """Run one ingestion cycle for the requested source type.

        Parameters:
            request: Ingestion request describing the target connector.

        Returns:
            A summary containing indexed document and chunk counts.

        Raises:
            UnsupportedSourceError: If no connector exists for the requested source type.
        """

        connector = self._connectors.get(request.source_type)
        if connector is None:
            raise UnsupportedSourceError(f"Unsupported source_type: {request.source_type}")

        ensure_qdrant_collections(
            self._qdrant_client,
            dense_collection=self._dense_collection,
            sparse_collection=self._sparse_collection,
        )

        since = None if request.full_reindex else self._last_indexed_at.get(request.source_type)
        documents = await connector.fetch(since)

        chunks = []
        payloads = []
        latest_update: datetime | None = None

        for document in documents:
            latest_update = max(
                latest_update or ensure_utc(document.updated_at),
                ensure_utc(document.updated_at),
            )
            for chunk in self._preprocessor.chunk(document):
                metadata = self._preprocessor.enrich_metadata(chunk, document)
                chunks.append(chunk)
                payloads.append(
                    {
                        "chunk_id": chunk.chunk_id,
                        "doc_id": chunk.doc_id,
                        "content": chunk.content,
                        "source_type": metadata.source_type,
                        "source_uri": metadata.source_uri,
                        "authority_tier": metadata.authority_tier,
                        "created_at": metadata.created_at.isoformat(),
                        "resolution_status": metadata.resolution_status,
                        "is_accepted_answer": metadata.is_accepted_answer,
                        "recency_score": metadata.recency_score,
                    }
                )

        if chunks:
            vectors = await self._embedder.embed_texts([chunk.content for chunk in chunks])
            dense_points = [
                models.PointStruct(
                    id=self._point_id(chunk.chunk_id),
                    vector=vector,
                    payload=payload,
                )
                for chunk, vector, payload in zip(chunks, vectors, payloads, strict=True)
            ]
            sparse_points = [
                models.PointStruct(
                    id=self._point_id(chunk.chunk_id),
                    vector={SPARSE_VECTOR_NAME: self._sparse_encoder.encode(chunk.content)},
                    payload=payload,
                )
                for chunk, payload in zip(chunks, payloads, strict=True)
            ]

            self._qdrant_client.upsert(
                collection_name=self._dense_collection,
                points=dense_points,
                wait=True,
            )
            self._qdrant_client.upsert(
                collection_name=self._sparse_collection,
                points=sparse_points,
                wait=True,
            )

        if latest_update is not None:
            self._last_indexed_at[request.source_type] = latest_update

        return IngestResponse(
            status="success",
            source_type=request.source_type,
            documents_indexed=len(documents),
            chunks_indexed=len(chunks),
        )

    def _point_id(self, chunk_id: str) -> str:
        """Convert a stable chunk identifier into a deterministic UUID string."""

        return str(uuid5(NAMESPACE_URL, chunk_id))


def deterministic_test_vector(text: str, size: int = DENSE_VECTOR_SIZE) -> list[float]:
    """Build a deterministic dense vector for tests from arbitrary text."""

    digest = hashlib.sha256(text.encode("utf-8")).digest()
    values: list[float] = []
    while len(values) < size:
        for byte in digest:
            values.append(byte / 255.0)
            if len(values) == size:
                break
    return values
