from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException
import asyncio
from datetime import datetime, timezone
import httpx

from qdrant_client import AsyncQdrantClient, QdrantClient

from nexgen_shared.logging import configure_structlog, get_logger
from nexgen_shared.schemas import KnowledgeChunk, KnowledgeRequest, KnowledgeResult

from .authority import AuthorityScorer
from .connectors.local_file import LocalFileConnector
from .dense import DenseRetriever
from .fusion import WRRFFusion
from .ingest_service import (
    IngestRequest,
    IngestResponse,
    IngestService,
    OllamaEmbedder,
    UnsupportedSourceError,
)
from .preprocessor import Preprocessor
from .reranker import CrossEncoderReranker
from .settings import Settings
from .sparse import SparseRetriever


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Configure the RAG service application state for startup and shutdown."""

    settings = Settings()
    app.state.settings = settings

    configure_structlog(log_level=settings.log_level, json_format=False)
    app.state.log = get_logger(service="rag", query_id=None)
    app.state.log.info("startup", rag_port=settings.rag_port)
    
    # Synchronous client for ingest
    app.state.ingest_service = IngestService(
        qdrant_client=QdrantClient(url=settings.qdrant_url),
        connectors={"local_file": LocalFileConnector(settings.docs_path)},
        preprocessor=Preprocessor(),
        embedder=OllamaEmbedder(
            base_url=settings.llamacpp_embed_server_url,
            model=settings.embedding_model,
        ),
        dense_collection=settings.dense_collection,
        sparse_collection=settings.sparse_collection,
    )

    # Asynchronous client and components for retrieval
    async_qdrant_client = AsyncQdrantClient(url=settings.qdrant_url)
    httpx_client = httpx.AsyncClient()
    app.state.async_qdrant_client = async_qdrant_client
    app.state.httpx_client = httpx_client
    app.state.dense_retriever = DenseRetriever(async_qdrant_client, httpx_client, settings)
    app.state.sparse_retriever = SparseRetriever(async_qdrant_client, settings)
    app.state.fusion = WRRFFusion(settings)
    app.state.reranker = CrossEncoderReranker(settings)
    app.state.authority_scorer = AuthorityScorer()

    try:
        yield
    finally:
        await httpx_client.aclose()
        await async_qdrant_client.close()
        app.state.log.info("shutdown")


app = FastAPI(title="nexgen-rag", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, Any]:
    """Return a basic liveness response for the RAG service."""

    return {"status": "ok", "service": "rag"}


@app.post("/ingest", response_model=IngestResponse)
async def ingest(request: IngestRequest) -> IngestResponse:
    """Fetch source documents, preprocess them, and upsert them into Qdrant."""

    try:
        return await app.state.ingest_service.ingest(request)
    except UnsupportedSourceError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/knowledge", response_model=KnowledgeResult)
async def knowledge(request: KnowledgeRequest) -> KnowledgeResult:
    """Return knowledge payload populated from end-to-end RAG retrieval pipeline."""

    dense_retriever = app.state.dense_retriever
    sparse_retriever = app.state.sparse_retriever
    fusion = app.state.fusion
    reranker = app.state.reranker
    scorer = app.state.authority_scorer

    # 1. Parallel retrieval
    dense_chunks, sparse_chunks = await asyncio.gather(
        dense_retriever.retrieve(request),
        sparse_retriever.retrieve(request)
    )

    # 2. WRRF Fusion
    w_dense, w_sparse = fusion.classify_query(request.semantic_query)
    fused_chunks = fusion.fuse(dense_chunks, sparse_chunks, w_dense, w_sparse)

    # 3. Cross-Encoder Reranking (limit candidates for performance)
    candidate_chunks = fused_chunks[: request.max_chunks * 2]
    reranked_chunks = reranker.rerank(request.semantic_query, candidate_chunks)

    # 4. Authority Scoring
    scored_chunks = scorer.score(reranked_chunks)

    # 5. Final Top-K Slice
    top_chunks = scored_chunks[: request.max_chunks]

    # Construct result chunks
    retrieved_at = datetime.now(timezone.utc)
    knowledge_chunks = [
        KnowledgeChunk(
            chunk_id=c.chunk_id,
            source_type=c.metadata.source_type,
            source_uri=c.metadata.source_uri,
            authority_tier=c.metadata.authority_tier,
            recency_score=c.metadata.recency_score,
            content=c.content,
            retrieved_at=retrieved_at,
        )
        for c in top_chunks
    ]

    return KnowledgeResult(
        query_id=request.query_id,
        status="success",
        chunks=knowledge_chunks,
        total_tokens_after_compression=0,
        conflict_detected=False,
        error=None,
    )
