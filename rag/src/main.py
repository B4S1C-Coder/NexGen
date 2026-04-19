from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException
from qdrant_client import QdrantClient

from nexgen_shared.logging import configure_structlog, get_logger
from nexgen_shared.schemas import KnowledgeRequest, KnowledgeResult

from .connectors.local_file import LocalFileConnector
from .ingest_service import (
    IngestRequest,
    IngestResponse,
    IngestService,
    OllamaEmbedder,
    UnsupportedSourceError,
)
from .preprocessor import Preprocessor
from .settings import Settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Configure the RAG service application state for startup and shutdown."""

    settings = Settings()
    app.state.settings = settings

    configure_structlog(log_level=settings.log_level, json_format=False)
    app.state.log = get_logger(service="rag", query_id=None)
    app.state.log.info("startup", rag_port=settings.rag_port)
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

    try:
        yield
    finally:
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
    """Return an empty successful knowledge payload for Phase 0 scaffolding."""

    return KnowledgeResult(
        query_id=request.query_id,
        status="success",
        chunks=[],
        total_tokens_after_compression=0,
        conflict_detected=False,
        error=None,
    )
