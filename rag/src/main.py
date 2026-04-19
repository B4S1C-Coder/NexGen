from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI

from nexgen_shared.logging import configure_structlog, get_logger
from nexgen_shared.schemas import KnowledgeRequest, KnowledgeResult

from .settings import Settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Configure the RAG service application state for startup and shutdown."""

    settings = Settings()
    app.state.settings = settings

    configure_structlog(log_level=settings.log_level, json_format=False)
    app.state.log = get_logger(service="rag", query_id=None)
    app.state.log.info("startup", rag_port=settings.rag_port)

    try:
        yield
    finally:
        app.state.log.info("shutdown")


app = FastAPI(title="nexgen-rag", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, Any]:
    """Return a basic liveness response for the RAG service."""

    return {"status": "ok", "service": "rag"}


@app.post("/ingest")
async def ingest() -> dict[str, str]:
    """Accept a stub ingest request and acknowledge it immediately."""

    return {"status": "accepted"}


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
