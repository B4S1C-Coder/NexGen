"""FastAPI entry point for the Master service (port 8000)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Request

from nexgen_shared.logging import configure_structlog, get_logger
from nexgen_shared.schemas import RCAReport, UserQuery

from src.orchestrator import MasterOrchestrator
from src.settings import Settings


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Build one orchestrator at startup and share it across requests."""
    settings = Settings()
    configure_structlog(log_level=settings.log_level)
    app.state.orchestrator = MasterOrchestrator(settings)
    get_logger(service="master").info(
        "startup", mock_services=settings.mock_services, llm_enabled=app.state.orchestrator.llm_enabled
    )
    yield


app = FastAPI(title="nexgen-master", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, Any]:
    """Liveness check."""
    return {"status": "ok", "service": "master"}


@app.post("/query", response_model=RCAReport)
async def query(user_query: UserQuery, request: Request) -> RCAReport:
    """Answer a question with a root-cause analysis report."""
    orchestrator: MasterOrchestrator = request.app.state.orchestrator
    return await orchestrator.execute_query(user_query)


@app.get("/session/{session_id}")
async def get_session(session_id: str, request: Request) -> dict[str, Any]:
    """Return the stored questions and messages of a session, or 404."""
    orchestrator: MasterOrchestrator = request.app.state.orchestrator
    state = await orchestrator.sessions.get(session_id)
    if state is None:
        raise HTTPException(status_code=404, detail=f"session {session_id} not found")
    return state.model_dump(mode="json")
