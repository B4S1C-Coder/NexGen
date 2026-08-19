from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException

from nexgen_shared.logging import configure_structlog, get_logger
from nexgen_shared.schemas import RCAReport, UserQuery
from .orchestrator import MasterOrchestrator
from .settings import Settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start the orchestrator and structured logging for the Master service."""
    settings = Settings()
    app.state.settings = settings

    configure_structlog(
        log_level=settings.log_level,
        json_format=False
    )

    app.state.log = get_logger(service="master", query_id=None)
    app.state.orchestrator = MasterOrchestrator(settings=settings)
    app.state.log.info("startup", master_port=settings.master_port)

    try:
        yield
    finally:
        app.state.log.info("shutdown")


app = FastAPI(title="nexgen-master", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, Any]:
    """Liveness probe used by the TUI and compose healthchecks."""
    return {"status": "ok", "service": "master"}


@app.get("/status")
async def status() -> dict[str, Any]:
    """Report whether the orchestrator is bound and downstream URLs are set."""
    settings: Settings = app.state.settings
    orchestrator = getattr(app.state, "orchestrator", None)
    return {
        "status": "ok",
        "service": "master",
        "orchestrator": orchestrator is not None,
        "query_service": settings.query_service,
        "rag_service": settings.rag_service_url,
    }


@app.get("/session/{session_id}")
async def get_session(session_id: str) -> dict[str, Any]:
    """Return stored session history for a TUI or operator follow-up."""
    orchestrator: MasterOrchestrator = app.state.orchestrator
    state = await orchestrator.session_manager.get(session_id)
    if not state.query_history and not state.active_context_window:
        raise HTTPException(status_code=404, detail="session not found")
    return {
        "session_id": state.session_id,
        "history": [
            {"query_id": q.query_id, "raw_text": q.raw_text}
            for q in state.query_history
        ],
        "turns": len(state.active_context_window),
    }


@app.get("/query/{query_id}/trace")
async def query_trace(query_id: str) -> dict[str, Any]:
    """Return live DAG stage events recorded for ``query_id``."""
    orchestrator: MasterOrchestrator = app.state.orchestrator
    return {"query_id": query_id, "events": orchestrator.traces.get(query_id, [])}


@app.post("/query", response_model=RCAReport)
async def query(user_query: UserQuery) -> RCAReport:
    """Run the full Master DAG and return an RCA report."""
    orchestrator: MasterOrchestrator = app.state.orchestrator
    log = get_logger(service="master", query_id=user_query.query_id)
    log.info("query_accepted", text=user_query.raw_text[:120])
    return await orchestrator.execute_query(user_query)
