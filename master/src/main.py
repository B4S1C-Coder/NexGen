from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

import httpx
from fastapi import FastAPI

from nexgen_shared.logging import configure_structlog, get_logger
from nexgen_shared.schemas import RCAEvidenceItem, RCAReport, UserQuery
from .settings import Settings

@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = Settings()
    app.state.settings = settings

    configure_structlog(
        log_level=settings.log_level,
        json_format=False
    )

    app.state.log = get_logger(service="master", query_id=None)
    app.state.log.info("startup", master_port=settings.master_port)
    app.state.http = httpx.AsyncClient(timeout=settings.http_timeout_seconds)

    try:
        yield
    finally:
        await app.state.http.aclose()
        app.state.log.info("shutdown")

app = FastAPI(title="nexgen-master", lifespan=lifespan)

@app.get("/health")
async def health() -> dict[str, Any]:
    return { "status": "ok", "service": "master" }

@app.get("/session/{session_id}")
async def get_session(session_id: str) -> dict[str, Any]:
    # Redis backed manager would come here
    return { "session_id": session_id, "history": [] }

@app.post("/query", response_model=RCAReport)
async def query(user_query: UserQuery) -> RCAReport:
    log = get_logger(service="master", query_id=user_query.query_id)

    # Downstream services would be called here
    return RCAReport(
        query_id=user_query.query_id,
        root_cause_summary="Not yet implement (Phase 0).",
        confidence=0.0,
        evidence=[
            RCAEvidenceItem(
                type="system",
                ref="master",
                snippet="Phase 0. Downstream calls not wired yet."
            )
        ],
        recommended_actions = ["Implement Master Orchestration pipeline."],
        reasoning_trace_summary="No reasoning here (Phase 0).",
        mttr_estimate_minutes=0,
        generated_at=datetime.now(timezone.utc)
    )