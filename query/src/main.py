"""FastAPI application entry-point for the NL-to-KQL Pipeline service.

Exposes three endpoints as defined in AGENTS.md §6.1:
- POST /retrieve  — translate NL to KQL and fetch log rows
- GET  /health    — liveness probe
- GET  /schema-cache/status — index-schema cache freshness
"""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import FastAPI
from pydantic_settings import BaseSettings, SettingsConfigDict

from nexgen_shared.schemas import LogRetrievalRequest, LogRetrievalResult


# ---------------------------------------------------------------------------
# Configuration — reads from query/.env automatically
# ---------------------------------------------------------------------------

class Settings(BaseSettings):
    """Service configuration loaded from environment variables / .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    query_port: int = 8001
    log_level: str = "INFO"
    elasticsearch_url: str = "http://localhost:9200"
    ollama_base_url: str = "http://localhost:11434"
    query_llm_model: str = "qwen2.5-coder:7b-instruct-q4_K_M"
    qdrant_url: str = "http://localhost:6333"
    max_repair_attempts: int = 3
    default_max_results: int = 500
    schema_cache_refresh_interval_seconds: int = 300


settings = Settings()

# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------

app = FastAPI(
    title="NexGen NL-to-KQL Pipeline",
    description="Translates natural language to KQL and retrieves logs from Elasticsearch.",
    version="0.1.0",
)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.post("/retrieve", response_model=LogRetrievalResult)
async def retrieve(request: LogRetrievalRequest) -> LogRetrievalResult:
    """Accept a LogRetrievalRequest, return a LogRetrievalResult.

    Currently a stub — returns an empty success result.
    Real pipeline stages (SchemaLinker → FewShotSelector → KQLGenerator
    → Validator → RepairAgent → ESExecutor → PIIMasker) will be wired
    in subsequent tasks (P1-Q1 through P2-Q6).

    Args:
        request: The incoming log retrieval request from the Master LLM.

    Returns:
        A LogRetrievalResult with status 'success' and empty hits.
    """
    return LogRetrievalResult(
        query_id=request.query_id,
        status="success",
        kql_generated="",
        syntax_valid=False,
        refinement_attempts=0,
        hits=[],
        hit_count=0,
        error=None,
    )


@app.get("/health")
async def health() -> dict[str, str]:
    """Liveness probe endpoint.

    Returns:
        A simple status dict confirming the service is running.
    """
    return {"status": "ok", "service": "query"}


@app.get("/schema-cache/status")
async def schema_cache_status() -> dict[str, object]:
    """Report the freshness of the Elasticsearch index-schema cache.

    Returns:
        Cache metadata including last refresh time and index count.
        Returns nulls until SchemaLinker is implemented in P1-Q1.
    """
    return {
        "last_refreshed": None,
        "index_count": 0,
        "field_count": 0,
        "is_stale": True,
    }