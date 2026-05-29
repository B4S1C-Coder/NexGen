"""FastAPI application entry-point for the NL-to-KQL Pipeline service.

Exposes three endpoints as defined in AGENTS.md §6.1:
- POST /retrieve  — translate NL to KQL and fetch log rows
- GET  /health    — liveness probe
- GET  /schema-cache/status — index-schema cache freshness

P2-Q6: Full pipeline wired — all components connected.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from pydantic_settings import BaseSettings, SettingsConfigDict

from nexgen_shared.schemas import (
    LogRetrievalRequest,
    LogRetrievalResult,
    LogHit,
)
from nexgen_shared.errors import (
    E001SchemaLinkingFailure,
    E002KqlSyntaxError,
    E003ElasticsearchTimeout,
)

from .schema_linker import SchemaLinker
from .few_shot import FewShotSelector
from .generator import KQLGenerator
from .validator import KQLValidator
from .repair import RepairAgent
from .executor import ElasticsearchExecutor
from .pii import PIIMasker

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    query_port: int = 8001
    log_level: str = "INFO"
    elasticsearch_url: str = "http://localhost:9200"
    ollama_base_url: str = "http://localhost:11434"
    qdrant_url: str = "http://localhost:6333"
    max_repair_attempts: int = 3
    default_max_results: int = 500
    schema_cache_refresh_interval_seconds: int = 300


settings = Settings()


# ---------------------------------------------------------------------------
# Component instances (module-level singletons)
# ---------------------------------------------------------------------------

schema_linker = SchemaLinker()
few_shot_selector = FewShotSelector()
generator = KQLGenerator()
validator = KQLValidator()
repair_agent = RepairAgent(generator, validator)
executor = ElasticsearchExecutor()
pii_masker = PIIMasker()


# ---------------------------------------------------------------------------
# Lifespan — startup and shutdown all components
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start all pipeline components on app startup, shut down on exit."""
    logger.info("Starting NL-to-KQL pipeline components...")
    await schema_linker.startup()
    await few_shot_selector.startup()
    generator.startup()
    await executor.startup()
    logger.info("All pipeline components started.")

    yield

    logger.info("Shutting down NL-to-KQL pipeline components...")
    await executor.shutdown()
    generator.shutdown()
    await few_shot_selector.shutdown()
    await schema_linker.shutdown()
    logger.info("All pipeline components shut down.")


# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------

app = FastAPI(
    title="NexGen NL-to-KQL Pipeline",
    description="Translates natural language to KQL and retrieves logs.",
    version="0.2.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# POST /retrieve — full pipeline
# ---------------------------------------------------------------------------

@app.post("/retrieve", response_model=LogRetrievalResult)
async def retrieve(request: LogRetrievalRequest) -> LogRetrievalResult:
    """Accept a LogRetrievalRequest, run the full NL-to-KQL pipeline.

    Pipeline stages:
        1. SchemaLinker.link()           → SchemaContext
        2. FewShotSelector.select()      → list[FewShotExample]
        3. RepairAgent.repair()          → validated KQL string
        4. ElasticsearchExecutor.execute()→ ExecutorResult
        5. PIIMasker.mask()              → cleaned hits
        6. Assemble LogRetrievalResult
    """
    refinement_attempts = 0

    try:
        # Stage 1 — Schema linking
        schema_ctx = await schema_linker.link(
            natural_language=request.natural_language,
            index_hints=request.index_hints,
            schema_context_from_request=request.schema_context or {},
        )

        # Stage 2 — Few-shot example retrieval
        examples = await few_shot_selector.select(request.natural_language)

        # Stage 3 — KQL generation with validation and repair
        kql = await repair_agent.repair(
            natural_language=request.natural_language,
            schema_ctx=schema_ctx,
            examples=examples,
        )

        # Stage 4 — Execute against Elasticsearch
        max_results = request.max_results or settings.default_max_results
        exec_result = await executor.execute(
            kql=kql,
            schema_ctx=schema_ctx,
            max_results=max_results,
        )

        # Stage 5 — Mask PII in raw hits
        clean_hits = pii_masker.mask(exec_result.hits)

        # Stage 6 — Assemble result
        status = "success"
        if exec_result.timed_out or exec_result.shards_failed > 0:
            status = "partial"

        log_hits = [
            LogHit(
                timestamp=h.get("@timestamp", ""),
                service=h.get("service.name", h.get("service", "")),
                level=h.get("log.level", h.get("level", "")),
                message=h.get("message", ""),
                trace_id=h.get("trace.id", h.get("trace_id", "")),
            )
            for h in clean_hits
        ]

        return LogRetrievalResult(
            query_id=request.query_id,
            status=status,
            kql_generated=kql,
            syntax_valid=True,
            refinement_attempts=refinement_attempts,
            hits=log_hits,
            hit_count=exec_result.total,
            error=None,
        )

    except E001SchemaLinkingFailure as exc:
        logger.error("Schema linking failed: %s", exc)
        return LogRetrievalResult(
            query_id=request.query_id,
            status="failure",
            kql_generated="",
            syntax_valid=False,
            refinement_attempts=0,
            hits=[],
            hit_count=0,
            error=f"E001: {exc}",
        )

    except E002KqlSyntaxError as exc:
        logger.error("KQL syntax error after all repair attempts: %s", exc)
        return LogRetrievalResult(
            query_id=request.query_id,
            status="failure",
            kql_generated="",
            syntax_valid=False,
            refinement_attempts=settings.max_repair_attempts,
            hits=[],
            hit_count=0,
            error=f"E002: {exc}",
        )

    except E003ElasticsearchTimeout as exc:
        logger.error("Elasticsearch timeout: %s", exc)
        return LogRetrievalResult(
            query_id=request.query_id,
            status="failure",
            kql_generated="",
            syntax_valid=True,
            refinement_attempts=0,
            hits=[],
            hit_count=0,
            error=f"E003: {exc}",
        )

    except Exception as exc:
        logger.exception("Unexpected error in /retrieve: %s", exc)
        return LogRetrievalResult(
            query_id=request.query_id,
            status="failure",
            kql_generated="",
            syntax_valid=False,
            refinement_attempts=0,
            hits=[],
            hit_count=0,
            error=f"Unexpected error: {exc}",
        )


# ---------------------------------------------------------------------------
# GET /health
# ---------------------------------------------------------------------------

@app.get("/health")
async def health() -> dict[str, str]:
    """Liveness probe endpoint."""
    return {"status": "ok", "service": "query"}


# ---------------------------------------------------------------------------
# GET /schema-cache/status
# ---------------------------------------------------------------------------

@app.get("/schema-cache/status")
async def schema_cache_status() -> dict[str, object]:
    """Report the freshness of the Elasticsearch index-schema cache."""
    status = schema_linker.cache_status()
    return status