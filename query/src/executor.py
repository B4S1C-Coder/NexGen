"""Elasticsearch Executor — Stage 6 of the NL-to-KQL pipeline.

Translates validated KQL to ES Query DSL via kql_dsl.py, executes the
search against the live Elasticsearch cluster, and returns raw log hits.

Defined in query.md §3.6.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from elasticsearch import AsyncElasticsearch
from elasticsearch import ConnectionError as ESConnectionError
from elasticsearch import TransportError
from pydantic_settings import BaseSettings, SettingsConfigDict

from nexgen_shared.errors import E003ElasticsearchTimeout
from .kql_dsl import kql_to_dsl
from .schema_linker import SchemaContext

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

class ExecutorSettings(BaseSettings):
    """Configuration for the Elasticsearch Executor."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    elasticsearch_url: str = "http://localhost:9200"
    elasticsearch_username: str = "elastic"
    elasticsearch_password: str = "changeme"
    es_request_timeout: int = 20
    es_max_results_hard_cap: int = 2000


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class ExecutorResult:
    """Raw result from Elasticsearch before PII masking.

    Attributes:
        hits: List of raw _source dicts from Elasticsearch.
        total: Total number of matching documents in the index.
        timed_out: True if Elasticsearch reported a timeout.
        shards_failed: Number of shards that failed during search.
    """

    hits: list[dict]
    total: int
    timed_out: bool
    shards_failed: int


# ---------------------------------------------------------------------------
# ElasticsearchExecutor
# ---------------------------------------------------------------------------

class ElasticsearchExecutor:
    """Executes KQL queries against a live Elasticsearch cluster.

    Translates KQL to ES Query DSL internally using kql_dsl.py, then
    uses the official elasticsearch-py async client to run the search.

    Usage:
        executor = ElasticsearchExecutor()
        await executor.startup()
        result = await executor.execute(kql, schema_ctx, max_results=500)
        await executor.shutdown()
    """

    def __init__(self) -> None:
        """Initialise with no client — call startup() before using."""
        self._settings = ExecutorSettings()
        self._client: AsyncElasticsearch | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def startup(self) -> None:
        """Create the async Elasticsearch client.

        Should be called once during FastAPI app lifespan startup.
        Does not perform a connection test — connection is verified
        lazily on the first search call.
        """
        self._client = AsyncElasticsearch(
            self._settings.elasticsearch_url,
            basic_auth=(
                self._settings.elasticsearch_username,
                self._settings.elasticsearch_password,
            ),
            request_timeout=self._settings.es_request_timeout,
        )
        logger.info(
            "ElasticsearchExecutor started. Target: %s",
            self._settings.elasticsearch_url,
        )

    async def shutdown(self) -> None:
        """Close the async Elasticsearch client connection.

        Should be called during FastAPI app lifespan shutdown.
        """
        if self._client is not None:
            await self._client.close()
            logger.info("ElasticsearchExecutor shut down cleanly.")

    # ------------------------------------------------------------------
    # Core execution
    # ------------------------------------------------------------------

    async def execute(
        self,
        kql: str,
        schema_ctx: SchemaContext,
        max_results: int,
    ) -> ExecutorResult:
        """Translate KQL to DSL and execute the search against Elasticsearch.

        Enforces the hard cap from settings — max_results is capped at
        ES_MAX_RESULTS_HARD_CAP regardless of what the caller requests.

        Args:
            kql: A validated KQL query string from the KQLValidator.
            schema_ctx: SchemaContext from SchemaLinker containing the
                list of indices to search against.
            max_results: Maximum number of hits to return. Capped at
                ES_MAX_RESULTS_HARD_CAP from settings.

        Returns:
            ExecutorResult with hits, total count, and shard metadata.

        Raises:
            E003ElasticsearchTimeout: On ConnectionError or TransportError.
            RuntimeError: If startup() was never called.
        """
        if self._client is None:
            raise RuntimeError(
                "ElasticsearchExecutor.startup() must be called before execute()."
            )

        # Enforce hard cap — never return more than configured maximum
        capped_results = min(max_results, self._settings.es_max_results_hard_cap)

        # Translate KQL string to ES Query DSL dict
        dsl = kql_to_dsl(kql)
        logger.debug("KQL: %s", kql)
        logger.debug("DSL: %s", dsl)

        # Determine which indices to search
        indices = schema_ctx.selected_indices or ["*"]
        index_pattern = ",".join(indices)

        try:
            response = await self._client.search(
                index=index_pattern,
                body={
                    **dsl,
                    "size": capped_results,
                    "_source": True,
                },
            )
        except ESConnectionError as exc:
            raise E003ElasticsearchTimeout(
                f"Elasticsearch connection failed during search: {exc}"
            ) from exc
        except TransportError as exc:
            raise E003ElasticsearchTimeout(
                f"Elasticsearch transport error during search: {exc}"
            ) from exc

        # Extract results from ES response structure
        hits_raw = response.get("hits", {})
        hit_list = [
            hit.get("_source", {})
            for hit in hits_raw.get("hits", [])
        ]
        total = hits_raw.get("total", {}).get("value", 0)
        timed_out = response.get("timed_out", False)
        shards_failed = response.get("_shards", {}).get("failed", 0)

        logger.info(
            "ES search complete. index=%s hits=%d total=%d timed_out=%s",
            index_pattern,
            len(hit_list),
            total,
            timed_out,
        )

        return ExecutorResult(
            hits=hit_list,
            total=total,
            timed_out=timed_out,
            shards_failed=shards_failed,
        )