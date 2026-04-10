"""Schema Linker — Stage 1 of the NL-to-KQL pipeline.

Fetches Elasticsearch index mappings, caches them in memory, and
resolves which indices and fields are relevant for an incoming query.

Defined in query.md §3.1.
"""

from __future__ import annotations

import asyncio
import fnmatch
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from elasticsearch import AsyncElasticsearch
from elasticsearch import ConnectionError as ESConnectionError
from pydantic_settings import BaseSettings, SettingsConfigDict

from nexgen_shared.errors import E001SchemaLinkingFailure, E003ElasticsearchTimeout

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Settings — reads from query/.env automatically
# ---------------------------------------------------------------------------

class SchemaLinkerSettings(BaseSettings):
    """Configuration values the Schema Linker reads from the .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    elasticsearch_url: str = "http://localhost:9200"
    elasticsearch_username: str = "elastic"
    elasticsearch_password: str = "changeme"
    es_request_timeout: int = 20
    schema_cache_refresh_interval_seconds: int = 300


# ---------------------------------------------------------------------------
# Data classes — these travel between pipeline stages
# ---------------------------------------------------------------------------

@dataclass
class FieldMeta:
    """Metadata describing one field inside an Elasticsearch index.

    Attributes:
        name: Full dot-notation field name, e.g. 'http.status_code'.
        es_type: Elasticsearch type string, e.g. 'keyword', 'integer',
            'nested', 'date'.
        sample_values: Up to 5 representative values seen in this field.
            Populated later by semantic enrichment; empty for now.
        is_nested: True when this field has ES type 'nested'. Nested
            fields require a special query wrapper in KQL — the generator
            must know about this to avoid producing invalid queries.
        nested_path: The nearest ancestor field that is 'nested', e.g.
            for field 'http.headers.name', nested_path might be 'http'.
            None if the field has no nested ancestor.
    """

    name: str
    es_type: str
    sample_values: list[Any] = field(default_factory=list)
    is_nested: bool = False
    nested_path: str | None = None


@dataclass
class SchemaContext:
    """Resolved schema snapshot passed to every downstream pipeline stage.

    The KQL Generator reads selected_indices and relevant_fields to build
    its prompt. The Executor uses selected_indices to know where to search.

    Attributes:
        selected_indices: Actual ES index names chosen for this query.
        relevant_fields: All FieldMeta objects from those indices.
        time_field: Primary timestamp field. Almost always '@timestamp'.
        max_result_size: Upper bound on hits; from LogRetrievalRequest.
    """

    selected_indices: list[str]
    relevant_fields: list[FieldMeta]
    time_field: str = "@timestamp"
    max_result_size: int = 500


# ---------------------------------------------------------------------------
# SchemaLinker — the main class
# ---------------------------------------------------------------------------

class SchemaLinker:
    """Fetches, caches, and serves Elasticsearch index schema information.

    Lifecycle:
        1. Call await startup() once during FastAPI app startup.
        2. Call await link(...) for every incoming LogRetrievalRequest.
        3. Call await shutdown() during FastAPI app shutdown.

    The cache is a dict mapping index name → list of FieldMeta.
    A background asyncio task refreshes it every N seconds so the
    service always reflects the latest ES index structure.
    """

    def __init__(self) -> None:
        """Initialise with an empty cache. Call startup() before using."""
        self._settings = SchemaLinkerSettings()
        self._client: AsyncElasticsearch | None = None
        self._cache: dict[str, list[FieldMeta]] = {}
        self._last_refreshed: datetime | None = None
        self._refresh_task: asyncio.Task[None] | None = None

    # ------------------------------------------------------------------
    # Lifecycle methods
    # ------------------------------------------------------------------

    async def startup(self) -> None:
        """Connect to Elasticsearch and load the initial schema cache.

        Creates the async ES client, performs the first cache load,
        then starts a background task for periodic refresh.

        Raises:
            E003ElasticsearchTimeout: If the initial ES connection fails.
        """
        self._client = AsyncElasticsearch(
            self._settings.elasticsearch_url,
            basic_auth=(
                self._settings.elasticsearch_username,
                self._settings.elasticsearch_password,
            ),
            request_timeout=self._settings.es_request_timeout,
        )
        await self.refresh_cache()
        self._refresh_task = asyncio.create_task(self._background_refresh())
        logger.info(
            "SchemaLinker started. Indices in cache: %d", len(self._cache)
        )

    async def shutdown(self) -> None:
        """Cancel the background refresh task and close the ES client.

        Call this during FastAPI app shutdown to avoid resource leaks.
        """
        if self._refresh_task is not None:
            self._refresh_task.cancel()
        if self._client is not None:
            await self._client.close()
        logger.info("SchemaLinker shut down cleanly.")

    # ------------------------------------------------------------------
    # Cache management
    # ------------------------------------------------------------------

    async def refresh_cache(self) -> None:
        """Fetch all index mappings from ES and rebuild the in-memory cache.

        Skips indices whose names start with '.' (ES internal indices
        like .kibana, .security-*).

        Raises:
            E003ElasticsearchTimeout: If the ES connection fails.
        """
        if self._client is None:
            return

        try:
            # GET /_mapping returns a dict: {index_name: {mappings: {...}}}
            response = await self._client.indices.get_mapping(index="*")
        except ESConnectionError as exc:
            raise E003ElasticsearchTimeout(
                f"Cannot reach Elasticsearch to refresh schema cache: {exc}"
            ) from exc

        new_cache: dict[str, list[FieldMeta]] = {}

        for index_name, mapping_data in response.items():
            # Skip Elasticsearch's own internal system indices
            if index_name.startswith("."):
                continue

            properties = (
                mapping_data
                .get("mappings", {})
                .get("properties", {})
            )
            new_cache[index_name] = self._extract_fields(properties)

        self._cache = new_cache
        self._last_refreshed = datetime.now(timezone.utc)
        logger.info(
            "Schema cache refreshed — %d indices loaded.", len(self._cache)
        )

    async def _background_refresh(self) -> None:
        """Periodically refresh the schema cache while the service runs.

        Errors are caught and logged — they do NOT crash the service.
        The previous cache remains valid until the next successful refresh.
        """
        interval = self._settings.schema_cache_refresh_interval_seconds
        while True:
            await asyncio.sleep(interval)
            try:
                await self.refresh_cache()
            except Exception as exc:  # noqa: BLE001
                logger.warning("Background schema refresh failed: %s", exc)

    def _extract_fields(
        self,
        properties: dict[str, Any],
        parent_path: str = "",
        nested_path: str | None = None,
    ) -> list[FieldMeta]:
        """Recursively walk ES mapping properties and build FieldMeta list.

        Handles arbitrarily deep nesting. When a field has type 'nested',
        all its children inherit that nested_path so the KQL Generator
        knows to wrap them in a nested query clause.

        Args:
            properties: The 'properties' dict from an ES mapping response.
            parent_path: Dot-notation prefix accumulated during recursion.
            nested_path: The nearest 'nested' ancestor's path, if any.

        Returns:
            Flat list of FieldMeta for every field found in properties.
        """
        result: list[FieldMeta] = []

        for field_name, field_config in properties.items():
            # Build the full dot-notation name for this field
            full_name = (
                f"{parent_path}.{field_name}" if parent_path else field_name
            )
            es_type = field_config.get("type", "object")
            is_nested = es_type == "nested"

            # If THIS field is nested, its children should reference it
            current_nested_path = full_name if is_nested else nested_path

            result.append(
                FieldMeta(
                    name=full_name,
                    es_type=es_type,
                    sample_values=[],
                    is_nested=is_nested,
                    nested_path=current_nested_path if is_nested else nested_path,
                )
            )

            # Recurse into sub-properties (object and nested fields both
            # have a 'properties' key containing their child fields)
            sub_properties = field_config.get("properties", {})
            if sub_properties:
                result.extend(
                    self._extract_fields(
                        sub_properties,
                        parent_path=full_name,
                        nested_path=current_nested_path,
                    )
                )

        return result

    # ------------------------------------------------------------------
    # Public API — called for every incoming request
    # ------------------------------------------------------------------

    async def link(
        self,
        natural_language: str,
        index_hints: list[str],
        schema_context_from_request: dict[str, Any],
    ) -> SchemaContext:
        """Resolve which ES indices and fields are relevant for this query.

        Steps:
        1. Raise E001 if the cache is empty (ES was never reachable).
        2. Match index_hints against cached index names using wildcards.
        3. Fall back to ALL cached indices if no hints match.
        4. Collect all FieldMeta from matched indices, deduplicating
           by field name.
        5. Merge any known_fields from the request's schema_context
           that are not already in the cache.

        Args:
            natural_language: The user's question. Not used in this
                phase — will be used in P3-Q1 for Qdrant-based semantic
                disambiguation.
            index_hints: Patterns like ['payments-*', 'gateway-*']
                supplied by the Master LLM in LogRetrievalRequest.
            schema_context_from_request: The schema_context dict from
                LogRetrievalRequest. May contain known_fields that the
                Master already knows about.

        Returns:
            SchemaContext ready to pass to FewShotSelector and
            KQLGenerator.

        Raises:
            E001SchemaLinkingFailure: When cache is empty — meaning ES
                is unreachable or has no user-facing indices at all.
        """
        if not self._cache:
            raise E001SchemaLinkingFailure(
                "Schema cache is empty. Elasticsearch may be unreachable "
                "or contain no user-facing indices."
            )

        # --- Step 1: match hints against real index names ---------------
        matched_indices = self._match_indices(index_hints)

        if not matched_indices:
            # No hints matched anything — use every cached index
            logger.warning(
                "No index hints matched. Falling back to all %d indices.",
                len(self._cache),
            )
            matched_indices = list(self._cache.keys())

        # --- Step 2: collect fields, deduplicated by name ---------------
        seen: set[str] = set()
        relevant_fields: list[FieldMeta] = []

        for index_name in matched_indices:
            for fm in self._cache.get(index_name, []):
                if fm.name not in seen:
                    seen.add(fm.name)
                    relevant_fields.append(fm)

        # --- Step 3: merge known_fields from the request ----------------
        for kf in schema_context_from_request.get("known_fields", []):
            if kf not in seen:
                relevant_fields.append(
                    FieldMeta(name=kf, es_type="keyword", sample_values=[])
                )
                seen.add(kf)

        return SchemaContext(
            selected_indices=matched_indices,
            relevant_fields=relevant_fields,
            time_field="@timestamp",
            max_result_size=500,
        )

    def _match_indices(self, index_hints: list[str]) -> list[str]:
        """Match wildcard index hints against cached index names.

        Uses Python's fnmatch so 'payments-*' matches 'payments-2026.04'.

        Args:
            index_hints: List of patterns from the Master LLM request.

        Returns:
            List of actual cached index names that matched at least
            one hint. Preserves insertion order. No duplicates.
        """
        matched: list[str] = []
        for cached_index in self._cache:
            for hint in index_hints:
                if fnmatch.fnmatch(cached_index, hint):
                    matched.append(cached_index)
                    break   # don't add the same index twice
        return matched

    # ------------------------------------------------------------------
    # Status reporting
    # ------------------------------------------------------------------

    def cache_status(self) -> dict[str, object]:
        """Return cache metadata for the GET /schema-cache/status endpoint.

        Returns:
            Dict with keys: last_refreshed (ISO string or None),
            index_count (int), field_count (int), is_stale (bool).
        """
        is_stale = True
        if self._last_refreshed is not None:
            age = (
                datetime.now(timezone.utc) - self._last_refreshed
            ).total_seconds()
            is_stale = (
                age > self._settings.schema_cache_refresh_interval_seconds
            )

        total_fields = sum(len(fields) for fields in self._cache.values())

        return {
            "last_refreshed": (
                self._last_refreshed.isoformat()
                if self._last_refreshed is not None
                else None
            ),
            "index_count": len(self._cache),
            "field_count": total_fields,
            "is_stale": is_stale,
        }