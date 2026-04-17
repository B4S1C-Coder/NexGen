"""Integration test for ElasticsearchExecutor against a real ES instance.

Requires Elasticsearch running on localhost:9200 (via docker compose).
Seeds a test document, executes a real KQL query, verifies the result.

TASKS.md P1-Q2: "Integration test (requires running ES): simple term
query returns expected document from a seeded index."

Run with:
    pytest tests/integration/test_executor_es.py -v
"""

from __future__ import annotations

import uuid

import pytest
from elasticsearch import AsyncElasticsearch

from src.executor import ElasticsearchExecutor
from src.schema_linker import FieldMeta, SchemaContext

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ES_URL = "http://localhost:9200"

# Unique index per test run — avoids stale data from previous runs
TEST_INDEX = f"nexgen-test-{uuid.uuid4().hex[:8]}"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_schema_ctx() -> SchemaContext:
    """Build a SchemaContext targeting the test index.

    Returns:
        SchemaContext with the test index and relevant fields.
    """
    return SchemaContext(
        selected_indices=[TEST_INDEX],
        relevant_fields=[
            FieldMeta("service.name", "keyword"),
            FieldMeta("log.level", "keyword"),
            FieldMeta("message", "text"),
            FieldMeta("trace.id", "keyword"),
        ],
        time_field="@timestamp",
        max_result_size=10,
    )


async def create_and_seed_index(client: AsyncElasticsearch) -> None:
    """Create the test index and seed one known document.

    Args:
        client: An open AsyncElasticsearch client.

    Returns:
        None
    """
    # Always delete first to start clean (ignore if not found)
    try:
        await client.indices.delete(index=TEST_INDEX)
    except Exception:
        pass

    # Create index with explicit field mappings
    await client.indices.create(
        index=TEST_INDEX,
        body={
            "mappings": {
                "properties": {
                    "service.name": {"type": "keyword"},
                    "log.level":    {"type": "keyword"},
                    "message":      {"type": "text"},
                    "trace.id":     {"type": "keyword"},
                }
            }
        },
    )

    # Seed one known document
    await client.index(
        index=TEST_INDEX,
        id="test-doc-1",
        body={
            "service.name": "payments",
            "log.level":    "ERROR",
            "message":      "Connection refused to db-primary",
            "trace.id":     "abc-123-xyz",
        },
        refresh="wait_for",
    )


async def delete_index(client: AsyncElasticsearch) -> None:
    """Delete the test index silently.

    Args:
        client: An open AsyncElasticsearch client.

    Returns:
        None
    """
    try:
        await client.indices.delete(index=TEST_INDEX)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestExecutorWithRealElasticsearch:
    """Integration tests for ElasticsearchExecutor against real ES.

    Each test creates its own client and handles setup/teardown
    independently to avoid event loop and fixture scope issues.
    """

    @pytest.mark.asyncio
    async def test_term_query_returns_seeded_document(self) -> None:
        """A term query on service.name must return the seeded document.

        This is the core TASKS.md P1-Q2 requirement:
        'simple term query returns expected document from a seeded index'
        """
        client = AsyncElasticsearch(ES_URL)
        try:
            await create_and_seed_index(client)

            executor = ElasticsearchExecutor()
            await executor.startup()
            try:
                result = await executor.execute(
                    kql='service.name: "payments"',
                    schema_ctx=make_schema_ctx(),
                    max_results=10,
                )
            finally:
                await executor.shutdown()

            assert result.total >= 1, (
                f"Expected at least 1 hit, got {result.total}"
            )
            assert len(result.hits) >= 1
            assert result.timed_out is False

        finally:
            await delete_index(client)
            await client.close()

    @pytest.mark.asyncio
    async def test_and_query_returns_correct_document(self) -> None:
        """An AND query combining service.name and log.level must match."""
        client = AsyncElasticsearch(ES_URL)
        try:
            await create_and_seed_index(client)

            executor = ElasticsearchExecutor()
            await executor.startup()
            try:
                result = await executor.execute(
                    kql='service.name: "payments" AND log.level: "ERROR"',
                    schema_ctx=make_schema_ctx(),
                    max_results=10,
                )
            finally:
                await executor.shutdown()

            assert result.total >= 1
            assert len(result.hits) >= 1

        finally:
            await delete_index(client)
            await client.close()

    @pytest.mark.asyncio
    async def test_non_matching_query_returns_zero_hits(self) -> None:
        """A query that matches nothing must return zero hits, not an error."""
        client = AsyncElasticsearch(ES_URL)
        try:
            await create_and_seed_index(client)

            executor = ElasticsearchExecutor()
            await executor.startup()
            try:
                result = await executor.execute(
                    kql='service.name: "nonexistent-service-xyz"',
                    schema_ctx=make_schema_ctx(),
                    max_results=10,
                )
            finally:
                await executor.shutdown()

            assert result.total == 0
            assert result.hits == []
            assert result.timed_out is False

        finally:
            await delete_index(client)
            await client.close()

    @pytest.mark.asyncio
    async def test_executor_result_has_correct_structure(self) -> None:
        """ExecutorResult must have all required fields with correct types."""
        client = AsyncElasticsearch(ES_URL)
        try:
            await create_and_seed_index(client)

            executor = ElasticsearchExecutor()
            await executor.startup()
            try:
                result = await executor.execute(
                    kql='service.name: "payments"',
                    schema_ctx=make_schema_ctx(),
                    max_results=10,
                )
            finally:
                await executor.shutdown()

            assert isinstance(result.hits, list)
            assert isinstance(result.total, int)
            assert isinstance(result.timed_out, bool)
            assert isinstance(result.shards_failed, int)
            assert result.shards_failed == 0

        finally:
            await delete_index(client)
            await client.close()