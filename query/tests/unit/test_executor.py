"""Unit tests for ElasticsearchExecutor (executor.py).

Uses unittest.mock to avoid needing a real Elasticsearch instance.
Tests verify correct DSL construction, result extraction, and error handling.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from src.executor import ElasticsearchExecutor, ExecutorResult
from src.schema_linker import FieldMeta, SchemaContext
from nexgen_shared.errors import E003ElasticsearchTimeout


def make_schema_ctx(indices: list[str] | None = None) -> SchemaContext:
    """Build a minimal SchemaContext for testing.

    Args:
        indices: Index names to include. Defaults to ['logs-2026'].

    Returns:
        SchemaContext with the given indices and one dummy field.
    """
    return SchemaContext(
        selected_indices=indices if indices is not None else ["logs-2026"],
        relevant_fields=[FieldMeta("service.name", "keyword")],
        time_field="@timestamp",
        max_result_size=500,
    )


def make_es_response(
    hits: list[dict],
    total: int = 0,
    timed_out: bool = False,
    shards_failed: int = 0,
) -> dict:
    """Build a fake Elasticsearch _search response dict.

    Args:
        hits: List of _source dicts to include in the response.
        total: Total hit count to report.
        timed_out: Whether to simulate a timeout flag.
        shards_failed: Number of failed shards to report.

    Returns:
        Dict mimicking the structure of a real ES response.
    """
    return {
        "hits": {
            "total": {"value": total or len(hits)},
            "hits": [{"_source": h} for h in hits],
        },
        "timed_out": timed_out,
        "_shards": {"failed": shards_failed},
    }


class TestExecutorStartupShutdown:
    """Tests for lifecycle methods."""

    @pytest.mark.asyncio
    async def test_execute_before_startup_raises_runtime_error(self) -> None:
        """Calling execute() without startup() must raise RuntimeError."""
        executor = ElasticsearchExecutor()
        with pytest.raises(RuntimeError, match="startup()"):
            await executor.execute(
                'service.name: "auth"', make_schema_ctx(), max_results=10
            )


class TestExecutorExecute:
    """Tests for the execute() method with mocked ES client."""

    @pytest.mark.asyncio
    async def test_returns_executor_result_instance(self) -> None:
        """execute() must return an ExecutorResult."""
        executor = ElasticsearchExecutor()
        mock_client = AsyncMock()
        mock_client.search = AsyncMock(
            return_value=make_es_response(
                [{"service": "auth"}], total=1
            )
        )
        executor._client = mock_client

        result = await executor.execute(
            'service.name: "auth"', make_schema_ctx(), max_results=10
        )
        assert isinstance(result, ExecutorResult)

    @pytest.mark.asyncio
    async def test_hits_extracted_from_source(self) -> None:
        """execute() must return _source contents as hits."""
        executor = ElasticsearchExecutor()
        mock_client = AsyncMock()
        fake_hit = {"service": "payments", "level": "ERROR"}
        mock_client.search = AsyncMock(
            return_value=make_es_response([fake_hit], total=1)
        )
        executor._client = mock_client

        result = await executor.execute(
            'service.name: "payments"', make_schema_ctx(), max_results=10
        )
        assert len(result.hits) == 1
        assert result.hits[0] == fake_hit

    @pytest.mark.asyncio
    async def test_total_count_extracted_correctly(self) -> None:
        """execute() must report the correct total from ES response."""
        executor = ElasticsearchExecutor()
        mock_client = AsyncMock()
        mock_client.search = AsyncMock(
            return_value=make_es_response([], total=42)
        )
        executor._client = mock_client

        result = await executor.execute(
            'service.name: "auth"', make_schema_ctx(), max_results=10
        )
        assert result.total == 42

    @pytest.mark.asyncio
    async def test_hard_cap_enforced(self) -> None:
        """max_results must be capped at ES_MAX_RESULTS_HARD_CAP."""
        executor = ElasticsearchExecutor()
        executor._settings.es_max_results_hard_cap = 100
        mock_client = AsyncMock()
        mock_client.search = AsyncMock(
            return_value=make_es_response([], total=0)
        )
        executor._client = mock_client

        await executor.execute(
            'service.name: "auth"', make_schema_ctx(), max_results=9999
        )
        call_kwargs = mock_client.search.call_args
        body = call_kwargs.kwargs.get("body")
        assert body["size"] == 100

    @pytest.mark.asyncio
    async def test_connection_error_raises_e003(self) -> None:
        """ConnectionError from ES must raise E003ElasticsearchTimeout."""
        from elasticsearch import ConnectionError as ESConnectionError

        executor = ElasticsearchExecutor()
        mock_client = AsyncMock()
        mock_client.search = AsyncMock(
            side_effect=ESConnectionError("connection refused")
        )
        executor._client = mock_client

        with pytest.raises(E003ElasticsearchTimeout):
            await executor.execute(
                'service.name: "auth"', make_schema_ctx(), max_results=10
            )

    @pytest.mark.asyncio
    async def test_index_pattern_built_from_schema_ctx(self) -> None:
        """Executor must search the indices from SchemaContext."""
        executor = ElasticsearchExecutor()
        mock_client = AsyncMock()
        mock_client.search = AsyncMock(
            return_value=make_es_response([], total=0)
        )
        executor._client = mock_client
        ctx = make_schema_ctx(indices=["payments-2026", "gateway-2026"])

        await executor.execute('log.level: "ERROR"', ctx, max_results=10)

        call_kwargs = mock_client.search.call_args
        index_arg = call_kwargs.kwargs.get("index")
        assert "payments-2026" in index_arg
        assert "gateway-2026" in index_arg


class TestExecutorResultFields:
    """Tests that ExecutorResult fields are populated correctly."""

    @pytest.mark.asyncio
    async def test_timed_out_flag_propagated(self) -> None:
        """timed_out=True in ES response must appear in ExecutorResult."""
        executor = ElasticsearchExecutor()
        mock_client = AsyncMock()
        mock_client.search = AsyncMock(
            return_value=make_es_response([], timed_out=True)
        )
        executor._client = mock_client

        result = await executor.execute(
            'service.name: "auth"', make_schema_ctx(), max_results=10
        )
        assert result.timed_out is True

    @pytest.mark.asyncio
    async def test_shards_failed_propagated(self) -> None:
        """shards_failed count from ES response must appear in result."""
        executor = ElasticsearchExecutor()
        mock_client = AsyncMock()
        mock_client.search = AsyncMock(
            return_value=make_es_response([], shards_failed=2)
        )
        executor._client = mock_client

        result = await executor.execute(
            'service.name: "auth"', make_schema_ctx(), max_results=10
        )
        assert result.shards_failed == 2

    @pytest.mark.asyncio
    async def test_empty_hits_returns_empty_list(self) -> None:
        """Zero hits from ES must produce an empty hits list."""
        executor = ElasticsearchExecutor()
        mock_client = AsyncMock()
        mock_client.search = AsyncMock(
            return_value=make_es_response([], total=0)
        )
        executor._client = mock_client

        result = await executor.execute(
            'service.name: "auth"', make_schema_ctx(), max_results=10
        )
        assert result.hits == []
        assert result.total == 0

    @pytest.mark.asyncio
    async def test_transport_error_raises_e003(self) -> None:
        """TransportError from ES must also raise E003."""
        from elasticsearch import TransportError

        executor = ElasticsearchExecutor()
        mock_client = AsyncMock()
        mock_client.search = AsyncMock(
            side_effect=TransportError("transport failed")
        )
        executor._client = mock_client

        with pytest.raises(E003ElasticsearchTimeout):
            await executor.execute(
                'service.name: "auth"', make_schema_ctx(), max_results=10
            )

    @pytest.mark.asyncio
    async def test_empty_indices_falls_back_to_wildcard(self) -> None:
        """Empty selected_indices must search '*' (all indices)."""
        executor = ElasticsearchExecutor()
        mock_client = AsyncMock()
        mock_client.search = AsyncMock(
            return_value=make_es_response([], total=0)
        )
        executor._client = mock_client
        ctx = make_schema_ctx(indices=[])

        await executor.execute('log.level: "ERROR"', ctx, max_results=10)

        call_kwargs = mock_client.search.call_args
        index_arg = call_kwargs.kwargs.get("index")
        assert index_arg == "*"

    @pytest.mark.asyncio
    async def test_dsl_body_contains_query_key(self) -> None:
        """The body sent to ES must contain a 'query' key."""
        executor = ElasticsearchExecutor()
        mock_client = AsyncMock()
        mock_client.search = AsyncMock(
            return_value=make_es_response([], total=0)
        )
        executor._client = mock_client

        await executor.execute(
            'service.name: "auth"', make_schema_ctx(), max_results=10
        )

        call_kwargs = mock_client.search.call_args
        body = call_kwargs.kwargs.get("body")
        assert body is not None
        assert "query" in body