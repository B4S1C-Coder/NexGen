"""Unit tests for SchemaLinker (TASKS.md P1-Q1).

All tests inject a pre-built cache so no real Elasticsearch is needed.
"""

from __future__ import annotations

import pytest

from src.schema_linker import FieldMeta, SchemaContext, SchemaLinker
from nexgen_shared.errors import E001SchemaLinkingFailure


# ---------------------------------------------------------------------------
# Helper — build a linker with a fake cache injected directly
# ---------------------------------------------------------------------------

def linker_with_cache(cache: dict[str, list[FieldMeta]]) -> SchemaLinker:
    """Return a SchemaLinker with a pre-populated cache.

    Bypasses startup() so no real Elasticsearch connection is needed.

    Args:
        cache: Mapping of index name to list of FieldMeta.

    Returns:
        SchemaLinker ready to call .link() immediately.
    """
    linker = SchemaLinker()
    linker._cache = cache
    return linker


# ---------------------------------------------------------------------------
# Tests for _extract_fields — the recursive mapping parser
# ---------------------------------------------------------------------------

class TestExtractFields:
    """Tests for the internal _extract_fields method."""

    def test_simple_flat_field_extracted(self) -> None:
        """A flat keyword field returns one FieldMeta with correct name."""
        linker = SchemaLinker()
        props = {"service_name": {"type": "keyword"}}
        fields = linker._extract_fields(props)
        assert len(fields) == 1
        assert fields[0].name == "service_name"
        assert fields[0].es_type == "keyword"
        assert fields[0].is_nested is False

    def test_nested_field_marked_is_nested_true(self) -> None:
        """A field of ES type 'nested' must have is_nested=True."""
        linker = SchemaLinker()
        props = {
            "http": {
                "type": "nested",
                "properties": {"status_code": {"type": "integer"}},
            }
        }
        fields = linker._extract_fields(props)
        http_field = next(f for f in fields if f.name == "http")
        assert http_field.is_nested is True
        assert http_field.nested_path == "http"

    def test_child_of_nested_field_inherits_nested_path(self) -> None:
        """Children of a nested field carry the parent's nested_path."""
        linker = SchemaLinker()
        props = {
            "http": {
                "type": "nested",
                "properties": {"status_code": {"type": "integer"}},
            }
        }
        fields = linker._extract_fields(props)
        child = next(f for f in fields if f.name == "http.status_code")
        assert child.nested_path == "http"
        assert child.is_nested is False

    def test_dot_notation_applied_to_nested_children(self) -> None:
        """Child field name must be parent.child in dot notation."""
        linker = SchemaLinker()
        props = {
            "user": {
                "type": "object",
                "properties": {"id": {"type": "keyword"}},
            }
        }
        fields = linker._extract_fields(props)
        names = [f.name for f in fields]
        assert "user.id" in names

    def test_deeply_nested_field_name_is_correct(self) -> None:
        """Three-level nesting must produce correct dot-notation names."""
        linker = SchemaLinker()
        props = {
            "a": {
                "type": "object",
                "properties": {
                    "b": {
                        "type": "object",
                        "properties": {
                            "c": {"type": "keyword"}
                        },
                    }
                },
            }
        }
        fields = linker._extract_fields(props)
        names = [f.name for f in fields]
        assert "a.b.c" in names


# ---------------------------------------------------------------------------
# Tests for _match_indices — wildcard pattern matching
# ---------------------------------------------------------------------------

class TestMatchIndices:
    """Tests for _match_indices wildcard matching."""

    def test_exact_name_matches(self) -> None:
        """An exact index name hint matches directly."""
        linker = linker_with_cache(
            {"payments-2026": [FieldMeta("service.name", "keyword")]}
        )
        assert "payments-2026" in linker._match_indices(["payments-2026"])

    def test_wildcard_matches_versioned_index(self) -> None:
        """'payments-*' must match 'payments-2026.04'."""
        linker = linker_with_cache(
            {
                "payments-2026.04": [FieldMeta("service.name", "keyword")],
                "auth-2026.04": [FieldMeta("log.level", "keyword")],
            }
        )
        matched = linker._match_indices(["payments-*"])
        assert "payments-2026.04" in matched
        assert "auth-2026.04" not in matched

    def test_no_matching_hint_returns_empty(self) -> None:
        """A hint that matches nothing returns an empty list."""
        linker = linker_with_cache(
            {"payments-2026": [FieldMeta("service.name", "keyword")]}
        )
        assert linker._match_indices(["nonexistent-*"]) == []

    def test_multiple_hints_can_match_multiple_indices(self) -> None:
        """Two hints can each match a different index."""
        linker = linker_with_cache(
            {
                "payments-2026": [FieldMeta("service.name", "keyword")],
                "auth-2026": [FieldMeta("log.level", "keyword")],
            }
        )
        matched = linker._match_indices(["payments-*", "auth-*"])
        assert "payments-2026" in matched
        assert "auth-2026" in matched


# ---------------------------------------------------------------------------
# Tests for link() — the main public method
# ---------------------------------------------------------------------------

class TestLink:
    """Tests for the link() method."""

    @pytest.mark.asyncio
    async def test_raises_e001_when_cache_is_empty(self) -> None:
        """Empty cache must raise E001SchemaLinkingFailure."""
        linker = SchemaLinker()   # cache is {} by default
        with pytest.raises(E001SchemaLinkingFailure):
            await linker.link("show errors", ["logs-*"], {})

    @pytest.mark.asyncio
    async def test_returns_schema_context_instance(self) -> None:
        """link() must return a SchemaContext object."""
        linker = linker_with_cache(
            {"payments-2026": [FieldMeta("service.name", "keyword")]}
        )
        result = await linker.link("show payment errors", ["payments-*"], {})
        assert isinstance(result, SchemaContext)

    @pytest.mark.asyncio
    async def test_matched_index_in_selected_indices(self) -> None:
        """The matched index must appear in SchemaContext.selected_indices."""
        linker = linker_with_cache(
            {
                "payments-2026": [FieldMeta("service.name", "keyword")],
                "auth-2026": [FieldMeta("log.level", "keyword")],
            }
        )
        result = await linker.link("show payment errors", ["payments-*"], {})
        assert "payments-2026" in result.selected_indices
        assert "auth-2026" not in result.selected_indices

    @pytest.mark.asyncio
    async def test_falls_back_to_all_indices_when_no_hint_matches(
        self,
    ) -> None:
        """When hints match nothing, all cached indices are used."""
        linker = linker_with_cache(
            {
                "payments-2026": [FieldMeta("service.name", "keyword")],
                "auth-2026": [FieldMeta("log.level", "keyword")],
            }
        )
        result = await linker.link("show errors", ["nonexistent-*"], {})
        assert len(result.selected_indices) == 2

    @pytest.mark.asyncio
    async def test_known_fields_from_request_merged_in(self) -> None:
        """known_fields from the request must appear in relevant_fields."""
        linker = linker_with_cache(
            {"payments-2026": [FieldMeta("service.name", "keyword")]}
        )
        result = await linker.link(
            "show errors",
            ["payments-*"],
            {"known_fields": ["custom.field"], "value_samples": {}},
        )
        names = [f.name for f in result.relevant_fields]
        assert "custom.field" in names

    @pytest.mark.asyncio
    async def test_fields_deduplicated_across_indices(self) -> None:
        """Same field name in two indices must appear only once in result."""
        shared = FieldMeta("service.name", "keyword")
        linker = linker_with_cache(
            {
                "payments-2026": [shared],
                "payments-2025": [shared],
            }
        )
        result = await linker.link("show errors", ["payments-*"], {})
        names = [f.name for f in result.relevant_fields]
        assert names.count("service.name") == 1


# ---------------------------------------------------------------------------
# Tests for cache_status()
# ---------------------------------------------------------------------------

class TestCacheStatus:
    """Tests for the cache_status() reporting method."""

    def test_empty_cache_reports_stale(self) -> None:
        """Fresh SchemaLinker with no cache must report is_stale=True."""
        linker = SchemaLinker()
        status = linker.cache_status()
        assert status["index_count"] == 0
        assert status["is_stale"] is True
        assert status["last_refreshed"] is None

    def test_index_and_field_counts_are_accurate(self) -> None:
        """Counts must reflect exactly what is in the cache."""
        linker = linker_with_cache(
            {
                "payments-2026": [
                    FieldMeta("service.name", "keyword"),
                    FieldMeta("log.level", "keyword"),
                ],
                "auth-2026": [
                    FieldMeta("user.id", "keyword"),
                ],
            }
        )
        status = linker.cache_status()
        assert status["index_count"] == 2
        assert status["field_count"] == 3