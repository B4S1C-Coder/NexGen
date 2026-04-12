"""Unit tests for the KQL-to-DSL transpiler (kql_dsl.py).

All tests are pure Python — no Elasticsearch connection required.
Each test verifies one specific KQL pattern maps to the correct ES DSL.

KQL syntax verified against:
https://www.elastic.co/docs/explore-analyze/query-filter/languages/kql
"""

from __future__ import annotations

import pytest

from src.kql_dsl import kql_to_dsl


class TestEmptyAndMatchAll:
    """Tests for edge cases — empty and whitespace-only input."""

    def test_empty_string_returns_match_all(self) -> None:
        """Empty KQL must return a match_all query."""
        result = kql_to_dsl("")
        assert result == {"query": {"match_all": {}}}

    def test_whitespace_only_returns_match_all(self) -> None:
        """Whitespace-only KQL must return a match_all query."""
        result = kql_to_dsl("   ")
        assert result == {"query": {"match_all": {}}}


class TestTermQueries:
    """Tests for field: value term queries."""

    def test_quoted_string_produces_term_query(self) -> None:
        """field: "value" must produce an ES term query for exact match."""
        result = kql_to_dsl('service.name: "payments"')
        assert result["query"] == {"term": {"service.name": "payments"}}

    def test_unquoted_string_produces_match_query(self) -> None:
        """field: value (no quotes) must produce a full-text match query."""
        result = kql_to_dsl("message: timeout")
        assert result["query"] == {"match": {"message": "timeout"}}

    def test_exists_wildcard_produces_exists_query(self) -> None:
        """field: * must produce an ES exists query."""
        result = kql_to_dsl("trace.id: *")
        assert result["query"] == {"exists": {"field": "trace.id"}}

    def test_at_symbol_field_name_supported(self) -> None:
        """Field names starting with @ (like @timestamp) must be supported."""
        result = kql_to_dsl('@timestamp: "2026-04-01"')
        assert "term" in result["query"]
        assert "@timestamp" in result["query"]["term"]


class TestRangeQueries:
    """Tests for numeric and date range queries."""

    def test_greater_than_or_equal(self) -> None:
        """field >= value must produce range with gte."""
        result = kql_to_dsl("http.status_code >= 400")
        assert result["query"] == {
            "range": {"http.status_code": {"gte": "400"}}
        }

    def test_less_than_or_equal(self) -> None:
        """field <= value must produce range with lte."""
        result = kql_to_dsl("http.status_code <= 499")
        assert result["query"] == {
            "range": {"http.status_code": {"lte": "499"}}
        }

    def test_less_than(self) -> None:
        """field < value must produce range with lt."""
        result = kql_to_dsl("http.status_code < 500")
        assert result["query"] == {
            "range": {"http.status_code": {"lt": "500"}}
        }

    def test_greater_than(self) -> None:
        """field > value must produce range with gt."""
        result = kql_to_dsl("http.response_time_ms > 5000")
        assert result["query"] == {
            "range": {"http.response_time_ms": {"gt": "5000"}}
        }

    def test_date_math_no_quotes(self) -> None:
        """@timestamp >= now-1h must produce range with gte and no quotes."""
        result = kql_to_dsl("@timestamp >= now-1h")
        assert result["query"] == {
            "range": {"@timestamp": {"gte": "now-1h"}}
        }

    def test_date_math_thirty_minutes(self) -> None:
        """@timestamp >= now-30m must work correctly."""
        result = kql_to_dsl("@timestamp >= now-30m")
        assert result["query"] == {
            "range": {"@timestamp": {"gte": "now-30m"}}
        }

    def test_date_math_start_of_day(self) -> None:
        """@timestamp >= now/d (start of today) must work correctly."""
        result = kql_to_dsl("@timestamp >= now/d")
        assert result["query"] == {
            "range": {"@timestamp": {"gte": "now/d"}}
        }


class TestBooleanOperators:
    """Tests for AND, OR, NOT boolean combinations."""

    def test_and_produces_bool_must(self) -> None:
        """field AND field must produce bool.must with two clauses."""
        result = kql_to_dsl('service.name: "auth" AND log.level: "ERROR"')
        query = result["query"]
        assert "bool" in query
        assert "must" in query["bool"]
        assert len(query["bool"]["must"]) == 2

    def test_or_produces_bool_should(self) -> None:
        """field OR field must produce bool.should."""
        result = kql_to_dsl('log.level: "ERROR" OR log.level: "WARN"')
        query = result["query"]
        assert "bool" in query
        assert "should" in query["bool"]
        assert len(query["bool"]["should"]) == 2

    def test_not_produces_bool_must_not(self) -> None:
        """NOT field must produce bool.must_not."""
        result = kql_to_dsl('NOT log.level: "DEBUG"')
        query = result["query"]
        assert "bool" in query
        assert "must_not" in query["bool"]

    def test_three_and_clauses(self) -> None:
        """Three AND conditions must all appear in bool.must."""
        result = kql_to_dsl(
            'service.name: "payments" AND log.level: "ERROR"'
            ' AND @timestamp >= now-30m'
        )
        query = result["query"]
        assert len(query["bool"]["must"]) == 3


class TestTermSets:
    """Tests for field: (value1 OR value2) term set queries."""

    def test_term_set_two_values(self) -> None:
        """field: (v1 OR v2) must produce an ES terms query."""
        result = kql_to_dsl('log.level: ("ERROR" OR "WARN")')
        assert result["query"] == {
            "terms": {"log.level": ["ERROR", "WARN"]}
        }

    def test_term_set_three_values(self) -> None:
        """Three values in a term set must all appear in terms list."""
        result = kql_to_dsl('http.method: ("GET" OR "POST" OR "DELETE")')
        query = result["query"]
        assert "terms" in query
        assert len(query["terms"]["http.method"]) == 3


class TestWildcardAndExists:
    """Tests for wildcard pattern queries and exists checks."""

    def test_trailing_wildcard_produces_wildcard_query(self) -> None:
        """field: val* must produce a wildcard query."""
        result = kql_to_dsl("service.name: pay*")
        assert result["query"] == {
            "wildcard": {"service.name": {"value": "pay*"}}
        }

    def test_field_exists_check(self) -> None:
        """field: * must produce an exists query."""
        result = kql_to_dsl("trace.id: *")
        assert result["query"] == {"exists": {"field": "trace.id"}}


class TestParenthesesAndPrecedence:
    """Tests for parenthesised expressions and operator precedence."""

    def test_parenthesised_group_or_with_and(self) -> None:
        """(A AND B) OR C must respect parentheses grouping."""
        result = kql_to_dsl(
            '(service.name: "auth" AND log.level: "ERROR")'
            ' OR service.name: "gateway"'
        )
        query = result["query"]
        assert "bool" in query
        assert "should" in query["bool"]

    def test_and_binds_tighter_than_or(self) -> None:
        """A AND B OR C must evaluate AND first, then OR at top level."""
        result = kql_to_dsl(
            'service.name: "auth" AND log.level: "ERROR"'
            ' OR service.name: "gateway"'
        )
        query = result["query"]
        # OR at top level produces should
        assert "bool" in query
        assert "should" in query["bool"]

    def test_not_with_parenthesised_expression(self) -> None:
        """NOT (A AND B) must negate the entire grouped expression."""
        result = kql_to_dsl(
            'NOT (service.name: "auth" AND log.level: "DEBUG")'
        )
        query = result["query"]
        assert "bool" in query
        assert "must_not" in query["bool"]


class TestNestedFieldQueries:
    """Tests for nested field curly brace syntax per KQL official docs."""

    def test_nested_field_produces_nested_query(self) -> None:
        """field:{ sub AND sub } must produce an ES nested query."""
        result = kql_to_dsl('user:{ first: "Alice" AND last: "White" }')
        query = result["query"]
        assert "nested" in query
        assert query["nested"]["path"] == "user"

    def test_nested_query_contains_inner_bool(self) -> None:
        """Nested query inner expression with AND must be a bool.must."""
        result = kql_to_dsl('user:{ first: "Alice" AND last: "White" }')
        inner = result["query"]["nested"]["query"]
        assert "bool" in inner
        assert "must" in inner["bool"]


class TestCaseInsensitiveOperators:
    """Tests that AND/OR/NOT are case-insensitive per KQL official spec."""

    def test_lowercase_and_works(self) -> None:
        """Lowercase 'and' must work identically to uppercase 'AND'."""
        result = kql_to_dsl('service.name: "auth" and log.level: "ERROR"')
        assert "bool" in result["query"]
        assert "must" in result["query"]["bool"]

    def test_lowercase_or_works(self) -> None:
        """Lowercase 'or' must work identically to uppercase 'OR'."""
        result = kql_to_dsl('log.level: "ERROR" or log.level: "WARN"')
        assert "bool" in result["query"]
        assert "should" in result["query"]["bool"]

    def test_lowercase_not_works(self) -> None:
        """Lowercase 'not' must work identically to uppercase 'NOT'."""
        result = kql_to_dsl('not log.level: "DEBUG"')
        assert "bool" in result["query"]
        assert "must_not" in result["query"]["bool"]


class TestComplexRealisticQueries:
    """Tests for realistic combined KQL queries matching fallback examples."""

    def test_service_and_level_and_timestamp(self) -> None:
        """Full realistic query: service + level + timestamp."""
        result = kql_to_dsl(
            'service.name: "payments" AND http.status_code: 500'
            ' AND @timestamp >= now-30m'
        )
        query = result["query"]
        assert "bool" in query
        assert len(query["bool"]["must"]) == 3

    def test_status_code_range_4xx(self) -> None:
        """4xx range: status >= 400 AND status < 500."""
        result = kql_to_dsl(
            "http.status_code >= 400 AND http.status_code < 500"
        )
        query = result["query"]
        assert "bool" in query
        must = query["bool"]["must"]
        assert len(must) == 2
        assert all("range" in clause for clause in must)

    def test_term_set_with_timestamp(self) -> None:
        """Term set combined with date range."""
        result = kql_to_dsl(
            'log.level: ("ERROR" OR "WARN") AND @timestamp >= now-2h'
        )
        query = result["query"]
        assert "bool" in query
        assert "must" in query["bool"]


class TestFallbackBehavior:
    """Tests for multi_match fallback on unrecognised input."""

    def test_bare_text_falls_back_to_multi_match(self) -> None:
        """A plain word with no field: prefix produces a multi_match query."""
        result = kql_to_dsl("timeout")
        assert result["query"] == {
            "multi_match": {"query": "timeout", "fields": ["*"]}
        }