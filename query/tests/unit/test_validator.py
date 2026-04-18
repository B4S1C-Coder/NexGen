"""Unit tests for KQLValidator (validator.py).

All tests are pure Python — no Elasticsearch or Qdrant required.
Each test verifies one specific validation rule in isolation.
"""

from __future__ import annotations

import pytest

from src.validator import (
    KQLValidator,
    ValidationResult,
    _check_balanced,
    _check_boundary_operators,
    _check_colon_values,
    _check_double_operators,
    _check_field_names,
)
from src.schema_linker import FieldMeta, SchemaContext


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_schema_ctx(field_names: list[str]) -> SchemaContext:
    """Build a minimal SchemaContext with the given field names."""
    return SchemaContext(
        selected_indices=["logs-*"],
        relevant_fields=[FieldMeta(name=n, es_type="keyword") for n in field_names],
        time_field="@timestamp",
        max_result_size=500,
    )


# ---------------------------------------------------------------------------
# Tests for ValidationResult dataclass
# ---------------------------------------------------------------------------

class TestValidationResult:
    """Tests for the ValidationResult dataclass."""

    def test_valid_result_has_no_errors(self) -> None:
        """A valid result must have an empty errors list."""
        result = ValidationResult(valid=True, errors=[], kql="service.name: *")
        assert result.valid is True
        assert result.errors == []

    def test_invalid_result_has_errors(self) -> None:
        """An invalid result must have at least one error."""
        result = ValidationResult(valid=False, errors=["some error"], kql="")
        assert result.valid is False
        assert len(result.errors) >= 1


# ---------------------------------------------------------------------------
# Tests for KQLValidator.validate() — happy path
# ---------------------------------------------------------------------------

class TestValidatorHappyPath:
    """Tests for valid KQL strings that should pass all checks."""

    def test_simple_term_query_is_valid(self) -> None:
        """A basic field:value query must be valid."""
        result = KQLValidator().validate('service.name: "payments"')
        assert result.valid is True
        assert result.errors == []

    def test_boolean_and_query_is_valid(self) -> None:
        """AND query with two terms must be valid."""
        result = KQLValidator().validate(
            'service.name: "auth" AND log.level: "ERROR"'
        )
        assert result.valid is True

    def test_three_clause_and_query_is_valid(self) -> None:
        """Three-clause AND query must be valid."""
        result = KQLValidator().validate(
            'service.name: "payments" AND log.level: "ERROR" AND @timestamp >= now-30m'
        )
        assert result.valid is True

    def test_term_set_is_valid(self) -> None:
        """Term set with OR must be valid."""
        result = KQLValidator().validate('log.level: ("ERROR" OR "WARN")')
        assert result.valid is True

    def test_quoted_value_with_spaces_is_valid(self) -> None:
        """Quoted value containing spaces must not trigger colon check."""
        result = KQLValidator().validate('message: "connection refused"')
        assert result.valid is True

    def test_date_math_range_is_valid(self) -> None:
        """Date math range must be valid."""
        result = KQLValidator().validate("@timestamp >= now-1h")
        assert result.valid is True

    def test_exists_wildcard_is_valid(self) -> None:
        """Exists check must be valid."""
        result = KQLValidator().validate("trace.id: *")
        assert result.valid is True

    def test_not_prefix_is_valid(self) -> None:
        """NOT prefix is valid at start of expression."""
        result = KQLValidator().validate('NOT log.level: "DEBUG"')
        assert result.valid is True


# ---------------------------------------------------------------------------
# Tests for Check 1 — empty string
# ---------------------------------------------------------------------------

class TestEmptyStringCheck:
    """Tests for the empty string guard."""

    def test_empty_string_is_invalid(self) -> None:
        """Empty KQL must be invalid."""
        result = KQLValidator().validate("")
        assert result.valid is False
        assert any("empty" in e.lower() for e in result.errors)

    def test_whitespace_only_is_invalid(self) -> None:
        """Whitespace-only KQL must be invalid."""
        result = KQLValidator().validate("   ")
        assert result.valid is False


# ---------------------------------------------------------------------------
# Tests for Check 2 — balanced parentheses
# ---------------------------------------------------------------------------

class TestBalancedParentheses:
    """Tests for the balanced parentheses and braces check."""

    def test_unclosed_paren_is_invalid(self) -> None:
        """Unclosed parenthesis must be caught."""
        error = _check_balanced('log.level: ("ERROR" OR "WARN"')
        assert error is not None
        assert "Unclosed" in error

    def test_unexpected_closing_paren_is_invalid(self) -> None:
        """Extra closing parenthesis must be caught."""
        error = _check_balanced('log.level: "ERROR")')
        assert error is not None
        assert "Unexpected" in error

    def test_balanced_parens_returns_none(self) -> None:
        """Balanced parentheses must return None (no error)."""
        error = _check_balanced('log.level: ("ERROR" OR "WARN")')
        assert error is None

    def test_nested_balanced_parens_returns_none(self) -> None:
        """Nested balanced parentheses must return None."""
        error = _check_balanced('(service.name: "auth" AND (log.level: "ERROR"))')
        assert error is None


# ---------------------------------------------------------------------------
# Tests for Check 3 — double operators
# ---------------------------------------------------------------------------

class TestDoubleOperators:
    """Tests for the double boolean operator check."""

    def test_and_and_is_invalid(self) -> None:
        """AND AND must be caught."""
        error = _check_double_operators('service.name: "auth" AND AND log.level: "ERROR"')
        assert error is not None
        assert "AND AND" in error or "AND" in error

    def test_or_or_is_invalid(self) -> None:
        """OR OR must be caught."""
        error = _check_double_operators('log.level: "ERROR" OR OR log.level: "WARN"')
        assert error is not None

    def test_valid_and_returns_none(self) -> None:
        """Single AND must return None."""
        error = _check_double_operators('service.name: "auth" AND log.level: "ERROR"')
        assert error is None

    def test_multiple_double_operators_all_reported(self) -> None:
        """Multiple double operators must all appear in the error message."""
        error = _check_double_operators(
            'service.name: "a" AND AND log.level: "b" OR OR trace.id: *'
        )
        assert error is not None
        assert "AND AND" in error
        assert "OR OR" in error


# ---------------------------------------------------------------------------
# Tests for Check 4 — boundary operators
# ---------------------------------------------------------------------------

class TestBoundaryOperators:
    """Tests for dangling operators at expression boundaries."""

    def test_starts_with_and_is_invalid(self) -> None:
        """Expression starting with AND must be invalid."""
        error = _check_boundary_operators('AND service.name: "auth"')
        assert error is not None

    def test_ends_with_or_is_invalid(self) -> None:
        """Expression ending with OR must be invalid."""
        error = _check_boundary_operators('service.name: "auth" OR')
        assert error is not None

    def test_valid_expression_returns_none(self) -> None:
        """Valid expression must return None."""
        error = _check_boundary_operators('service.name: "auth" AND log.level: "ERROR"')
        assert error is None

    def test_not_at_start_is_valid(self) -> None:
        """NOT at start of expression is valid KQL."""
        error = _check_boundary_operators('NOT log.level: "DEBUG"')
        assert error is None


# ---------------------------------------------------------------------------
# Tests for Check 5 — colon without value
# ---------------------------------------------------------------------------

class TestColonWithoutValue:
    """Tests for colon expressions missing a value."""

    def test_field_colon_and_is_invalid(self) -> None:
        """field: AND must be caught as missing value."""
        errors = _check_colon_values('service.name: AND log.level: "ERROR"')
        assert len(errors) >= 1
        assert "service.name" in errors[0]

    def test_valid_colon_expression_no_errors(self) -> None:
        """Valid field:value must return no errors."""
        errors = _check_colon_values('service.name: "payments"')
        assert errors == []

    def test_quoted_value_not_flagged(self) -> None:
        """Quoted value with spaces must not trigger colon check."""
        errors = _check_colon_values('message: "connection refused"')
        assert errors == []

    def test_wildcard_value_not_flagged(self) -> None:
        """Wildcard value must not trigger colon check."""
        errors = _check_colon_values("trace.id: *")
        assert errors == []


# ---------------------------------------------------------------------------
# Tests for Check 6 — field name schema validation
# ---------------------------------------------------------------------------

class TestFieldNameValidation:
    """Tests for field name schema validation."""

    def test_known_field_passes(self) -> None:
        """Field present in schema must not produce errors."""
        ctx = make_schema_ctx(["service.name", "log.level"])
        errors = _check_field_names('service.name: "auth"', ctx)
        assert errors == []

    def test_unknown_field_produces_error(self) -> None:
        """Field absent from schema must produce an error."""
        ctx = make_schema_ctx(["service.name", "log.level"])
        errors = _check_field_names('nonexistent.field: "value"', ctx)
        assert len(errors) >= 1
        assert "nonexistent.field" in errors[0]

    def test_empty_schema_skips_check(self) -> None:
        """Empty schema must skip field name check (cold start protection)."""
        ctx = make_schema_ctx([])
        errors = _check_field_names('any.field: "value"', ctx)
        assert errors == []

    def test_unknown_field_reported_only_once(self) -> None:
        """Same unknown field used twice must only be reported once."""
        ctx = make_schema_ctx(["log.level"])
        errors = _check_field_names(
            'unknown.field: "a" AND unknown.field: "b"', ctx
        )
        assert len(errors) == 1