"""KQL Validator — Stage 4 of the NL-to-KQL pipeline.

Validates KQL strings produced by the KQLGenerator without executing
them against Elasticsearch. Catches syntax errors early so the
RepairAgent can ask the LLM to fix them before wasting an ES query.

Checks performed:
  1. Empty string guard
  2. Balanced parentheses and curly braces
  3. No double boolean operators (AND AND, OR OR, etc.)
  4. No dangling operators at start or end of expression
  5. Colon expressions have a value after the colon
  6. Field names exist in SchemaContext (when schema is provided)

Defined in TASKS.md P2-Q3.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from .schema_linker import SchemaContext


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class ValidationResult:
    """Result of validating a KQL query string.

    Attributes:
        valid:   True if the KQL passed all checks.
        errors:  List of human-readable error descriptions.
        kql:     The original KQL string (stripped of whitespace).
    """

    valid: bool
    errors: list[str]
    kql: str


# ---------------------------------------------------------------------------
# KQLValidator
# ---------------------------------------------------------------------------

class KQLValidator:
    """Validates KQL strings for syntax correctness.

    Does not require Elasticsearch — all checks are pure string analysis.
    Optionally uses SchemaContext to validate field names against the
    actual index mapping fetched by SchemaLinker.

    Usage:
        validator = KQLValidator()
        result = validator.validate(kql_string, schema_ctx)
        if not result.valid:
            # pass result.errors to RepairAgent
    """

    def validate(
        self,
        kql: str,
        schema_ctx: SchemaContext | None = None,
    ) -> ValidationResult:
        """Run all validation checks on a KQL string.

        Args:
            kql:        The KQL string to validate.
            schema_ctx: Optional SchemaContext for field name validation.
                        If None, field name checks are skipped.

        Returns:
            ValidationResult with valid flag, error list, and original KQL.
        """
        kql = kql.strip()
        errors: list[str] = []

        # Check 1 — empty string
        if not kql:
            return ValidationResult(
                valid=False,
                errors=["KQL string is empty."],
                kql=kql,
            )

        # Check 2 — balanced parentheses and curly braces
        paren_error = _check_balanced(kql)
        if paren_error:
            errors.append(paren_error)

        # Check 3 — double boolean operators (uses finditer to catch all)
        double_op_error = _check_double_operators(kql)
        if double_op_error:
            errors.append(double_op_error)

        # Check 4 — dangling operators at boundaries
        boundary_error = _check_boundary_operators(kql)
        if boundary_error:
            errors.append(boundary_error)

        # Check 5 — colon expressions have a value
        colon_errors = _check_colon_values(kql)
        errors.extend(colon_errors)

        # Check 6 — field names exist in schema (optional)
        if schema_ctx is not None:
            field_errors = _check_field_names(kql, schema_ctx)
            errors.extend(field_errors)

        return ValidationResult(
            valid=len(errors) == 0,
            errors=errors,
            kql=kql,
        )


# ---------------------------------------------------------------------------
# Individual check functions
# ---------------------------------------------------------------------------

def _check_balanced(kql: str) -> str | None:
    """Check that parentheses and curly braces are balanced.

    Args:
        kql: The KQL string to check.

    Returns:
        An error message string if unbalanced, None if balanced.
    """
    stack: list[tuple[str, int]] = []
    pairs = {"(": ")", "{": "}"}
    closing = set(pairs.values())

    for i, char in enumerate(kql):
        if char in pairs:
            stack.append((char, i))
        elif char in closing:
            if not stack:
                return f"Unexpected closing '{char}' at position {i}."
            expected_open = [k for k, v in pairs.items() if v == char][0]
            if stack[-1][0] != expected_open:
                return (
                    f"Mismatched bracket: opened with '{stack[-1][0]}' "
                    f"at position {stack[-1][1]} but closed with '{char}' "
                    f"at position {i}."
                )
            stack.pop()

    if stack:
        unclosed = stack[-1]
        return f"Unclosed '{unclosed[0]}' at position {unclosed[1]}."
    return None


def _check_double_operators(kql: str) -> str | None:
    """Check for double boolean operators like AND AND or OR OR.

    Uses finditer to catch ALL occurrences, not just the first.

    Args:
        kql: The KQL string to check.

    Returns:
        An error message string if any double operator found, None otherwise.
    """
    pattern = re.compile(
        r"\b(AND\s+AND|OR\s+OR|NOT\s+NOT|AND\s+OR|OR\s+AND)\b",
        re.IGNORECASE,
    )
    matches = list(pattern.finditer(kql))
    if not matches:
        return None
    if len(matches) == 1:
        m = matches[0]
        return (
            f"Double or conflicting boolean operator: "
            f"'{m.group()}' at position {m.start()}."
        )
    # Multiple double operators found — report all of them
    details = ", ".join(
        f"'{m.group()}' at position {m.start()}" for m in matches
    )
    return f"Multiple double/conflicting boolean operators found: {details}."


def _check_boundary_operators(kql: str) -> str | None:
    """Check for boolean operators at the very start or end of expression.

    Args:
        kql: The KQL string to check.

    Returns:
        An error message string if dangling operator found, None otherwise.
    """
    # Expression begins with AND/OR (NOT at start is valid — means negation)
    start_pattern = re.compile(r"^\s*(AND|OR)\s+", re.IGNORECASE)
    if start_pattern.match(kql):
        return "KQL expression starts with a boolean operator (AND/OR)."

    # Expression ends with AND/OR/NOT
    end_pattern = re.compile(r"\s+(AND|OR|NOT)\s*$", re.IGNORECASE)
    if end_pattern.search(kql):
        return "KQL expression ends with a boolean operator (AND/OR/NOT)."

    return None


def _check_colon_values(kql: str) -> list[str]:
    """Check that field:value expressions have a value after the colon.

    Only fires when a colon is followed immediately by AND/OR/NOT or
    end of string. Quoted values like field: "value" are correctly ignored.

    Args:
        kql: The KQL string to check.

    Returns:
        List of error message strings (empty if no errors).
    """
    errors = []
    # Match field: followed directly by a boolean keyword or end of string
    # Quoted strings after colon are NOT matched by this pattern
    pattern = re.compile(
        r"([\w.@]+)\s*:\s*(?=AND\b|OR\b|NOT\b|$)",
        re.IGNORECASE,
    )
    for match in pattern.finditer(kql):
        field = match.group(1)
        # Skip if this field name is itself a boolean keyword
        if field.upper() in {"AND", "OR", "NOT"}:
            continue
        errors.append(
            f"Field '{field}' has a colon but no value at position {match.start()}."
        )
    return errors


def _check_field_names(kql: str, schema_ctx: SchemaContext) -> list[str]:
    """Check that field names in the KQL exist in the SchemaContext.

    Extracts field names from field:value and field>=value patterns and
    checks each against the relevant_fields list in SchemaContext.

    Skips validation entirely when schema is empty to avoid false
    positives on cold start before SchemaLinker has cached anything.

    Args:
        kql:        The KQL string to check.
        schema_ctx: SchemaContext from SchemaLinker with known fields.

    Returns:
        List of error message strings for unknown fields (empty if all valid).
    """
    known_fields = {f.name for f in schema_ctx.relevant_fields}
    # Skip check if schema is empty — cold start protection
    if not known_fields:
        return []

    errors = []
    # Extract field names from field: and field>=/<= patterns
    field_pattern = re.compile(r"([\w.@]+)\s*[:<>!=]")
    seen: set[str] = set()

    for match in field_pattern.finditer(kql):
        field_name = match.group(1)
        # Skip KQL keywords and date math keywords
        if field_name.upper() in {"AND", "OR", "NOT", "NOW"}:
            continue
        # Skip duplicates — only report each unknown field once
        if field_name in seen:
            continue
        seen.add(field_name)

        if field_name not in known_fields:
            sample = sorted(known_fields)[:5]
            errors.append(
                f"Field '{field_name}' not found in schema. "
                f"Sample known fields: {sample}."
            )
    return errors