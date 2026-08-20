"""KQL Validator — Stage 4 of the NL-to-KQL pipeline.

Validates KQL strings produced by the KQLGenerator without executing
them against Elasticsearch. Catches syntax errors early so the
RepairAgent can ask the LLM to fix them before wasting an ES query.

Uses `kaquel` to enforce proper KQL syntax.

Defined in TASKS.md P2-Q3.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from kaquel.kql import parse_kql

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
    """Validates KQL strings for syntax correctness using kaquel.

    Does not require Elasticsearch — all checks are pure string analysis.
    Optionally uses SchemaContext to validate field names against the
    actual index mapping fetched by SchemaLinker.
    """

    def validate(
        self,
        kql: str,
        schema_ctx: SchemaContext | None = None,
    ) -> ValidationResult:
        """Run all validation checks on a KQL string."""
        kql = kql.strip()
        errors: list[str] = []

        if not kql:
            return ValidationResult(
                valid=False,
                errors=["KQL string is empty."],
                kql=kql,
            )

        # 1. Syntax check via kaquel parser
        try:
            parse_kql(kql)
        except Exception as exc:
            errors.append(f"KQL Syntax Error: {exc}")

        # 2. Check field names exist in schema (optional)
        if schema_ctx is not None and len(errors) == 0:
            field_errors = _check_field_names(kql, schema_ctx)
            errors.extend(field_errors)

        return ValidationResult(
            valid=len(errors) == 0,
            errors=errors,
            kql=kql,
        )


def _check_field_names(kql: str, schema_ctx: SchemaContext) -> list[str]:
    """Check that field names in the KQL exist in the SchemaContext.

    Extracts field names from field:value and field>=value patterns and
    checks each against the relevant_fields list in SchemaContext.
    """
    known_fields = {f.name for f in schema_ctx.relevant_fields}
    if not known_fields:
        return []

    errors = []
    field_pattern = re.compile(r"([\w.@]+)\s*[:<>!=]")
    seen: set[str] = set()

    for match in field_pattern.finditer(kql):
        field_name = match.group(1)
        if field_name.upper() in {"AND", "OR", "NOT", "NOW"}:
            continue
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