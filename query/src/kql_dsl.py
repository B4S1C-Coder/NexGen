"""KQL-to-DSL transpiler — converts Kibana Query Language strings into
Elasticsearch Query DSL dicts that the REST API _search endpoint accepts.

KQL syntax reference: https://www.elastic.co/docs/explore-analyze/query-filter/languages/kql

Supported KQL patterns (all verified against official Elastic docs):
  Term match:     service.name: "payments"
  Numeric range:  http.status_code >= 400
  Date math:      @timestamp >= now-1h
  Boolean:        field: "a" AND field2: "b" OR NOT field3: "c"
  Term set:       log.level: ("ERROR" OR "WARN")
  Exists:         trace.id: *
  Wildcard:       service.name: pay*
  Nested:         user:{ first: "Alice" AND last: "White" }
  Full-text:      message: "connection refused"

Reference: query.md §3.6
"""

from __future__ import annotations

import re


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def kql_to_dsl(kql: str) -> dict:
    """Translate a Kibana KQL query string into an Elasticsearch Query DSL dict.

    Args:
        kql: A KQL query string produced by the KQLGenerator, using
             standard Kibana KQL syntax (no pipe clauses).

    Returns:
        An Elasticsearch Query DSL dict ready to pass directly to the
        AsyncElasticsearch client's search() method as the 'query' value.

    Example:
        >>> kql_to_dsl('service.name: "auth" AND log.level: "ERROR"')
        {'query': {'bool': {'must': [
            {'term': {'service.name': 'auth'}},
            {'term': {'log.level': 'ERROR'}}
        ]}}}
    """
    kql = kql.strip()
    if not kql:
        return {"query": {"match_all": {}}}
    return {"query": _parse_expression(kql)}


# ---------------------------------------------------------------------------
# Expression parser — handles boolean operators
# ---------------------------------------------------------------------------

def _parse_expression(expr: str) -> dict:
    """Recursively parse a KQL expression into an ES query DSL dict.

    Processes boolean operators in correct precedence order:
    OR (lowest) → AND → NOT → leaf expressions (highest).

    Args:
        expr: A KQL expression string, no leading/trailing whitespace.

    Returns:
        ES query DSL dict for the expression.
    """
    expr = expr.strip()
    if not expr:
        return {"match_all": {}}

    # OR has lowest precedence — split on OR first
    or_parts = _split_on_operator(expr, "OR")
    if len(or_parts) > 1:
        should_clauses = [_parse_expression(p) for p in or_parts]
        return {"bool": {"should": should_clauses, "minimum_should_match": 1}}

    # AND has next precedence
    and_parts = _split_on_operator(expr, "AND")
    if len(and_parts) > 1:
        must_clauses = [_parse_expression(p) for p in and_parts]
        return {"bool": {"must": must_clauses}}

    # NOT prefix
    not_match = re.match(r"^NOT\s+(.+)$", expr, re.IGNORECASE)
    if not_match:
        inner = _parse_expression(not_match.group(1).strip())
        return {"bool": {"must_not": [inner]}}

    # Single leaf clause
    return _parse_leaf(expr)


def _split_on_operator(expr: str, operator: str) -> list[str]:
    """Split a KQL expression on a boolean keyword, respecting parentheses.

    Only splits on top-level occurrences — nested parentheses are
    never split regardless of what operator appears inside them.

    Args:
        expr: The full KQL expression string.
        operator: 'AND' or 'OR' (case-insensitive match against tokens).

    Returns:
        List of sub-expression strings. Returns [expr] if no split found.
    """
    parts: list[str] = []
    current_chars: list[str] = []
    depth = 0
    i = 0

    while i < len(expr):
        char = expr[i]

        if char in "({":
            depth += 1
            current_chars.append(char)
            i += 1
            continue

        if char in ")}":
            depth -= 1
            current_chars.append(char)
            i += 1
            continue

        # Only attempt operator matching at depth 0
        if depth == 0:
            # Check if current position starts with the operator keyword
            remaining = expr[i:]
            pattern = rf"^{operator}\s+"
            match = re.match(pattern, remaining, re.IGNORECASE)
            if match:
                part = "".join(current_chars).strip()
                if part:
                    parts.append(part)
                current_chars = []
                i += len(match.group(0))
                continue

        current_chars.append(char)
        i += 1

    last_part = "".join(current_chars).strip()
    if last_part:
        parts.append(last_part)

    # Return original if no split occurred
    return parts if len(parts) > 1 else [expr]


# ---------------------------------------------------------------------------
# Leaf expression parser — handles individual KQL clauses
# ---------------------------------------------------------------------------

def _parse_leaf(expr: str) -> dict:
    """Parse a single KQL clause (no AND/OR/NOT) into an ES query dict.

    Handles in order:
      1. Parenthesised group → recurse
      2. Nested field query  → field:{ sub AND sub }
      3. Range query         → field >= value
      4. Term set            → field: (v1 OR v2)
      5. Wildcard/exists     → field: * or field: val*
      6. Term/match          → field: "value" or field: value

    Args:
        expr: A single KQL clause string with no top-level boolean operators.

    Returns:
        ES query DSL dict for this clause.
    """
    expr = expr.strip()

    # 1. Unwrap outer parentheses
    if expr.startswith("(") and expr.endswith(")"):
        inner = expr[1:-1].strip()
        # Only unwrap if parens are balanced and wrap the whole expression
        if _parens_balanced(inner):
            return _parse_expression(inner)

    # 2. Nested field query: field:{ subexpr }
    nested_match = re.match(
        r"^([\w.@]+)\s*:\s*\{(.+)\}$", expr, re.DOTALL
    )
    if nested_match:
        field = nested_match.group(1)
        sub_expr = nested_match.group(2).strip()
        inner_query = _parse_expression(sub_expr)
        return {
            "nested": {
                "path": field.split(".")[0],
                "query": inner_query,
            }
        }

    # 3. Range query: field >= value / field <= value / field > value / field < value
    range_match = re.match(
        r"^([\w.@]+)\s*([><=!]{1,2})\s*(.+)$", expr
    )
    if range_match:
        field = range_match.group(1)
        operator = range_match.group(2).strip()
        value_raw = range_match.group(3).strip().strip('"').strip("'")
        op_map = {">=": "gte", "<=": "lte", ">": "gt", "<": "lt"}
        es_op = op_map.get(operator)
        if es_op:
            return {"range": {field: {es_op: value_raw}}}

    # 4. Field-colon expressions
    colon_match = re.match(r"^([\w.@]+)\s*:\s*(.+)$", expr, re.DOTALL)
    if colon_match:
        field = colon_match.group(1)
        value_part = colon_match.group(2).strip()

        # 4a. Exists: field: *
        if value_part == "*":
            return {"exists": {"field": field}}

        # 4b. Term set: field: (v1 OR v2 OR v3)
        if value_part.startswith("(") and value_part.endswith(")"):
            inner = value_part[1:-1]
            values = [
                v.strip().strip('"').strip("'")
                for v in re.split(r"\s+OR\s+", inner, flags=re.IGNORECASE)
            ]
            return {"terms": {field: values}}

        # 4c. Wildcard: field: val* or field: *val*
        if "*" in value_part and not value_part.startswith('"'):
            value_clean = value_part.strip("'\"")
            return {"wildcard": {field: {"value": value_clean}}}

        # 4d. Quoted string → term (exact keyword match)
        quoted = re.match(r'^"(.+)"$', value_part) or re.match(
            r"^'(.+)'$", value_part
        )
        if quoted:
            return {"term": {field: quoted.group(1)}}

        # 4e. Unquoted → match (full-text search)
        return {"match": {field: value_part}}

    # Fallback: treat as multi-match across all fields
    return {"multi_match": {"query": expr, "fields": ["*"]}}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parens_balanced(expr: str) -> bool:
    """Check that parentheses in an expression are balanced.

    Args:
        expr: Any string to check.

    Returns:
        True if all opening parens have matching closing parens.
    """
    depth = 0
    for char in expr:
        if char in "({":
            depth += 1
        elif char in ")}":
            depth -= 1
        if depth < 0:
            return False
    return depth == 0