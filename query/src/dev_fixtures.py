"""Fixture-backed NL-to-KQL retrieval used when MOCK_SERVICES=true.

Produces a plausible KQL string and a small set of payments-outage log
hits so the Master orchestrator can run without Elasticsearch.
"""

from __future__ import annotations

from datetime import datetime, timezone

from nexgen_shared.schemas import LogHit, LogRetrievalRequest, LogRetrievalResult


def generate_fixture_kql(natural_language: str) -> str:
    """Derive a deterministic KQL string from a natural-language question.

    Args:
        natural_language: The inbound operator question.

    Returns:
        A KQL expression covering service, status, and a rolling time window.
    """
    text = natural_language.lower()
    clauses: list[str] = []
    if "payment" in text:
        clauses.append("service.name: payments")
    elif "auth" in text:
        clauses.append("service.name: auth")
    elif "gateway" in text:
        clauses.append("service.name: gateway")
    if "500" in text or "error" in text or "fail" in text:
        clauses.append("http.status_code: 500")
    else:
        clauses.append("log.level: ERROR")
    clauses.append("@timestamp >= now-30m")
    return " AND ".join(clauses)


def fixture_log_hits(natural_language: str) -> list[LogHit]:
    """Return seeded log hits for the demo payments outage.

    Args:
        natural_language: Used to lightly filter which services appear.

    Returns:
        Masked ``LogHit`` records suitable for ``LogRetrievalResult.hits``.
    """
    stamp = datetime(2026, 4, 6, 9, 58, 21, tzinfo=timezone.utc)
    hits = [
        LogHit(
            timestamp=stamp,
            service="payments",
            level="ERROR",
            message="Connection refused: db-primary:5432",
            trace_id="abc123",
        ),
        LogHit(
            timestamp=datetime(2026, 4, 6, 9, 57, 4, tzinfo=timezone.utc),
            service="payments",
            level="ERROR",
            message="TCP timeout contacting db-primary after 5000ms",
            trace_id="abc123",
        ),
        LogHit(
            timestamp=datetime(2026, 4, 6, 9, 58, 40, tzinfo=timezone.utc),
            service="gateway",
            level="WARN",
            message="Upstream payments returned HTTP 500 for /charge",
            trace_id="abc123",
        ),
    ]
    text = natural_language.lower()
    if "auth" in text and "payment" not in text:
        return [
            LogHit(
                timestamp=stamp,
                service="auth",
                level="ERROR",
                message="Token issuer unreachable: auth-db:5432",
                trace_id="def456",
            )
        ]
    return hits


def mock_retrieve(request: LogRetrievalRequest) -> LogRetrievalResult:
    """Assemble a successful ``LogRetrievalResult`` from local fixtures.

    Args:
        request: Canonical retrieval request from the Master service.

    Returns:
        A ``success`` result with generated KQL and seeded hits.
    """
    kql = generate_fixture_kql(request.natural_language)
    hits = fixture_log_hits(request.natural_language)[: request.max_results]
    return LogRetrievalResult(
        query_id=request.query_id,
        status="success",
        kql_generated=kql,
        syntax_valid=True,
        refinement_attempts=0,
        hits=hits,
        hit_count=len(hits),
        error=None,
    )
