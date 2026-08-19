from nexgen_shared.schemas import LogRetrievalRequest, SchemaContextPayload, TimeRange

from src.dev_fixtures import generate_fixture_kql, mock_retrieve


def test_generate_fixture_kql_includes_payments_and_status() -> None:
    """Payments outage phrasing must produce a service + 500 KQL clause."""
    kql = generate_fixture_kql("Why did payments fail with HTTP 500?")
    assert "service.name: payments" in kql
    assert "http.status_code: 500" in kql


def test_mock_retrieve_returns_success_hits() -> None:
    """Fixture retrieve must return a schema-valid success payload with hits."""
    request = LogRetrievalRequest(
        query_id="q-1",
        natural_language="Show HTTP 500 errors from the payments service",
        index_hints=["payments-*"],
        time_range=TimeRange(**{"from": "now-30m", "to": "now"}),
        max_results=50,
        schema_context=SchemaContextPayload(known_fields=[], value_samples={}),
    )
    result = mock_retrieve(request)
    assert result.status == "success"
    assert result.hit_count >= 1
    assert result.hits[0].service == "payments"
    assert result.error is None
