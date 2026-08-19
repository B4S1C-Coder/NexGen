from tui.runtime import ServiceSpec, build_service_env, REPO_ROOT


def test_build_service_env_enables_mock_on_query() -> None:
    """Query/RAG subprocesses must run with MOCK_SERVICES=true."""
    spec = ServiceSpec("query", REPO_ROOT / "query", 8001, mock=True)
    env = build_service_env(spec)
    assert env["MOCK_SERVICES"] == "true"
    assert str(REPO_ROOT / "query") in env["PYTHONPATH"]
    assert str(REPO_ROOT / "nexgen_shared") in env["PYTHONPATH"]


def test_build_service_env_master_uses_http_not_inline_mock() -> None:
    """Master must call Query/RAG over HTTP rather than in-process mocks."""
    spec = ServiceSpec("master", REPO_ROOT / "master", 8000, mock=False)
    env = build_service_env(spec)
    assert env["MOCK_SERVICES"] == "false"
    assert env["QUERY_SERVICE_URL"] == "http://127.0.0.1:8001"
    assert env["RAG_SERVICE_URL"] == "http://127.0.0.1:8002"
