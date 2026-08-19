from datetime import datetime, timezone

import httpx
import pytest
from nexgen_shared.schemas import RCAReport

from tui.client import NexGenClient


@pytest.mark.asyncio
async def test_investigate_posts_user_query(monkeypatch: pytest.MonkeyPatch) -> None:
    """The TUI client must POST a valid UserQuery to Master /query."""
    captured: dict = {}

    class FakeResponse:
        status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {
                "query_id": captured["json"]["query_id"],
                "root_cause_summary": "db-primary unreachable",
                "confidence": 0.86,
                "evidence": [],
                "recommended_actions": ["failover"],
                "reasoning_trace_summary": "ok",
                "mttr_estimate_minutes": 5,
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }

    class FakeAsyncClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        async def __aenter__(self) -> "FakeAsyncClient":
            return self

        async def __aexit__(self, *args) -> None:
            return None

        async def post(self, url: str, json: dict) -> FakeResponse:
            captured["url"] = url
            captured["json"] = json
            return FakeResponse()

    monkeypatch.setattr(httpx, "AsyncClient", FakeAsyncClient)
    client = NexGenClient()
    report = await client.investigate("Why did payments fail?", "sess-1")
    assert isinstance(report, RCAReport)
    assert captured["url"].endswith("/query")
    assert captured["json"]["raw_text"] == "Why did payments fail?"
    assert captured["json"]["session_id"] == "sess-1"
    assert client.last_query_id == report.query_id
