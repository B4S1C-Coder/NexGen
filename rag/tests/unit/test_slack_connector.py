import pytest
from datetime import datetime, UTC
from unittest.mock import AsyncMock, patch
from rag.src.connectors.slack import SlackConnector

@pytest.fixture
def slack_connector():
    return SlackConnector(bot_token="xoxb-test-token")

@pytest.mark.asyncio
async def test_slack_fetch_no_auth():
    connector = SlackConnector(bot_token=None)
    docs = await connector.fetch(None)
    assert len(docs) == 0

@pytest.mark.asyncio
@patch("httpx.AsyncClient.get")
async def test_slack_fetch_threads(mock_get, slack_connector):
    def mock_get_side_effect(url, params, headers):
        mock_resp = AsyncMock()
        mock_resp.raise_for_status.return_value = None
        
        if "conversations.list" in url:
            mock_resp.json.return_value = {
                "ok": True,
                "channels": [{"id": "C123", "name": "general"}]
            }
        elif "conversations.history" in url:
            mock_resp.json.return_value = {
                "ok": True,
                "messages": [
                    {"ts": "100.0", "thread_ts": "100.0", "user": "U1", "text": "Parent message"}
                ]
            }
        elif "conversations.replies" in url:
            mock_resp.json.return_value = {
                "ok": True,
                "messages": [
                    {"ts": "100.0", "user": "U1", "text": "Parent message"},
                    {"ts": "101.0", "user": "U2", "text": "Reply message"}
                ]
            }
            
        return mock_resp

    mock_get.side_effect = mock_get_side_effect

    docs = await slack_connector.fetch(since=None)
    
    assert len(docs) == 1
    doc = docs[0]
    
    assert doc.doc_id == "slack-C123-100.0"
    assert "Parent message" in doc.raw_text
    assert "Reply message" in doc.raw_text
    assert doc.author == "U1"
    assert doc.metadata["resolution_status"] == "unknown"
