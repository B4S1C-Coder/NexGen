import pytest
from datetime import datetime, UTC
from unittest.mock import AsyncMock, patch
from rag.src.connectors.jira import JiraConnector

@pytest.fixture
def jira_connector():
    return JiraConnector(base_url="https://test.atlassian.net", api_token="test-token")

@pytest.mark.asyncio
async def test_jira_fetch_no_auth():
    connector = JiraConnector(base_url=None, api_token=None)
    docs = await connector.fetch(None)
    assert len(docs) == 0

@pytest.mark.asyncio
@patch("httpx.AsyncClient.get")
async def test_jira_fetch_issues(mock_get, jira_connector):
    # Mocking httpx response
    mock_response = AsyncMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {
        "issues": [
            {
                "key": "TEST-1",
                "fields": {
                    "summary": "Test Issue",
                    "description": {
                        "type": "text",
                        "text": "This is a test description."
                    },
                    "created": "2023-01-01T10:00:00.000+0000",
                    "updated": "2023-01-02T10:00:00.000+0000",
                    "creator": {"displayName": "Test User"},
                    "resolution": {"name": "Done"},
                    "comment": {
                        "comments": [
                            {
                                "id": "10000",
                                "body": {"type": "text", "text": "This is a flagged comment fix."},
                                "created": "2023-01-02T10:00:00.000+0000",
                                "updated": "2023-01-02T10:00:00.000+0000",
                                "author": {"displayName": "Test User 2"},
                                "properties": [{"key": "is_accepted_answer"}]
                            }
                        ]
                    }
                }
            }
        ]
    }
    mock_get.return_value = mock_response

    docs = await jira_connector.fetch(since=None)
    
    # 1 doc for description, 1 doc for the comment
    assert len(docs) == 2
    
    desc_doc = docs[0]
    assert desc_doc.doc_id == "TEST-1-desc"
    assert desc_doc.raw_text == "This is a test description."
    assert desc_doc.metadata["resolution_status"] == "resolved"
    assert desc_doc.metadata["is_accepted_answer"] is False

    comment_doc = docs[1]
    assert comment_doc.doc_id == "TEST-1-comment-10000"
    assert comment_doc.raw_text == "This is a flagged comment fix."
    assert comment_doc.metadata["resolution_status"] == "resolved"
    assert comment_doc.metadata["is_accepted_answer"] is True
