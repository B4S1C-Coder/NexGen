from __future__ import annotations

import base64
from datetime import datetime
from typing import Any

import httpx

from .base import BaseConnector, RawDocument, SourceType, ensure_utc


class JiraConnector(BaseConnector):
    """Fetch issues and comments from Jira Cloud REST API."""

    def __init__(self, base_url: str | None, api_token: str | None) -> None:
        self.base_url = (base_url or "").rstrip("/")
        self.api_token = api_token

    def source_type(self) -> SourceType:
        return "jira"

    async def fetch(self, since: datetime | None) -> list[RawDocument]:
        if not self.base_url or not self.api_token:
            return []

        jql = "order by updated DESC"
        if since is not None:
            since_str = ensure_utc(since).strftime("%Y-%m-%d %H:%M")
            jql = f'updated >= "{since_str}" order by updated DESC'

        documents: list[RawDocument] = []
        async with httpx.AsyncClient(base_url=self.base_url) as client:
            headers = {
                "Authorization": f"Basic {base64.b64encode(self.api_token.encode()).decode()}",
                "Accept": "application/json",
            }
            
            # Fetch issues
            response = await client.get(
                "/rest/api/3/search",
                params={"jql": jql, "maxResults": 50, "fields": "summary,description,created,updated,creator,resolution,comment"},
                headers=headers
            )
            response.raise_for_status()
            data = response.json()

            for issue in data.get("issues", []):
                key = issue.get("key")
                fields = issue.get("fields", {})
                
                title = fields.get("summary") or key
                created_at = self._parse_datetime(fields.get("created"))
                updated_at = self._parse_datetime(fields.get("updated"))
                creator = fields.get("creator", {}).get("displayName", "unknown")
                
                resolution_obj = fields.get("resolution")
                resolution_name = resolution_obj.get("name") if resolution_obj else None
                resolution_status = "resolved" if resolution_name == "Done" else "open"

                # Map issue description
                description = self._extract_text(fields.get("description"))
                if description:
                    documents.append(RawDocument(
                        doc_id=f"{key}-desc",
                        source_type=self.source_type(),
                        source_uri=f"{self.base_url}/browse/{key}",
                        title=title,
                        raw_text=description,
                        created_at=created_at,
                        updated_at=updated_at,
                        author=creator,
                        metadata={
                            "jira_status": resolution_status,
                            "resolution_status": resolution_status,
                            "is_accepted_answer": False
                        }
                    ))

                # Map comments
                comments = fields.get("comment", {}).get("comments", [])
                for comment in comments:
                    comment_id = comment.get("id")
                    c_created = self._parse_datetime(comment.get("created"))
                    c_updated = self._parse_datetime(comment.get("updated"))
                    c_author = comment.get("author", {}).get("displayName", "unknown")
                    c_body = self._extract_text(comment.get("body"))
                    
                    properties = comment.get("properties", [])
                    is_flagged = any(p.get("key") == "is_accepted_answer" for p in properties)
                    
                    is_accepted = resolution_name == "Done" and is_flagged
                    
                    if c_body:
                        documents.append(RawDocument(
                            doc_id=f"{key}-comment-{comment_id}",
                            source_type=self.source_type(),
                            source_uri=f"{self.base_url}/browse/{key}?focusedCommentId={comment_id}",
                            title=f"Comment on {title}",
                            raw_text=c_body,
                            created_at=c_created,
                            updated_at=c_updated,
                            author=c_author,
                            metadata={
                                "jira_status": resolution_status,
                                "resolution_status": resolution_status,
                                "is_accepted_answer": is_accepted
                            }
                        ))

        return documents

    def _parse_datetime(self, raw_value: str | None) -> datetime:
        if not raw_value:
            return ensure_utc(datetime.now())
        # Jira datetime format: 2020-01-01T00:00:00.000+0000
        candidate = raw_value.replace("+0000", "+00:00")
        try:
            return ensure_utc(datetime.fromisoformat(candidate))
        except ValueError:
            return ensure_utc(datetime.now())

    def _extract_text(self, adf_node: Any) -> str:
        # Simplified Atlassian Document Format extractor
        if not adf_node:
            return ""
        if isinstance(adf_node, str):
            return adf_node
        
        text_parts = []
        if isinstance(adf_node, dict):
            if adf_node.get("type") == "text":
                text_parts.append(adf_node.get("text", ""))
            for child in adf_node.get("content", []):
                text_parts.append(self._extract_text(child))
        elif isinstance(adf_node, list):
            for item in adf_node:
                text_parts.append(self._extract_text(item))
                
        return "".join(text_parts).strip()
