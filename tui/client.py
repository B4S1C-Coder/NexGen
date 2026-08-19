"""HTTP client used by the TUI to talk to Master (and health-check all three)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

import httpx
from nexgen_shared.schemas import RCAReport, UserQuery


class NexGenClient:
    """Thin async wrapper around the three NexGen HTTP surfaces.

    Args:
        master_url: Base URL for the Master orchestrator.
        query_url: Base URL for the NL-to-KQL service.
        rag_url: Base URL for the RAG service.
        timeout_seconds: Bound for RCA synthesis (includes downstream calls).
    """

    def __init__(
        self,
        master_url: str = "http://127.0.0.1:8000",
        query_url: str = "http://127.0.0.1:8001",
        rag_url: str = "http://127.0.0.1:8002",
        timeout_seconds: float = 60.0,
    ) -> None:
        self.master_url = master_url.rstrip("/")
        self.query_url = query_url.rstrip("/")
        self.rag_url = rag_url.rstrip("/")
        self._timeout = timeout_seconds
        self.last_query_id: str | None = None

    async def health(self) -> dict[str, bool]:
        """Probe ``/health`` on Master, Query, and RAG.

        Returns:
            Mapping of service name to liveness.
        """
        async with httpx.AsyncClient(timeout=2.0) as client:
            result: dict[str, bool] = {}
            for name, url in (
                ("master", f"{self.master_url}/health"),
                ("query", f"{self.query_url}/health"),
                ("rag", f"{self.rag_url}/health"),
            ):
                try:
                    response = await client.get(url)
                    result[name] = response.status_code == 200
                except httpx.HTTPError:
                    result[name] = False
            return result

    async def investigate(self, raw_text: str, session_id: str) -> RCAReport:
        """POST a natural-language question to Master ``/query``.

        Args:
            raw_text: Operator question.
            session_id: Stable session identifier for multi-turn history.

        Returns:
            Parsed ``RCAReport`` from the orchestrator.
        """
        payload = UserQuery(
            query_id=str(uuid4()),
            raw_text=raw_text,
            session_id=session_id,
            timestamp_utc=datetime.now(timezone.utc),
        )
        self.last_query_id = payload.query_id
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.post(
                f"{self.master_url}/query",
                json=payload.model_dump(mode="json"),
            )
            response.raise_for_status()
            return RCAReport.model_validate(response.json())

    async def trace(self, query_id: str) -> list[dict[str, Any]]:
        """Fetch DAG stage events recorded for a query.

        Args:
            query_id: Identifier returned on the RCA report.

        Returns:
            A list of stage dictionaries, possibly empty.
        """
        async with httpx.AsyncClient(timeout=2.0) as client:
            response = await client.get(f"{self.master_url}/query/{query_id}/trace")
            response.raise_for_status()
            return list(response.json().get("events", []))
