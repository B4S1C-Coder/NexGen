from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Literal

SourceType = Literal["runbook", "jira", "slack", "github"]


@dataclass(slots=True)
class RawDocument:
    """Normalized source document returned by RAG connectors.

    Parameters:
        doc_id: Stable source-specific identifier.
        source_type: Connector source classification.
        source_uri: Canonical source reference or fallback file name.
        title: Human-readable title for the document.
        raw_text: Extracted source content.
        created_at: Source creation time.
        updated_at: Source last modification time.
        author: Source author or owner.
        metadata: Additional source-specific fields.
    """

    doc_id: str
    source_type: SourceType
    source_uri: str
    title: str
    raw_text: str
    created_at: datetime
    updated_at: datetime
    author: str
    metadata: dict[str, object] = field(default_factory=dict)


class BaseConnector(ABC):
    """Abstract base class for every RAG document connector."""

    @abstractmethod
    async def fetch(self, since: datetime | None) -> list[RawDocument]:
        """Fetch normalized source documents newer than ``since``.

        Parameters:
            since: Optional lower bound used for incremental syncing.

        Returns:
            A list of ``RawDocument`` values ready for preprocessing.
        """

    @abstractmethod
    def source_type(self) -> SourceType:
        """Return the source classification emitted by the connector."""


def ensure_utc(value: datetime) -> datetime:
    """Normalize datetimes to UTC for connector comparisons and storage."""

    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
