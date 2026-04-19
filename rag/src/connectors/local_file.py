from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import frontmatter
from pypdf import PdfReader

from .base import BaseConnector, RawDocument, SourceType, ensure_utc


class LocalFileConnector(BaseConnector):
    """Load local Markdown, text, and PDF documents from the docs directory."""

    SUPPORTED_EXTENSIONS = {".md", ".txt", ".pdf"}

    def __init__(self, docs_path: str | Path = "data/docs") -> None:
        """Create a local connector rooted at ``docs_path``.

        Parameters:
            docs_path: Directory containing local fallback documents.
        """

        self.docs_path = Path(docs_path)

    def source_type(self) -> SourceType:
        """Return the source type used for local fallback documents."""

        return "runbook"

    async def fetch(self, since: datetime | None) -> list[RawDocument]:
        """Read supported files under the docs directory into ``RawDocument`` items.

        Parameters:
            since: Optional lower bound; files updated on or before it are skipped.

        Returns:
            A sorted list of normalized local documents.
        """

        if not self.docs_path.exists():
            return []

        normalized_since = ensure_utc(since) if since is not None else None
        documents: list[RawDocument] = []

        for path in sorted(self.docs_path.rglob("*")):
            if not path.is_file() or path.suffix.lower() not in self.SUPPORTED_EXTENSIONS:
                continue

            document = self._build_document(path)
            if normalized_since is not None and document.updated_at <= normalized_since:
                continue
            documents.append(document)

        return documents

    def _build_document(self, path: Path) -> RawDocument:
        """Construct a ``RawDocument`` from a single local file path."""

        stat = path.stat()
        default_created = datetime.fromtimestamp(stat.st_ctime, tz=UTC)
        default_updated = datetime.fromtimestamp(stat.st_mtime, tz=UTC)

        metadata: dict[str, Any]
        raw_text: str

        if path.suffix.lower() == ".pdf":
            metadata = {}
            raw_text = self._extract_pdf_text(path)
        else:
            post = frontmatter.load(path)
            metadata = dict(post.metadata)
            raw_text = post.content.strip()

        created_at = self._parse_datetime(metadata.get("created_at"), default_created)
        updated_at = self._parse_datetime(metadata.get("updated_at"), default_updated)
        title = str(metadata.get("title") or path.stem)
        author = str(metadata.get("author") or "unknown")
        source_uri = str(metadata.get("source_uri") or path.name)
        doc_id = str(metadata.get("doc_id") or path.stem)

        return RawDocument(
            doc_id=doc_id,
            source_type=self.source_type(),
            source_uri=source_uri,
            title=title,
            raw_text=raw_text,
            created_at=created_at,
            updated_at=updated_at,
            author=author,
            metadata=metadata,
        )

    def _extract_pdf_text(self, path: Path) -> str:
        """Extract PDF text content, ignoring blank pages."""

        reader = PdfReader(str(path))
        texts = [(page.extract_text() or "").strip() for page in reader.pages]
        return "\n".join(text for text in texts if text).strip()

    def _parse_datetime(self, raw_value: object, default: datetime) -> datetime:
        """Parse metadata timestamps and normalize them to UTC."""

        if raw_value is None:
            return default
        if isinstance(raw_value, datetime):
            return ensure_utc(raw_value)
        if isinstance(raw_value, str):
            candidate = raw_value.replace("Z", "+00:00")
            return ensure_utc(datetime.fromisoformat(candidate))
        return default
