from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime

from .connectors.base import RawDocument, ensure_utc


@dataclass(slots=True)
class ProcessedChunk:
    """A normalized text chunk produced from a raw source document.

    Parameters:
        chunk_id: Stable chunk identifier derived from the source document.
        doc_id: Parent document identifier.
        content: Chunk text after technical-ID tagging.
        ordinal: Zero-based chunk position within the document.
    """

    chunk_id: str
    doc_id: str
    content: str
    ordinal: int


@dataclass(slots=True)
class ChunkMetadata:
    """Metadata attached to a processed chunk before indexing.

    Parameters:
        chunk_id: Stable chunk identifier.
        doc_id: Parent document identifier.
        source_type: Source class of the parent document.
        source_uri: Source reference for citations.
        authority_tier: ``A`` for authoritative sources, ``B`` otherwise.
        created_at: Original document creation timestamp in UTC.
        resolution_status: ``open``, ``resolved``, ``deprecated``, or ``unknown``.
        is_accepted_answer: Whether the source represents an accepted fix.
        recency_score: Index-time recency prior to query-time decay.
    """

    chunk_id: str
    doc_id: str
    source_type: str
    source_uri: str
    authority_tier: str
    created_at: datetime
    resolution_status: str
    is_accepted_answer: bool
    recency_score: float


class Preprocessor:
    """Prepare raw source documents for retrieval-time indexing."""

    _RUNBOOK_CHUNK_SIZE = 512
    _RUNBOOK_OVERLAP = 64
    _JIRA_CHUNK_SIZE = 256
    _SLACK_CHUNK_SIZE = 256

    _IP_PATTERN = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
    _TRACE_PATTERN = re.compile(
        r"\b(?:trace|trace_id|trace-id|span|span_id|span-id)[=:]\s*([A-Za-z0-9-]{6,64})\b",
        re.IGNORECASE,
    )
    _HASH_PATTERN = re.compile(r"\b[0-9a-f]{7,40}\b")
    _PATH_PATTERN = re.compile(r"(?<!<)(?:/[^\s<>]+)+")
    _ERROR_CODE_PATTERN = re.compile(r"\b(?:ERR|E)[A-Z0-9_]{2,}\b")

    def chunk(self, doc: RawDocument) -> list[ProcessedChunk]:
        """Split a raw document into processed chunks using source-specific rules.

        Parameters:
            doc: Source document to chunk.

        Returns:
            A list of ``ProcessedChunk`` values ready for metadata enrichment.
        """

        text = doc.raw_text.strip()
        if not text:
            return []

        if doc.source_type == "slack":
            text = self.disentangle(text)

        tagged_text = self.tag_technical_ids(text)

        if doc.source_type in {"runbook", "github"}:
            raw_chunks = self._token_chunks(
                tagged_text,
                chunk_size=self._RUNBOOK_CHUNK_SIZE,
                overlap=self._RUNBOOK_OVERLAP,
            )
        elif doc.source_type == "jira":
            raw_chunks = self._jira_chunks(tagged_text)
        elif doc.source_type == "slack":
            raw_chunks = self._slack_chunks(tagged_text)
        else:
            raw_chunks = [tagged_text]

        return [
            ProcessedChunk(
                chunk_id=f"{doc.doc_id}-chunk-{index}",
                doc_id=doc.doc_id,
                content=chunk_text,
                ordinal=index,
            )
            for index, chunk_text in enumerate(raw_chunks)
            if chunk_text.strip()
        ]

    def tag_technical_ids(self, text: str) -> str:
        """Wrap technical identifiers in preservable tags before embedding.

        Parameters:
            text: Raw document text.

        Returns:
            The tagged text with IPs, trace IDs, hashes, file paths, and error
            codes wrapped in ``<TAG:value>`` markers.
        """

        patterns = [
            ("IP_ADDR", self._IP_PATTERN, 0),
            ("TRACE_ID", self._TRACE_PATTERN, 1),
            ("HASH", self._HASH_PATTERN, 0),
            ("PATH", self._PATH_PATTERN, 0),
            ("ERROR_CODE", self._ERROR_CODE_PATTERN, 0),
        ]

        matches: list[tuple[int, int, str, str]] = []
        occupied: list[tuple[int, int]] = []

        for tag_name, pattern, group_index in patterns:
            for match in pattern.finditer(text):
                start, end = match.span(group_index)
                if any(not (end <= occ_start or start >= occ_end) for occ_start, occ_end in occupied):
                    continue
                matches.append((start, end, tag_name, match.group(group_index)))
                occupied.append((start, end))

        matches.sort(key=lambda item: item[0])

        result: list[str] = []
        cursor = 0
        for start, end, tag_name, value in matches:
            if start < cursor:
                continue
            result.append(text[cursor:start])
            result.append(f"<{tag_name}:{value}>")
            cursor = end
        result.append(text[cursor:])
        return "".join(result)

    def enrich_metadata(self, chunk: ProcessedChunk, doc: RawDocument) -> ChunkMetadata:
        """Derive retrieval metadata for a processed chunk.

        Parameters:
            chunk: Chunk produced from ``chunk()``.
            doc: Parent raw document.

        Returns:
            A ``ChunkMetadata`` instance ready for indexing.
        """

        resolution_status = self._resolution_status(doc)
        is_accepted_answer = self._is_accepted_answer(doc)

        return ChunkMetadata(
            chunk_id=chunk.chunk_id,
            doc_id=doc.doc_id,
            source_type=doc.source_type,
            source_uri=doc.source_uri,
            authority_tier=self._authority_tier(doc, resolution_status, is_accepted_answer),
            created_at=ensure_utc(doc.created_at),
            resolution_status=resolution_status,
            is_accepted_answer=is_accepted_answer,
            recency_score=1.0,
        )

    def disentangle(self, raw_text: str) -> str:
        """Return Slack text unchanged until the real disentanglement model exists."""

        return raw_text

    def _jira_chunks(self, text: str) -> list[str]:
        """Split Jira content by comment boundaries and token-limit large comments."""

        sections = [section.strip() for section in re.split(r"\n\s*\n", text) if section.strip()]
        chunks: list[str] = []
        for section in sections or [text]:
            chunks.extend(self._token_chunks(section, chunk_size=self._JIRA_CHUNK_SIZE, overlap=0))
        return chunks

    def _slack_chunks(self, text: str) -> list[str]:
        """Approximate Slack burst windowing with paragraph-level grouping."""

        bursts = [burst.strip() for burst in re.split(r"\n\s*\n", text) if burst.strip()]
        chunks: list[str] = []
        for burst in bursts or [text]:
            chunks.extend(self._token_chunks(burst, chunk_size=self._SLACK_CHUNK_SIZE, overlap=0))
        return chunks

    def _token_chunks(self, text: str, chunk_size: int, overlap: int) -> list[str]:
        """Split text into token-like word windows with optional overlap."""

        tokens = text.split()
        if not tokens:
            return []

        step = max(1, chunk_size - overlap)
        chunks: list[str] = []
        for start in range(0, len(tokens), step):
            window = tokens[start : start + chunk_size]
            if not window:
                continue
            chunks.append(" ".join(window))
            if start + chunk_size >= len(tokens):
                break
        return chunks

    def _resolution_status(self, doc: RawDocument) -> str:
        """Infer a normalized resolution status from source metadata."""

        metadata = {str(key): value for key, value in doc.metadata.items()}
        raw_status = (
            metadata.get("resolution_status")
            or metadata.get("jira_status")
            or metadata.get("status")
        )
        if isinstance(raw_status, str):
            normalized = raw_status.strip().lower()
            if normalized in {"resolved", "done", "closed"}:
                return "resolved"
            if normalized in {"deprecated", "obsolete"}:
                return "deprecated"
            if normalized in {"open", "todo", "in_progress", "in progress"}:
                return "open"
        return "unknown"

    def _is_accepted_answer(self, doc: RawDocument) -> bool:
        """Infer whether a document reflects an accepted resolution."""

        accepted = doc.metadata.get("is_accepted_answer")
        if isinstance(accepted, bool):
            return accepted
        return False

    def _authority_tier(
        self,
        doc: RawDocument,
        resolution_status: str,
        is_accepted_answer: bool,
    ) -> str:
        """Assign authority tier based on the source type and status."""

        if doc.source_type == "runbook":
            return "A"
        if doc.source_type == "github":
            return "A" if bool(doc.metadata.get("pr_merged")) else "B"
        if doc.source_type == "jira":
            return "A" if resolution_status == "resolved" and is_accepted_answer else "B"
        return "B"
