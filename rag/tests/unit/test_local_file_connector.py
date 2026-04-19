from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path

from pypdf import PdfWriter

from src.connectors.local_file import LocalFileConnector


def test_local_file_connector_fetch_returns_three_documents(tmp_path: Path):
    docs_dir = tmp_path / "docs"
    docs_dir.mkdir()

    (docs_dir / "runbook.md").write_text(
        "---\n"
        "title: Payments DB Failover\n"
        "author: ops-team\n"
        "source_uri: confluence://runbooks/payments-db-failover\n"
        "created_at: 2026-04-01T10:00:00Z\n"
        "updated_at: 2026-04-03T11:30:00Z\n"
        "doc_id: runbook-001\n"
        "---\n"
        "Trigger failover to db-replica-1.\n",
        encoding="utf-8",
    )
    (docs_dir / "notes.txt").write_text(
        "Temporary mitigation details for connection exhaustion.\n",
        encoding="utf-8",
    )

    pdf_path = docs_dir / "report.pdf"
    writer = PdfWriter()
    writer.add_blank_page(width=72, height=72)
    with pdf_path.open("wb") as handle:
        writer.write(handle)

    connector = LocalFileConnector(docs_dir)

    documents = asyncio.run(connector.fetch(since=None))

    assert len(documents) == 3
    assert [document.source_type for document in documents] == ["runbook", "runbook", "runbook"]

    markdown_doc = next(document for document in documents if document.doc_id == "runbook-001")
    assert markdown_doc.title == "Payments DB Failover"
    assert markdown_doc.author == "ops-team"
    assert markdown_doc.source_uri == "confluence://runbooks/payments-db-failover"
    assert markdown_doc.raw_text == "Trigger failover to db-replica-1."

    text_doc = next(document for document in documents if document.doc_id == "notes")
    assert text_doc.source_uri == "notes.txt"
    assert "connection exhaustion" in text_doc.raw_text

    pdf_doc = next(document for document in documents if document.doc_id == "report")
    assert pdf_doc.source_uri == "report.pdf"
    assert pdf_doc.raw_text == ""


def test_local_file_connector_fetch_respects_since_filter(tmp_path: Path):
    docs_dir = tmp_path / "docs"
    docs_dir.mkdir()

    (docs_dir / "old.md").write_text(
        "---\nupdated_at: 2026-04-01T00:00:00Z\n---\nOld content.\n",
        encoding="utf-8",
    )
    (docs_dir / "new.md").write_text(
        "---\nupdated_at: 2026-04-10T00:00:00Z\n---\nNew content.\n",
        encoding="utf-8",
    )

    connector = LocalFileConnector(docs_dir)

    documents = asyncio.run(connector.fetch(since=datetime(2026, 4, 5, tzinfo=UTC)))

    assert [document.doc_id for document in documents] == ["new"]
