from __future__ import annotations

from datetime import UTC, datetime

from src.connectors.base import RawDocument
from src.preprocessor import Preprocessor


def _raw_document(
    *,
    source_type: str = "runbook",
    raw_text: str = "content",
    metadata: dict[str, object] | None = None,
) -> RawDocument:
    return RawDocument(
        doc_id=f"{source_type}-doc",
        source_type=source_type,  # type: ignore[arg-type]
        source_uri=f"{source_type}://doc",
        title=f"{source_type} title",
        raw_text=raw_text,
        created_at=datetime(2026, 4, 19, tzinfo=UTC),
        updated_at=datetime(2026, 4, 19, tzinfo=UTC),
        author="tester",
        metadata=metadata or {},
    )


def test_tag_technical_ids_wraps_ip_address():
    preprocessor = Preprocessor()

    tagged = preprocessor.tag_technical_ids("client ip 192.168.1.1 could not connect")

    assert "<IP_ADDR:192.168.1.1>" in tagged


def test_chunk_runbook_of_1000_tokens_yields_expected_chunk_count():
    preprocessor = Preprocessor()
    doc = _raw_document(
        source_type="runbook",
        raw_text=" ".join(f"token{i}" for i in range(1000)),
    )

    chunks = preprocessor.chunk(doc)

    assert len(chunks) == 3
    assert chunks[0].chunk_id == "runbook-doc-chunk-0"
    assert chunks[-1].chunk_id == "runbook-doc-chunk-2"


def test_enrich_metadata_assigns_authority_tier_by_source_type():
    preprocessor = Preprocessor()

    cases = [
        ("runbook", {}, "A"),
        ("github", {"pr_merged": True}, "A"),
        ("github", {"pr_merged": False}, "B"),
        ("jira", {"resolution_status": "resolved", "is_accepted_answer": True}, "A"),
        ("jira", {"resolution_status": "open"}, "B"),
        ("slack", {}, "B"),
    ]

    for source_type, metadata, expected_tier in cases:
        doc = _raw_document(source_type=source_type, metadata=metadata)
        chunk = preprocessor.chunk(doc)[0]

        enriched = preprocessor.enrich_metadata(chunk, doc)

        assert enriched.authority_tier == expected_tier

