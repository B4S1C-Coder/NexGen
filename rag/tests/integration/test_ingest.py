from __future__ import annotations

import asyncio
from pathlib import Path

from fastapi.testclient import TestClient
from qdrant_client import QdrantClient

from src.connectors.local_file import LocalFileConnector
from src.ingest_service import IngestService, deterministic_test_vector
from src.main import app
from src.preprocessor import Preprocessor
from src.qdrant_setup import ensure_qdrant_collections


class _FakeEmbedder:
    async def embed_texts(self, texts: list[str]) -> list[list[float]]:
        return [deterministic_test_vector(text) for text in texts]


def test_ingest_indexes_fixture_docs_into_dense_collection(tmp_path: Path):
    docs_dir = tmp_path / "docs"
    docs_dir.mkdir()

    for index in range(5):
        (docs_dir / f"doc-{index}.md").write_text(
            "---\n"
            f"title: Fixture {index}\n"
            f"doc_id: fixture-{index}\n"
            "---\n"
            f"Service payments saw ERR_CONN_RESET on 10.0.0.{index + 1}.\n",
            encoding="utf-8",
        )

    connector = LocalFileConnector(docs_dir)
    preprocessor = Preprocessor()
    documents = asyncio.run(connector.fetch(None))
    expected_chunk_count = sum(len(preprocessor.chunk(document)) for document in documents)

    qdrant_client = QdrantClient(location=":memory:")
    ensure_qdrant_collections(qdrant_client, "dense_test", "sparse_test")

    service = IngestService(
        qdrant_client=qdrant_client,
        connectors={"local_file": connector},
        preprocessor=preprocessor,
        embedder=_FakeEmbedder(),
        dense_collection="dense_test",
        sparse_collection="sparse_test",
    )

    before_count = qdrant_client.count("dense_test", exact=True).count

    with TestClient(app) as client:
        client.app.state.ingest_service = service
        response = client.post("/ingest", json={"source_type": "local_file", "full_reindex": True})

    after_dense_count = qdrant_client.count("dense_test", exact=True).count
    after_sparse_count = qdrant_client.count("sparse_test", exact=True).count

    assert response.status_code == 200
    assert response.json()["status"] == "success"
    assert response.json()["documents_indexed"] == 5
    assert response.json()["chunks_indexed"] == expected_chunk_count
    assert after_dense_count - before_count == expected_chunk_count
    assert after_sparse_count == expected_chunk_count
