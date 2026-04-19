from qdrant_client import QdrantClient

from src.qdrant_setup import ensure_qdrant_collections


def test_ensure_qdrant_collections_creates_dense_and_sparse_collections():
    client = QdrantClient(location=":memory:")

    ensure_qdrant_collections(
        client,
        dense_collection="dense_test",
        sparse_collection="sparse_test",
    )

    collection_names = {collection.name for collection in client.get_collections().collections}

    assert "dense_test" in collection_names
    assert "sparse_test" in collection_names
