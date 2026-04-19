from __future__ import annotations

from qdrant_client import QdrantClient, models

SPARSE_VECTOR_NAME = "text"
DENSE_VECTOR_SIZE = 768


def ensure_qdrant_collections(
    client: QdrantClient,
    dense_collection: str,
    sparse_collection: str,
    dense_vector_size: int = DENSE_VECTOR_SIZE,
) -> None:
    """Create the dense and sparse Qdrant collections when they do not exist.

    Parameters:
        client: Qdrant client used for collection management.
        dense_collection: Collection name for dense vectors.
        sparse_collection: Collection name for sparse vectors.
        dense_vector_size: Dense embedding dimensionality.
    """

    existing = {collection.name for collection in client.get_collections().collections}

    if dense_collection not in existing:
        client.create_collection(
            collection_name=dense_collection,
            vectors_config=models.VectorParams(
                size=dense_vector_size,
                distance=models.Distance.COSINE,
            ),
        )

    if sparse_collection not in existing:
        client.create_collection(
            collection_name=sparse_collection,
            vectors_config={},
            sparse_vectors_config={
                SPARSE_VECTOR_NAME: models.SparseVectorParams(
                    index=models.SparseIndexParams(),
                ),
            },
        )
