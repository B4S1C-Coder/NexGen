from __future__ import annotations

from qdrant_client import QdrantClient

from src.qdrant_setup import ensure_qdrant_collections
from src.settings import Settings


def main() -> None:
    """Create the RAG dense and sparse collections from environment settings."""

    settings = Settings()
    client = QdrantClient(url=settings.qdrant_url)
    ensure_qdrant_collections(
        client,
        dense_collection=settings.dense_collection,
        sparse_collection=settings.sparse_collection,
    )
    print(
        f"Initialized collections: {settings.dense_collection}, {settings.sparse_collection}",
    )


if __name__ == "__main__":
    main()
