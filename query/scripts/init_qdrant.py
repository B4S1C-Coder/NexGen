"""Qdrant collection initialisation script for the NL-to-KQL pipeline.

Creates the nexgen_few_shot collection used by the FewShotSelector.
Run this once before running seed_few_shot.py.

Usage:
    cd query
    python -m scripts.init_qdrant

Defined in TASKS.md P1-Q3.
"""

from __future__ import annotations

import sys

from qdrant_client import QdrantClient
from qdrant_client.http import models as qdrant_models

# ---------------------------------------------------------------------------
# Configuration — must match query/.env values
# ---------------------------------------------------------------------------

QDRANT_URL = "http://localhost:6333"

# Collection name as defined in query.md §5 and AGENTS.md
FEW_SHOT_COLLECTION = "nexgen_few_shot"

# Embedding dimension for nomic-embed-text via Ollama
# nomic-embed-text produces 768-dimensional vectors
EMBEDDING_DIMENSION = 768

# Cosine distance — standard for semantic similarity search
DISTANCE_METRIC = qdrant_models.Distance.COSINE


def create_few_shot_collection(client: QdrantClient) -> bool:
    """Create the nexgen_few_shot collection in Qdrant.

    If the collection already exists, skips creation and returns False.
    If creation succeeds, returns True.

    Args:
        client: An initialised QdrantClient instance.

    Returns:
        True if the collection was created, False if it already existed.
    """
    existing = [c.name for c in client.get_collections().collections]

    if FEW_SHOT_COLLECTION in existing:
        print(f"Collection '{FEW_SHOT_COLLECTION}' already exists — skipping.")
        return False

    client.create_collection(
        collection_name=FEW_SHOT_COLLECTION,
        vectors_config=qdrant_models.VectorParams(
            size=EMBEDDING_DIMENSION,
            distance=DISTANCE_METRIC,
        ),
    )
    print(
        f"Created collection '{FEW_SHOT_COLLECTION}' "
        f"({EMBEDDING_DIMENSION}-dim, cosine distance)."
    )
    return True


def verify_collection(client: QdrantClient) -> None:
    """Print collection info to confirm it exists and is configured correctly.

    Args:
        client: An initialised QdrantClient instance.

    Returns:
        None
    """
    info = client.get_collection(FEW_SHOT_COLLECTION)
    config = info.config.params.vectors
    print(f"Verified collection '{FEW_SHOT_COLLECTION}':")
    print(f"  Dimension : {config.size}")
    print(f"  Distance  : {config.distance}")
    print(f"  Points    : {info.points_count}")


def main() -> None:
    """Entry point — connect to Qdrant and initialise collections.

    Returns:
        None
    """
    print(f"Connecting to Qdrant at {QDRANT_URL} ...")
    client = QdrantClient(url=QDRANT_URL, check_compatibility=False)
    try:
        client.get_collections()
    except Exception as exc:
        print(f"ERROR: Cannot reach Qdrant at {QDRANT_URL}: {exc}")
        sys.exit(1)

    print("Connected successfully.")
    create_few_shot_collection(client)
    verify_collection(client)
    print("Initialisation complete.")


if __name__ == "__main__":
    main()