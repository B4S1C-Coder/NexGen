"""Few-shot example seeding script for the NL-to-KQL pipeline.

Reads NLQ→KQL pairs from data/fallback_examples.jsonl, embeds each
natural language question using nomic-embed-text via Ollama, and
upserts the resulting vectors into the nexgen_few_shot Qdrant collection.

Prerequisites:
    1. Qdrant running (docker compose up qdrant -d)
    2. Ollama running with nomic-embed-text pulled
    3. init_qdrant.py has been run first

Usage:
    cd query
    python -m scripts.seed_few_shot

Defined in TASKS.md P1-Q3.
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import httpx
from qdrant_client import QdrantClient
from qdrant_client.http import models as qdrant_models

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

QDRANT_URL = "http://localhost:6333"
OLLAMA_URL = "http://localhost:11434"
EMBED_MODEL = "nomic-embed-text"
FEW_SHOT_COLLECTION = "nexgen_few_shot"

# Path to the fallback examples file — relative to project root
EXAMPLES_PATH = Path(__file__).parent.parent / "data" / "fallback_examples.jsonl"


# ---------------------------------------------------------------------------
# Embedding
# ---------------------------------------------------------------------------

def embed_text(text: str) -> list[float]:
    """Get a vector embedding for a text string using Ollama.

    Calls the Ollama /api/embeddings endpoint with nomic-embed-text.
    Returns a list of 768 floats representing the semantic meaning
    of the input text.

    Args:
        text: The natural language string to embed.

    Returns:
        List of 768 floats (the embedding vector).

    Raises:
        SystemExit: If Ollama is unreachable or returns an error.
    """
    try:
        response = httpx.post(
            f"{OLLAMA_URL}/api/embeddings",
            json={"model": EMBED_MODEL, "prompt": text},
            timeout=30.0,
        )
        response.raise_for_status()
        return response.json()["embedding"]
    except httpx.ConnectError:
        print(f"ERROR: Cannot reach Ollama at {OLLAMA_URL}.")
        print("Make sure Ollama is running: ollama serve")
        sys.exit(1)
    except Exception as exc:
        print(f"ERROR: Embedding failed for text '{text[:50]}...': {exc}")
        sys.exit(1)


# ---------------------------------------------------------------------------
# Loading examples
# ---------------------------------------------------------------------------

def load_examples(path: Path) -> list[dict]:
    """Load NLQ→KQL pairs from a JSONL file.

    Each line must be a JSON object with at least 'id', 'nl', and 'kql' keys.

    Args:
        path: Path to the .jsonl file.

    Returns:
        List of dicts, one per example.

    Raises:
        SystemExit: If the file does not exist or cannot be parsed.
    """
    if not path.exists():
        print(f"ERROR: Examples file not found at {path}")
        sys.exit(1)

    examples = []
    with open(path, encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                examples.append(json.loads(line))
            except json.JSONDecodeError as exc:
                print(f"ERROR: Invalid JSON on line {line_num}: {exc}")
                sys.exit(1)

    print(f"Loaded {len(examples)} examples from {path.name}")
    return examples


# ---------------------------------------------------------------------------
# Seeding
# ---------------------------------------------------------------------------

def seed_collection(
    client: QdrantClient,
    examples: list[dict],
) -> int:
    """Embed each example and upsert into the Qdrant collection.

    Each point stored in Qdrant contains:
    - vector: 768-dim embedding of the natural language question
    - payload: the full example dict (id, nl, kql)

    Args:
        client: An initialised QdrantClient instance.
        examples: List of example dicts with id, nl, kql keys.

    Returns:
        Number of points successfully upserted.
    """
    points = []

    for i, example in enumerate(examples):
        example_id = example.get("id", f"fe-{i:03d}")
        nl_text = example["nl"]
        kql_text = example["kql"]

        print(f"  [{i + 1}/{len(examples)}] Embedding: {nl_text[:60]}...")
        vector = embed_text(nl_text)

        # Small delay to avoid overwhelming Ollama
        time.sleep(0.1)

        points.append(
            qdrant_models.PointStruct(
                # Use numeric ID for Qdrant (hash the string ID)
                id=i,
                vector=vector,
                payload={
                    "id": example_id,
                    "nl": nl_text,
                    "kql": kql_text,
                },
            )
        )

    # Upsert all points in one batch call
    client.upsert(
        collection_name=FEW_SHOT_COLLECTION,
        points=points,
    )

    return len(points)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """Connect to Qdrant and Ollama, embed examples, upsert into collection.

    Returns:
        None
    """
    print(f"Connecting to Qdrant at {QDRANT_URL} ...")
    client = QdrantClient(url=QDRANT_URL, check_compatibility=False)

    # Verify collection exists
    existing = [c.name for c in client.get_collections().collections]
    if FEW_SHOT_COLLECTION not in existing:
        print(
            f"ERROR: Collection '{FEW_SHOT_COLLECTION}' does not exist. "
            "Run init_qdrant.py first."
        )
        sys.exit(1)

    print(f"Collection '{FEW_SHOT_COLLECTION}' found.")

    # Verify Ollama is reachable
    print(f"Checking Ollama at {OLLAMA_URL} ...")
    try:
        resp = httpx.get(f"{OLLAMA_URL}/api/tags", timeout=5.0)
        resp.raise_for_status()
        print("Ollama is reachable.")
    except Exception:
        print(f"ERROR: Cannot reach Ollama at {OLLAMA_URL}")
        print("Make sure Ollama is running: ollama serve")
        sys.exit(1)

    # Load examples
    examples = load_examples(EXAMPLES_PATH)

    # Embed and seed
    print(f"\nEmbedding and upserting {len(examples)} examples ...")
    count = seed_collection(client, examples)

    # Verify final point count
    info = client.get_collection(FEW_SHOT_COLLECTION)
    print(f"\nSeeding complete. Points in collection: {info.points_count}")
    assert info.points_count == count, (
        f"Expected {count} points but found {info.points_count}"
    )
    print("Verification passed.")


if __name__ == "__main__":
    main()