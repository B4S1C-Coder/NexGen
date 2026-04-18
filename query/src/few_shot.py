"""Few-Shot Selector — Stage 2 of the NL-to-KQL pipeline.

Retrieves semantically similar NLQ→KQL examples from Qdrant to use
as few-shot demonstrations for the KQL Generator.

Falls back to data/fallback_examples.jsonl when Qdrant returns fewer
than MIN_QDRANT_RESULTS results above the similarity threshold.

Defined in TASKS.md P2-Q1.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path

import httpx
from qdrant_client import QdrantClient
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

FALLBACK_PATH = Path(__file__).parent.parent / "data" / "fallback_examples.jsonl"
FEW_SHOT_COLLECTION = "nexgen_few_shot"

# Minimum Qdrant results required before we trust Qdrant over fallback
MIN_QDRANT_RESULTS = 2

# Similarity threshold — cosine similarity must be above this to count
SIMILARITY_THRESHOLD = 0.70


class FewShotSettings(BaseSettings):
    """Configuration for the FewShotSelector."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    qdrant_url: str = "http://localhost:6333"
    ollama_url: str = "http://localhost:11434"
    embed_model: str = "nomic-embed-text"
    few_shot_top_k: int = 4
    few_shot_similarity_threshold: float = 0.70
    few_shot_min_qdrant_results: int = 2


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass
class FewShotExample:
    """A single NLQ→KQL demonstration example.

    Attributes:
        nl:    The natural language question.
        kql:   The correct Kibana KQL answer.
        score: Similarity score from Qdrant (None for fallback examples).
    """

    nl: str
    kql: str
    score: float | None = None


# ---------------------------------------------------------------------------
# FewShotSelector
# ---------------------------------------------------------------------------

class FewShotSelector:
    """Retrieves relevant NLQ→KQL examples for few-shot prompting.

    On startup loads fallback examples from disk. On each select() call,
    embeds the query and searches Qdrant. Falls back to static examples
    if Qdrant returns fewer than MIN_QDRANT_RESULTS above threshold.

    Usage:
        selector = FewShotSelector()
        await selector.startup()
        examples = await selector.select("show me payment errors")
    """

    def __init__(self) -> None:
        self._settings = FewShotSettings()
        self._client: QdrantClient | None = None
        self._fallback: list[FewShotExample] = []

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def startup(self) -> None:
        """Initialise Qdrant client and load fallback examples from disk.

        Should be called once during FastAPI app lifespan startup.
        """
        self._client = QdrantClient(
            url=self._settings.qdrant_url,
            check_compatibility=False,
        )
        self._fallback = _load_fallback_examples(FALLBACK_PATH)
        logger.info(
            "FewShotSelector started. Loaded %d fallback examples.",
            len(self._fallback),
        )

    async def shutdown(self) -> None:
        """Close the Qdrant client connection."""
        if self._client is not None:
            self._client.close()
            logger.info("FewShotSelector shut down.")

    # ------------------------------------------------------------------
    # Core selection
    # ------------------------------------------------------------------

    async def select(self, natural_language: str) -> list[FewShotExample]:
        """Return the most relevant few-shot examples for a query.

        Embeds the natural language query, searches Qdrant for similar
        examples, and returns those above SIMILARITY_THRESHOLD.

        Falls back to static JSONL examples if Qdrant returns fewer than
        MIN_QDRANT_RESULTS results above the threshold.

        Args:
            natural_language: The user's natural language query string.

        Returns:
            List of FewShotExample ordered by relevance (most similar first).
            Always returns between 1 and few_shot_top_k examples.
        """
        if self._client is None:
            logger.warning(
                "FewShotSelector.startup() not called — using fallback only."
            )
            return self._fallback[: self._settings.few_shot_top_k]

        # Embed the query
        try:
            vector = _embed(natural_language, self._settings)
        except Exception as exc:
            logger.warning(
                "Embedding failed (%s) — using fallback examples.", exc
            )
            return self._fallback[: self._settings.few_shot_top_k]

        # Search Qdrant
        try:
            hits = self._client.search(
                collection_name=FEW_SHOT_COLLECTION,
                query_vector=vector,
                limit=self._settings.few_shot_top_k,
                score_threshold=self._settings.few_shot_similarity_threshold,
                with_payload=True,
            )
        except Exception as exc:
            logger.warning(
                "Qdrant search failed (%s) — using fallback examples.", exc
            )
            return self._fallback[: self._settings.few_shot_top_k]

        qdrant_examples = [
            FewShotExample(
                nl=hit.payload["nl"],
                kql=hit.payload["kql"],
                score=hit.score,
            )
            for hit in hits
        ]

        if len(qdrant_examples) >= self._settings.few_shot_min_qdrant_results:
            logger.debug(
                "Qdrant returned %d examples above threshold.",
                len(qdrant_examples),
            )
            return qdrant_examples

        # Not enough Qdrant results — use fallback
        logger.info(
            "Qdrant returned %d results (below MIN=%d) — using fallback.",
            len(qdrant_examples),
            self._settings.few_shot_min_qdrant_results,
        )
        return self._fallback[: self._settings.few_shot_top_k]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _embed(text: str, settings: FewShotSettings) -> list[float]:
    """Get a vector embedding for text using Ollama nomic-embed-text.

    Args:
        text: The natural language string to embed.
        settings: FewShotSettings with ollama_url and embed_model.

    Returns:
        List of 768 floats representing the semantic meaning of text.

    Raises:
        RuntimeError: If Ollama is unreachable or returns an error.
    """
    try:
        response = httpx.post(
            f"{settings.ollama_url}/api/embeddings",
            json={"model": settings.embed_model, "prompt": text},
            timeout=30.0,
        )
        response.raise_for_status()
        return response.json()["embedding"]
    except httpx.ConnectError as exc:
        raise RuntimeError(
            f"Cannot reach Ollama at {settings.ollama_url}. "
            "Is Ollama running? Run: ollama serve"
        ) from exc
    except Exception as exc:
        raise RuntimeError(f"Embedding request failed: {exc}") from exc


def _load_fallback_examples(path: Path) -> list[FewShotExample]:
    """Load NLQ→KQL pairs from the fallback JSONL file.

    Args:
        path: Path to the fallback_examples.jsonl file.

    Returns:
        List of FewShotExample with score=None (static examples).
    """
    if not path.exists():
        logger.error("Fallback examples file not found at %s", path)
        return []

    examples = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                examples.append(
                    FewShotExample(nl=obj["nl"], kql=obj["kql"], score=None)
                )
            except (json.JSONDecodeError, KeyError) as exc:
                logger.warning("Skipping malformed fallback line: %s", exc)

    logger.info("Loaded %d fallback examples from %s", len(examples), path.name)
    return examples