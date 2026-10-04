"""Few-Shot Selector — Stage 2 of the NL-to-KQL pipeline.

Picks the NLQ→KQL examples from data/few_shot_examples.jsonl that share the
most words with the user's question, to show the LLM the expected KQL style.
Word overlap is used instead of embeddings: with a few dozen curated examples
it is good enough, needs no vector store, and is easy to reason about.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

EXAMPLES_PATH = Path(__file__).parent.parent / "data" / "few_shot_examples.jsonl"
TOP_K = 4


@dataclass
class FewShotExample:
    """A single NLQ→KQL demonstration example.

    Attributes:
        nl:    The natural language question.
        kql:   The correct Kibana KQL answer.
        score: Number of words shared with the user's question (None if not scored).
    """

    nl: str
    kql: str
    score: float | None = None


class FewShotSelector:
    """Returns the examples most similar (by shared words) to a question.

    Usage:
        selector = FewShotSelector()
        await selector.startup()
        examples = await selector.select("show me payment errors")
    """

    def __init__(self, path: Path = EXAMPLES_PATH, top_k: int = TOP_K) -> None:
        self._path = path
        self._top_k = top_k
        self._examples: list[FewShotExample] = []

    async def startup(self) -> None:
        """Load the examples file once at service start."""
        self._examples = _load_examples(self._path)
        logger.info("FewShotSelector loaded %d examples.", len(self._examples))

    async def shutdown(self) -> None:
        """Nothing to release; kept so main.py can treat all stages alike."""

    async def select(self, natural_language: str) -> list[FewShotExample]:
        """Return up to TOP_K examples, most shared words first (ties keep file order).

        Args:
            natural_language: The user's natural language query string.

        Returns:
            List of FewShotExample with ``score`` set to the shared-word count.
        """
        words = _words(natural_language)
        scored = [
            FewShotExample(nl=ex.nl, kql=ex.kql, score=float(len(words & _words(ex.nl))))
            for ex in self._examples
        ]
        scored.sort(key=lambda ex: ex.score or 0.0, reverse=True)
        return scored[: self._top_k]


def _words(text: str) -> set[str]:
    """Lower-case words of 3+ characters (drops 'a', 'of', 'in', ...)."""
    return {w for w in re.findall(r"[a-z0-9_.-]+", text.lower()) if len(w) >= 3}


def _load_examples(path: Path) -> list[FewShotExample]:
    """Load NLQ→KQL pairs from a JSONL file; missing file or bad lines are skipped.

    Args:
        path: Path to the JSONL file with ``nl`` and ``kql`` keys per line.

    Returns:
        List of FewShotExample with score=None.
    """
    if not path.exists():
        logger.warning("Few-shot examples file not found: %s", path)
        return []
    examples = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            obj = json.loads(line)
            examples.append(FewShotExample(nl=obj["nl"], kql=obj["kql"]))
        except (json.JSONDecodeError, KeyError):
            continue
    return examples
