"""Step 4: check we have enough data, then shrink the logs to fit the token budget."""

from __future__ import annotations

import tiktoken

from nexgen_shared.schemas import KnowledgeResult, LogHit, LogRetrievalResult, RCASynthesisInput

from src.intent import IntentResult


def log_line(hit: LogHit) -> str:
    """One log hit as a single readable line."""
    time = hit.timestamp.strftime("%H:%M:%S") if hit.timestamp else "?"
    return f"{time} {hit.service} {hit.level} {hit.message}"


class ContextAssembler:
    """Merges the fetched logs and docs into one RCASynthesisInput."""

    def __init__(self, max_tokens: int = 6000) -> None:
        self.max_tokens = max_tokens
        self.encoder = tiktoken.get_encoding("cl100k_base")

    def count_tokens(self, hits: list[LogHit]) -> int:
        """Number of tokens the log lines would take in a prompt."""
        return sum(len(self.encoder.encode(log_line(h))) for h in hits)

    def is_context_sufficient(
        self, intent: IntentResult, logs: LogRetrievalResult | None, docs: KnowledgeResult | None
    ) -> bool:
        """
        Logs are required whenever the intent asks for them (no logs = no evidence).
        Docs are only required for docs-only questions; otherwise they are a bonus.
        """
        if intent.logs_needed and (logs is None or not logs.hits):
            return False
        if intent.docs_needed and not intent.logs_needed and (docs is None or not docs.chunks):
            return False
        return True

    def assemble(
        self, query_id: str, question: str, logs: LogRetrievalResult | None, docs: KnowledgeResult | None
    ) -> RCASynthesisInput:
        """
        Drop repeated log messages (same service, level and text), keeping the first,
        then keep log lines in time order until the token budget is used up.
        """
        unique: list[LogHit] = []
        seen: set[tuple[str | None, str | None, str | None]] = set()
        for hit in sorted(logs.hits if logs else [], key=lambda h: h.timestamp.timestamp() if h.timestamp else 0.0):
            key = (hit.service, hit.level, hit.message)
            if key not in seen:
                seen.add(key)
                unique.append(hit)

        kept: list[LogHit] = []
        used = 0
        for hit in unique:
            cost = self.count_tokens([hit])
            if used + cost > self.max_tokens:
                break
            kept.append(hit)
            used += cost

        return RCASynthesisInput(
            query_id=query_id,
            original_query=question,
            log_evidence=kept,
            knowledge_context=docs.chunks if docs else [],
            reasoning_trace=[],
        )
