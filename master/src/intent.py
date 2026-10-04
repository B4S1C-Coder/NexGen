"""Step 1: decide whether a question needs logs, docs, or both."""

from __future__ import annotations

import re

from openai import AsyncOpenAI
from pydantic import BaseModel

from src.llm import ask_json, load_prompt

# Keyword groups. Order of the checks in classify() matters more than the lists.
QUANTITATIVE = r"\b(how many|count|number of|total|sum|average|avg|error rate|top \d+|list|show|fetch|latest)\b"
CAUSAL = r"\b(why|cause[ds]?|root cause|diagnose|investigate|what happened|went wrong|help)\b"
SYMPTOM = r"\b(fail\w*|errors?|down|outage|broken|slow|timing out|timeouts?|crash\w*|spike|5\d\d|5xx|429s?|oom|wrong|can't|cannot|not working)\b"
DOCS = r"\b(best practice|how (do|to|should)|what is|what's|what does|explain|describe|runbook|documentation|docs?|guide|recommended|policy|who owns|configure|where is)\b"


class IntentResult(BaseModel):
    """Routing decision for one question."""

    logs_needed: bool
    docs_needed: bool
    is_quantitative: bool = False
    index_hints: list[str] = []  # e.g. ["payments-*"]; first one is the service asked about
    used_llm: bool = False


class IntentClassifier:
    """Keyword rules first; the LLM is only asked when no rule matches."""

    def __init__(self, services: list[str], llm: AsyncOpenAI | None = None, model: str = "") -> None:
        self.services = services
        self.llm = llm
        self.model = model

    def find_services(self, text: str) -> list[str]:
        """
        Services named in the text, in the order they appear. "auth" matches
        auth-service and "payment" matches payments.
        """
        found: list[tuple[int, str]] = []
        for service in self.services:
            aliases = {service, service.split("-")[0].rstrip("s")}
            positions = [m.start() for a in aliases for m in re.finditer(rf"\b{re.escape(a)}", text)]
            if positions:
                found.append((min(positions), service))
        return [service for _, service in sorted(found)]

    async def classify(self, text: str) -> IntentResult:
        """Return an IntentResult for the question ``text``."""
        lower = text.lower()
        hints = [f"{s}-*" for s in self.find_services(lower)]

        def has(pattern: str) -> bool:
            return re.search(pattern, lower) is not None

        if has(QUANTITATIVE) and not has(CAUSAL):
            return IntentResult(logs_needed=True, docs_needed=False, is_quantitative=True, index_hints=hints)
        if has(CAUSAL):
            return IntentResult(logs_needed=True, docs_needed=True, index_hints=hints)
        if has(DOCS) and not has(SYMPTOM):
            return IntentResult(logs_needed=False, docs_needed=True, index_hints=hints)
        if has(SYMPTOM):
            return IntentResult(logs_needed=True, docs_needed=True, index_hints=hints)

        if self.llm is not None:
            data = await ask_json(self.llm, self.model, load_prompt("intent"), text)
            return IntentResult(
                logs_needed=bool(data.get("logs_needed", True)),
                docs_needed=bool(data.get("docs_needed", True)),
                is_quantitative=bool(data.get("is_quantitative", False)),
                index_hints=hints,
                used_llm=True,
            )
        # Unclear and no LLM: fetch both to be safe.
        return IntentResult(logs_needed=True, docs_needed=True, index_hints=hints)
