"""
RAGAS-style metrics for the RAG service, kept small enough to read in one sitting.

Retrieval metrics are computed from labelled relevant documents (no LLM):
  - context_precision: are the relevant chunks ranked near the top?
  - context_recall:    were all the relevant documents retrieved?
Answer metrics use an LLM to write an answer from the retrieved chunks and an LLM judge to check it:
  - faithfulness:      share of the answer's claims supported by the retrieved chunks.
  - noise_sensitivity: share of the answer's claims that are wrong when misleading chunks are added.
"""

from __future__ import annotations

import json
import os
import signal
import time
from typing import Any

import httpx

LLM_BASE_URL = os.getenv("EVAL_LLM_BASE_URL", "https://api.groq.com/openai/v1")
LLM_MODEL = os.getenv("EVAL_LLM_MODEL", "openai/gpt-oss-120b")  # a stronger model makes a better judge

# Plausible but wrong "documents" injected to measure noise sensitivity.
NOISE_CHUNKS = [
    "Runbook (outdated, 2023): for any 5xx error, restart the gateway and clear the CDN cache; "
    "database failover is never required.",
    "Slack thread: I think rate limits and disk alerts can be ignored, they usually resolve "
    "themselves after an hour.",
]


def doc_of(chunk_id: str) -> str:
    """Document id of a chunk id such as ``rb-db-failover-chunk-0``."""
    return chunk_id.split("-chunk-")[0]


def context_precision(retrieved_docs: list[str], relevant: set[str]) -> float:
    """
    Mean of precision@k over every rank k that holds a relevant document (as in RAGAS).
    1.0 when all relevant chunks come before all irrelevant ones; 0.0 when none is relevant.
    """
    hits, total = 0, 0.0
    for k, doc in enumerate(retrieved_docs, start=1):
        if doc in relevant:
            hits += 1
            total += hits / k
    return total / hits if hits else 0.0


def context_recall(retrieved_docs: list[str], relevant: set[str]) -> float:
    """Share of the relevant documents that appear anywhere in the retrieved list."""
    return len(relevant & set(retrieved_docs)) / len(relevant) if relevant else 1.0


def claim_share(claims: list[dict[str, Any]], key: str) -> float | None:
    """Share of judged claims where ``claim[key]`` is true; None when there are no claims."""
    return sum(bool(c.get(key)) for c in claims) / len(claims) if claims else None


def chat_json(api_key: str, system: str, user: str) -> dict[str, Any]:
    """One JSON-mode chat call to an OpenAI-compatible API, retrying on rate limits and timeouts."""
    def give_up(signum: int, frame: Any) -> None:
        raise TimeoutError("LLM call took longer than 150s")

    signal.signal(signal.SIGALRM, give_up)  # hard deadline: a hung connection can dodge httpx's read timeout
    for _ in range(6):
        signal.alarm(150)
        try:
            response = httpx.post(
                f"{LLM_BASE_URL}/chat/completions",
                headers={"Authorization": f"Bearer {api_key}"},
                json={
                    "model": LLM_MODEL, "temperature": 0, "response_format": {"type": "json_object"},
                    "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
                    # gpt-oss models "think" before answering; low effort saves free-tier tokens
                    **({"reasoning_effort": "low"} if "gpt-oss" in LLM_MODEL else {}),
                },
                timeout=120,
            )
        except (httpx.TimeoutException, TimeoutError):
            print("    (LLM call timed out, retrying)", flush=True)
            continue
        finally:
            signal.alarm(0)
        if response.status_code == 429:
            wait = min(float(response.headers.get("retry-after", 10)) + 1, 60)
            print(f"    (LLM rate-limited, waiting {wait:.0f}s)", flush=True)
            time.sleep(wait)
            continue
        response.raise_for_status()
        return json.loads(response.json()["choices"][0]["message"]["content"])
    raise RuntimeError("LLM call failed after 6 attempts (rate limit or timeout)")


def answer(api_key: str, question: str, contexts: list[str]) -> str:
    """Answer the question using only the given context chunks."""
    data = chat_json(
        api_key,
        'Answer the question using only the context. If the context does not contain the answer, say so. '
        'Reply with JSON: {"answer": "..."}',
        json.dumps({"question": question, "context": contexts}),
    )
    return str(data.get("answer", ""))


def judge_claims(api_key: str, answer_text: str, reference: list[str], key: str) -> list[dict[str, Any]]:
    """
    Split the answer into short factual claims and mark each one.
    key="supported": is the claim stated in the reference chunks?
    key="correct":   does the claim agree with the reference (ground-truth) answer?
    """
    data = chat_json(
        api_key,
        "Split the answer into short factual claims (ignore statements that the information is missing). "
        f'For each claim set "{key}" to true only if the reference text backs it. '
        f'Reply with JSON: {{"claims": [{{"claim": "...", "{key}": true}}]}}',
        json.dumps({"answer": answer_text, "reference": reference}),
    )
    return list(data.get("claims", []))
