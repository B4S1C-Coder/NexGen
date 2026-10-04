"""Tiny helper shared by every step that talks to the LLM."""

from __future__ import annotations

import json
from typing import Any

from openai import AsyncOpenAI

from src.settings import MASTER_DIR


def load_prompt(name: str) -> str:
    """Return the text of ``src/prompts/<name>.txt``."""
    return (MASTER_DIR / "src" / "prompts" / f"{name}.txt").read_text()


async def ask_json(client: AsyncOpenAI, model: str, system: str, user: str) -> dict[str, Any]:
    """
    Send one chat request and parse the reply as a JSON object.

    Parameters: the OpenAI-compatible client, model name, system prompt and user message.
    Returns: the parsed dict. Raises ``ValueError`` if the reply is not valid JSON.
    """
    response = await client.chat.completions.create(
        model=model,
        messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
        response_format={"type": "json_object"},
        temperature=0,
    )
    raw = (response.choices[0].message.content or "").strip()
    # Some models wrap JSON in a ```json fence even in JSON mode.
    raw = raw.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError("LLM reply is not a JSON object")
    return data
