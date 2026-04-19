"""KQL Generator — Stage 3 of the NL-to-KQL pipeline.

Calls the Groq API (OpenAI-compatible) with a structured prompt built
from the schema context, few-shot examples, and the user's natural
language query. Returns a raw Kibana KQL string.

The OpenAI-compatible SDK is used so switching to another provider
(fine-tuned model, OpenAI, Together AI) requires only changing
GROQ_BASE_URL and GROQ_MODEL in .env — no code changes.

Defined in TASKS.md P2-Q2.
"""

from __future__ import annotations

import logging
from pathlib import Path

from openai import OpenAI
from pydantic_settings import BaseSettings, SettingsConfigDict

from .few_shot import FewShotExample
from .schema_linker import SchemaContext

logger = logging.getLogger(__name__)

PROMPT_PATH = Path(__file__).parent.parent / "prompts" / "generator.txt"


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

class GeneratorSettings(BaseSettings):
    """Configuration for the KQLGenerator read from .env."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    groq_api_key: str = ""
    groq_base_url: str = "https://api.groq.com/openai/v1"
    groq_model: str = "llama-3.3-70b-versatile"
    generator_temperature: float = 0.05
    generator_max_tokens: int = 200


# ---------------------------------------------------------------------------
# KQLGenerator
# ---------------------------------------------------------------------------

class KQLGenerator:
    """Generates Kibana KQL strings from natural language using an LLM.

    Uses the OpenAI-compatible SDK pointed at Groq. The system prompt
    is loaded from prompts/generator.txt. The user message is assembled
    from schema field names, few-shot examples, and the NL query.

    Usage:
        generator = KQLGenerator()
        generator.startup()
        kql = await generator.generate(nl, schema_ctx, examples)
        generator.shutdown()
    """

    def __init__(self) -> None:
        self._settings = GeneratorSettings()
        self._system_prompt: str = _load_system_prompt(PROMPT_PATH)
        self._client: OpenAI | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def startup(self) -> None:
        """Initialise the OpenAI-compatible client.

        Should be called once during FastAPI app lifespan startup.
        """
        self._client = OpenAI(
            api_key=self._settings.groq_api_key,
            base_url=self._settings.groq_base_url,
        )
        logger.info(
            "KQLGenerator started. model=%s base_url=%s",
            self._settings.groq_model,
            self._settings.groq_base_url,
        )

    def shutdown(self) -> None:
        """Release the client. Called during FastAPI app lifespan shutdown."""
        self._client = None
        logger.info("KQLGenerator shut down.")

    # ------------------------------------------------------------------
    # Core generation
    # ------------------------------------------------------------------

    async def generate(
        self,
        natural_language: str,
        schema_ctx: SchemaContext,
        examples: list[FewShotExample],
    ) -> str:
        """Generate a KQL string from a natural language query.

        Assembles the user message from schema fields and few-shot
        examples, calls the LLM at low temperature, and returns the
        raw KQL string with whitespace stripped.

        Retries once on API failure or empty response before raising.

        Args:
            natural_language: The user's natural language query.
            schema_ctx:       SchemaContext from SchemaLinker.
            examples:         Few-shot NLQ->KQL examples from FewShotSelector.

        Returns:
            Raw KQL string — no markdown fences, no explanation.

        Raises:
            RuntimeError: If startup() was never called.
            RuntimeError: If both attempts fail or return empty responses.
        """
        if self._client is None:
            raise RuntimeError(
                "KQLGenerator.startup() must be called before generate()."
            )

        user_message = _build_user_message(
            natural_language, schema_ctx, examples
        )
        logger.debug("Generator user message:\n%s", user_message)

        last_exc: Exception | None = None

        for attempt in range(2):
            try:
                response = self._client.chat.completions.create(
                    model=self._settings.groq_model,
                    messages=[
                        {"role": "system", "content": self._system_prompt},
                        {"role": "user", "content": user_message},
                    ],
                    temperature=self._settings.generator_temperature,
                    max_tokens=self._settings.generator_max_tokens,
                )
                raw = response.choices[0].message.content or ""
                kql = raw.strip().strip("`").strip()

                if kql:
                    logger.info(
                        "Generated KQL (attempt %d): %s", attempt + 1, kql
                    )
                    return kql

                logger.warning(
                    "Empty response on attempt %d — retrying.", attempt + 1
                )

            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "Groq API call failed on attempt %d: %s", attempt + 1, exc
                )

        raise RuntimeError(
            f"KQLGenerator failed after 2 attempts. "
            f"model='{self._settings.groq_model}'. "
            f"Last error: {last_exc}"
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_system_prompt(path: Path) -> str:
    """Load the system prompt from disk.

    Args:
        path: Path to the generator.txt prompt file.

    Returns:
        System prompt string.

    Raises:
        FileNotFoundError: If the prompt file does not exist.
    """
    if not path.exists():
        raise FileNotFoundError(
            f"Generator system prompt not found at {path}. "
            "Ensure prompts/generator.txt exists."
        )
    return path.read_text(encoding="utf-8").strip()


def _build_user_message(
    natural_language: str,
    schema_ctx: SchemaContext,
    examples: list[FewShotExample],
) -> str:
    """Assemble the user message for the LLM from schema and examples.

    Args:
        natural_language: The user's natural language query.
        schema_ctx:       SchemaContext with field names and types.
        examples:         Few-shot NLQ->KQL examples.

    Returns:
        Formatted user message string ready to send to the LLM.
    """
    field_lines = "\n".join(
        f"  - {f.name} ({f.es_type})"
        for f in schema_ctx.relevant_fields[:20]
    )
    schema_section = (
        f"Available fields in indices {schema_ctx.selected_indices}:\n"
        f"{field_lines}"
        if field_lines
        else "No schema available — use common ECS field names."
    )

    example_lines = "\n\n".join(
        f"NL: {ex.nl}\nKQL: {ex.kql}"
        for ex in examples
    )
    examples_section = (
        f"Examples:\n{example_lines}"
        if example_lines
        else "No examples available."
    )

    return (
        f"{schema_section}\n\n"
        f"{examples_section}\n\n"
        f"NL: {natural_language}\n"
        f"KQL:"
    )