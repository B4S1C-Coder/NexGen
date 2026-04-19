"""Unit tests for KQLGenerator (generator.py).

All tests use mocked OpenAI client — no real Groq API calls made.
Tests verify prompt assembly, retry logic, and error handling.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.generator import KQLGenerator, _build_user_message, _load_system_prompt
from src.few_shot import FewShotExample
from src.schema_linker import FieldMeta, SchemaContext


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_schema_ctx(fields: list[str] | None = None) -> SchemaContext:
    """Build a minimal SchemaContext for testing."""
    names = fields or ["service.name", "log.level", "@timestamp"]
    return SchemaContext(
        selected_indices=["logs-*"],
        relevant_fields=[FieldMeta(name=n, es_type="keyword") for n in names],
        time_field="@timestamp",
        max_result_size=500,
    )


def make_examples() -> list[FewShotExample]:
    """Build two minimal few-shot examples."""
    return [
        FewShotExample(
            nl="Show auth errors",
            kql='service.name: "auth" AND log.level: "ERROR"',
        ),
        FewShotExample(
            nl="Show payment errors",
            kql='service.name: "payments" AND log.level: "ERROR"',
        ),
    ]


def make_mock_response(content: str) -> MagicMock:
    """Build a mock OpenAI chat completion response."""
    choice = MagicMock()
    choice.message.content = content
    response = MagicMock()
    response.choices = [choice]
    return response


def make_generator_with_mock(response_content: str) -> KQLGenerator:
    """Build a KQLGenerator with a mocked client returning given content."""
    gen = KQLGenerator()
    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value = make_mock_response(
        response_content
    )
    gen._client = mock_client
    return gen


# ---------------------------------------------------------------------------
# Tests for _load_system_prompt
# ---------------------------------------------------------------------------

class TestLoadSystemPrompt:
    """Tests for the system prompt loader."""

    def test_loads_real_prompt_file(self, tmp_path) -> None:
        """Must load content from an existing file."""
        prompt_file = tmp_path / "generator.txt"
        prompt_file.write_text("You are a KQL expert.", encoding="utf-8")
        result = _load_system_prompt(prompt_file)
        assert result == "You are a KQL expert."

    def test_missing_file_raises_file_not_found(self, tmp_path) -> None:
        """Missing prompt file must raise FileNotFoundError."""
        with pytest.raises(FileNotFoundError, match="generator.txt"):
            _load_system_prompt(tmp_path / "nonexistent.txt")

    def test_real_prompt_file_is_non_empty(self) -> None:
        """The actual prompts/generator.txt must exist and be non-empty."""
        from pathlib import Path
        path = Path(__file__).parent.parent.parent / "prompts" / "generator.txt"
        content = _load_system_prompt(path)
        assert len(content) > 50


# ---------------------------------------------------------------------------
# Tests for _build_user_message
# ---------------------------------------------------------------------------

class TestBuildUserMessage:
    """Tests for the user message assembler."""

    def test_contains_natural_language_query(self) -> None:
        """User message must contain the NL query."""
        msg = _build_user_message(
            "show payment errors", make_schema_ctx(), make_examples()
        )
        assert "show payment errors" in msg

    def test_contains_field_names(self) -> None:
        """User message must contain schema field names."""
        msg = _build_user_message(
            "any query", make_schema_ctx(["service.name", "log.level"]), []
        )
        assert "service.name" in msg
        assert "log.level" in msg

    def test_contains_few_shot_examples(self) -> None:
        """User message must contain NL and KQL from examples."""
        msg = _build_user_message(
            "any query", make_schema_ctx(), make_examples()
        )
        assert "Show auth errors" in msg
        assert 'service.name: "auth"' in msg

    def test_ends_with_kql_prompt(self) -> None:
        """User message must end with KQL: to prompt the model."""
        msg = _build_user_message(
            "show errors", make_schema_ctx(), make_examples()
        )
        assert msg.strip().endswith("KQL:")

    def test_empty_schema_shows_fallback(self) -> None:
        """Empty schema must show ECS fallback message."""
        ctx = SchemaContext(
            selected_indices=[],
            relevant_fields=[],
            time_field="@timestamp",
            max_result_size=500,
        )
        msg = _build_user_message("any query", ctx, [])
        assert "ECS" in msg or "common" in msg.lower()

    def test_empty_examples_shows_no_examples(self) -> None:
        """Empty examples list must show no examples message."""
        msg = _build_user_message("any query", make_schema_ctx(), [])
        assert "No examples" in msg


# ---------------------------------------------------------------------------
# Tests for KQLGenerator lifecycle
# ---------------------------------------------------------------------------

class TestKQLGeneratorLifecycle:
    """Tests for startup and shutdown."""

    @pytest.mark.asyncio
    async def test_generate_before_startup_raises_runtime_error(self) -> None:
        """Calling generate() without startup() must raise RuntimeError."""
        gen = KQLGenerator()
        with pytest.raises(RuntimeError, match="startup()"):
            await gen.generate("any query", make_schema_ctx(), [])


# ---------------------------------------------------------------------------
# Tests for KQLGenerator.generate()
# ---------------------------------------------------------------------------

class TestKQLGeneratorGenerate:
    """Tests for the generate() method with mocked Groq client."""

    @pytest.mark.asyncio
    async def test_returns_kql_string(self) -> None:
        """generate() must return the KQL string from the model."""
        gen = make_generator_with_mock('service.name: "payments"')
        result = await gen.generate(
            "show payment errors", make_schema_ctx(), make_examples()
        )
        assert result == 'service.name: "payments"'

    @pytest.mark.asyncio
    async def test_strips_markdown_fences(self) -> None:
        """generate() must strip backtick markdown fences from response."""
        gen = make_generator_with_mock('`service.name: "auth"`')
        result = await gen.generate(
            "show auth errors", make_schema_ctx(), make_examples()
        )
        assert result == 'service.name: "auth"'

    @pytest.mark.asyncio
    async def test_strips_whitespace(self) -> None:
        """generate() must strip leading and trailing whitespace."""
        gen = make_generator_with_mock('  log.level: "ERROR"  ')
        result = await gen.generate(
            "show errors", make_schema_ctx(), make_examples()
        )
        assert result == 'log.level: "ERROR"'

    @pytest.mark.asyncio
    async def test_retries_on_api_failure(self) -> None:
        """generate() must retry once on API exception."""
        gen = KQLGenerator()
        mock_client = MagicMock()
        mock_client.chat.completions.create.side_effect = [
            Exception("rate limit"),
            make_mock_response('log.level: "ERROR"'),
        ]
        gen._client = mock_client

        result = await gen.generate(
            "show errors", make_schema_ctx(), make_examples()
        )
        assert result == 'log.level: "ERROR"'
        assert mock_client.chat.completions.create.call_count == 2

    @pytest.mark.asyncio
    async def test_raises_after_two_failures(self) -> None:
        """generate() must raise RuntimeError after both attempts fail."""
        gen = KQLGenerator()
        mock_client = MagicMock()
        mock_client.chat.completions.create.side_effect = Exception("api down")
        gen._client = mock_client

        with pytest.raises(RuntimeError, match="2 attempts"):
            await gen.generate(
                "show errors", make_schema_ctx(), make_examples()
            )

    @pytest.mark.asyncio
    async def test_retries_on_empty_response(self) -> None:
        """generate() must retry when model returns empty string."""
        gen = KQLGenerator()
        mock_client = MagicMock()
        mock_client.chat.completions.create.side_effect = [
            make_mock_response(""),
            make_mock_response('log.level: "ERROR"'),
        ]
        gen._client = mock_client

        result = await gen.generate(
            "show errors", make_schema_ctx(), make_examples()
        )
        assert result == 'log.level: "ERROR"'

    @pytest.mark.asyncio
    async def test_client_called_with_correct_model(self) -> None:
        """generate() must call the API with the configured model."""
        gen = make_generator_with_mock('log.level: "ERROR"')
        await gen.generate("show errors", make_schema_ctx(), make_examples())

        call_kwargs = gen._client.chat.completions.create.call_args.kwargs
        assert call_kwargs["model"] == gen._settings.groq_model

    @pytest.mark.asyncio
    async def test_client_called_with_low_temperature(self) -> None:
        """generate() must use low temperature for deterministic output."""
        gen = make_generator_with_mock('log.level: "ERROR"')
        await gen.generate("show errors", make_schema_ctx(), make_examples())

        call_kwargs = gen._client.chat.completions.create.call_args.kwargs
        assert call_kwargs["temperature"] <= 0.1