"""Unit tests for RepairAgent (repair.py).

All tests use mocked generator and validator — no real API calls or
Elasticsearch connections required.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.repair import RepairAgent, MAX_REPAIR_ATTEMPTS
from src.few_shot import FewShotExample
from src.schema_linker import FieldMeta, SchemaContext
from src.validator import ValidationResult
from nexgen_shared.errors import E002KqlSyntaxError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_schema_ctx() -> SchemaContext:
    return SchemaContext(
        selected_indices=["logs-*"],
        relevant_fields=[FieldMeta(name="service.name", es_type="keyword")],
        time_field="@timestamp",
        max_result_size=500,
    )


def make_examples() -> list[FewShotExample]:
    return [FewShotExample(nl="show errors", kql='log.level: "ERROR"')]


def make_valid_result(kql: str) -> ValidationResult:
    return ValidationResult(valid=True, errors=[], kql=kql)


def make_invalid_result(kql: str, errors: list[str]) -> ValidationResult:
    return ValidationResult(valid=False, errors=errors, kql=kql)


def make_agent(generator_returns, validator_returns) -> RepairAgent:
    """Build RepairAgent with mocked generator and validator.

    generator_returns: list of str or Exception to return/raise per call
    validator_returns: list of ValidationResult to return per call
    """
    generator = MagicMock()
    side_effects = []
    for item in generator_returns:
        if isinstance(item, Exception):
            side_effects.append(item)
        else:
            side_effects.append(item)
    generator.generate = AsyncMock(side_effect=generator_returns)

    validator = MagicMock()
    validator.validate = MagicMock(side_effect=validator_returns)

    return RepairAgent(generator, validator)


# ---------------------------------------------------------------------------
# Tests — success cases
# ---------------------------------------------------------------------------

class TestRepairAgentSuccess:
    """Tests where valid KQL is produced."""

    @pytest.mark.asyncio
    async def test_returns_kql_on_first_valid_attempt(self) -> None:
        """Must return KQL immediately when first attempt is valid."""
        kql = 'service.name: "auth"'
        agent = make_agent(
            generator_returns=[kql],
            validator_returns=[make_valid_result(kql)],
        )
        result = await agent.repair("show auth errors", make_schema_ctx(), make_examples())
        assert result == kql

    @pytest.mark.asyncio
    async def test_generator_called_once_on_first_success(self) -> None:
        """Generator must be called exactly once when first attempt succeeds."""
        kql = 'log.level: "ERROR"'
        agent = make_agent(
            generator_returns=[kql],
            validator_returns=[make_valid_result(kql)],
        )
        await agent.repair("show errors", make_schema_ctx(), make_examples())
        assert agent._generator.generate.call_count == 1

    @pytest.mark.asyncio
    async def test_returns_kql_on_second_attempt(self) -> None:
        """Must succeed on second attempt after first is invalid."""
        kql_bad = 'service.name: AND'
        kql_good = 'service.name: "auth"'
        agent = make_agent(
            generator_returns=[kql_bad, kql_good],
            validator_returns=[
                make_invalid_result(kql_bad, ["Field 'service.name' has no value"]),
                make_valid_result(kql_good),
            ],
        )
        result = await agent.repair("show auth", make_schema_ctx(), make_examples())
        assert result == kql_good

    @pytest.mark.asyncio
    async def test_generator_called_twice_on_second_success(self) -> None:
        """Generator must be called twice when second attempt succeeds."""
        kql_bad = 'bad kql'
        kql_good = 'log.level: "ERROR"'
        agent = make_agent(
            generator_returns=[kql_bad, kql_good],
            validator_returns=[
                make_invalid_result(kql_bad, ["some error"]),
                make_valid_result(kql_good),
            ],
        )
        await agent.repair("show errors", make_schema_ctx(), make_examples())
        assert agent._generator.generate.call_count == 2


# ---------------------------------------------------------------------------
# Tests — failure cases
# ---------------------------------------------------------------------------

class TestRepairAgentFailure:
    """Tests where E002KqlSyntaxError is raised after all attempts."""

    @pytest.mark.asyncio
    async def test_raises_e002_after_all_attempts_fail(self) -> None:
        """Must raise E002KqlSyntaxError when all attempts produce invalid KQL."""
        kql = 'bad kql'
        agent = make_agent(
            generator_returns=[kql, kql, kql],
            validator_returns=[
                make_invalid_result(kql, ["error 1"]),
                make_invalid_result(kql, ["error 2"]),
                make_invalid_result(kql, ["error 3"]),
            ],
        )
        with pytest.raises(E002KqlSyntaxError):
            await agent.repair("show errors", make_schema_ctx(), make_examples())

    @pytest.mark.asyncio
    async def test_generator_called_max_attempts_times(self) -> None:
        """Generator must be called exactly MAX_REPAIR_ATTEMPTS times."""
        kql = 'bad kql'
        agent = make_agent(
            generator_returns=[kql] * MAX_REPAIR_ATTEMPTS,
            validator_returns=[make_invalid_result(kql, ["err"])] * MAX_REPAIR_ATTEMPTS,
        )
        with pytest.raises(E002KqlSyntaxError):
            await agent.repair("show errors", make_schema_ctx(), make_examples())
        assert agent._generator.generate.call_count == MAX_REPAIR_ATTEMPTS

    @pytest.mark.asyncio
    async def test_raises_e002_when_generator_always_crashes(self) -> None:
        """Must raise E002 when generator raises exception on every attempt."""
        agent = make_agent(
            generator_returns=[RuntimeError("api down")] * MAX_REPAIR_ATTEMPTS,
            validator_returns=[],
        )
        with pytest.raises(E002KqlSyntaxError):
            await agent.repair("show errors", make_schema_ctx(), make_examples())

    @pytest.mark.asyncio
    async def test_validator_not_called_when_generator_crashes(self) -> None:
        """Validator must not be called when generator raises exception."""
        agent = make_agent(
            generator_returns=[RuntimeError("api down")] * MAX_REPAIR_ATTEMPTS,
            validator_returns=[],
        )
        with pytest.raises(E002KqlSyntaxError):
            await agent.repair("show errors", make_schema_ctx(), make_examples())
        agent._validator.validate.assert_not_called()


# ---------------------------------------------------------------------------
# Tests — repair prompt behaviour
# ---------------------------------------------------------------------------

class TestRepairPromptBehaviour:
    """Tests that errors are fed back to generator on retry."""

    @pytest.mark.asyncio
    async def test_second_call_includes_error_in_query(self) -> None:
        """Second generator call must include validation errors in query."""
        kql_bad = 'bad kql'
        kql_good = 'log.level: "ERROR"'
        agent = make_agent(
            generator_returns=[kql_bad, kql_good],
            validator_returns=[
                make_invalid_result(kql_bad, ["Field 'x' not found"]),
                make_valid_result(kql_good),
            ],
        )
        await agent.repair("show errors", make_schema_ctx(), make_examples())

        second_call_query = agent._generator.generate.call_args_list[1][0][0]
        assert "Field 'x' not found" in second_call_query

    @pytest.mark.asyncio
    async def test_second_call_includes_original_request(self) -> None:
        """Second generator call must still include the original question."""
        kql_bad = 'bad kql'
        kql_good = 'log.level: "ERROR"'
        agent = make_agent(
            generator_returns=[kql_bad, kql_good],
            validator_returns=[
                make_invalid_result(kql_bad, ["some error"]),
                make_valid_result(kql_good),
            ],
        )
        await agent.repair("my original question", make_schema_ctx(), make_examples())

        second_call_query = agent._generator.generate.call_args_list[1][0][0]
        assert "my original question" in second_call_query


# ---------------------------------------------------------------------------
# Tests — configuration
# ---------------------------------------------------------------------------

class TestRepairAgentConfig:
    """Tests for max_attempts configuration."""

    def test_default_max_attempts_is_three(self) -> None:
        """Default MAX_REPAIR_ATTEMPTS must be 3."""
        assert MAX_REPAIR_ATTEMPTS == 3

    def test_custom_max_attempts_respected(self) -> None:
        """Custom max_attempts must be used instead of default."""
        agent = RepairAgent(MagicMock(), MagicMock(), max_attempts=1)
        assert agent._max_attempts == 1