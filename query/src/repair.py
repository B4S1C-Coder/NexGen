"""Repair Agent — Stage 5 of the NL-to-KQL pipeline.

Wraps KQLGenerator and KQLValidator in a retry loop. If the generated
KQL is invalid, the error messages are fed back to the generator so
the LLM can correct its output. Raises E002KqlSyntaxError if all
attempts are exhausted.

Defined in TASKS.md P2-Q4.
"""

from __future__ import annotations

import logging

from nexgen_shared.errors import E002KqlSyntaxError

from .few_shot import FewShotExample
from .generator import KQLGenerator
from .schema_linker import SchemaContext
from .validator import KQLValidator, ValidationResult

logger = logging.getLogger(__name__)

MAX_REPAIR_ATTEMPTS = 3

REPAIR_PREFIX = (
    "The KQL you produced was invalid. "
    "Fix the following errors and output ONLY the corrected KQL:\n"
)


class RepairAgent:
    """Generates and validates KQL, retrying on syntax errors.

    On each attempt the KQLGenerator is called. If the KQLValidator
    finds errors, those errors are prepended to the natural language
    query so the LLM understands what to fix on the next attempt.

    Generator exceptions and empty responses are also handled gracefully
    — they trigger a retry with an error description rather than
    crashing the whole pipeline.

    Usage:
        agent = RepairAgent(generator, validator)
        kql = await agent.repair(nl, schema_ctx, examples)
    """

    def __init__(
        self,
        generator: KQLGenerator,
        validator: KQLValidator,
        max_attempts: int = MAX_REPAIR_ATTEMPTS,
    ) -> None:
        """Initialise with injected generator and validator.

        Args:
            generator:    An initialised KQLGenerator instance.
            validator:    A KQLValidator instance.
            max_attempts: Maximum number of generate+validate cycles.
                          Defaults to MAX_REPAIR_ATTEMPTS (3).
        """
        self._generator = generator
        self._validator = validator
        self._max_attempts = max_attempts

    async def repair(
        self,
        natural_language: str,
        schema_ctx: SchemaContext,
        examples: list[FewShotExample],
    ) -> str:
        """Generate valid KQL, retrying with error feedback if invalid.

        On the first attempt the natural language query is sent as-is.
        On subsequent attempts the validation error messages are prepended
        to the query so the LLM knows what to fix.

        Generator exceptions and empty KQL responses are caught and
        treated as invalid — they trigger a retry with error context
        rather than propagating the exception immediately.

        Args:
            natural_language: The user's natural language query.
            schema_ctx:       SchemaContext from SchemaLinker.
            examples:         Few-shot examples from FewShotSelector.

        Returns:
            A validated KQL string that passed all KQLValidator checks.

        Raises:
            E002KqlSyntaxError: If all attempts produce invalid KQL
                                or the generator fails on every attempt.
        """
        current_query = natural_language
        last_result: ValidationResult | None = None

        for attempt in range(1, self._max_attempts + 1):
            logger.info(
                "RepairAgent attempt %d/%d. query=%r",
                attempt,
                self._max_attempts,
                current_query[:80],
            )

            # Call generator — catch crashes so retries can continue
            try:
                kql = await self._generator.generate(
                    current_query, schema_ctx, examples
                )
            except Exception as exc:
                logger.error(
                    "Generator failed on attempt %d: %s", attempt, exc
                )
                current_query = (
                    f"{REPAIR_PREFIX}- Generator error: {exc}\n\n"
                    f"Original request: {natural_language}"
                )
                continue

            # Treat empty KQL as invalid without calling the validator
            if not kql.strip():
                logger.warning(
                    "Generator returned empty KQL on attempt %d", attempt
                )
                current_query = (
                    f"{REPAIR_PREFIX}- Empty KQL returned\n\n"
                    f"Original request: {natural_language}"
                )
                continue

            # Validate the generated KQL
            result = self._validator.validate(kql, schema_ctx)
            last_result = result

            if result.valid:
                logger.info(
                    "RepairAgent succeeded on attempt %d. kql=%r",
                    attempt,
                    kql,
                )
                return kql

            # Build repair prompt for the next attempt
            error_summary = "\n".join(f"- {e}" for e in result.errors)
            current_query = (
                f"{REPAIR_PREFIX}{error_summary}\n\n"
                f"Original request: {natural_language}"
            )
            logger.warning(
                "Attempt %d produced invalid KQL. errors=%s",
                attempt,
                result.errors,
            )

        # All attempts exhausted
        errors = last_result.errors if last_result else ["Generator failed on all attempts"]
        last_kql = last_result.kql if last_result else ""
        raise E002KqlSyntaxError(
            f"KQL generation failed after {self._max_attempts} attempts. "
            f"Last KQL: {last_kql!r}. "
            f"Errors: {errors}"
        )