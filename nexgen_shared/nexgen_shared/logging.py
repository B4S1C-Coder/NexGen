"""Structlog setup and service/query-scoped loggers."""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog


def configure_structlog(
    *,
    log_level: str = "INFO",
    json_format: bool = False,
) -> None:
    """
    Configure global structlog + stdlib logging once (safe to call at startup).

    Parameters
    ----------
    log_level:
        Root log level name, e.g. ``INFO``.
    json_format:
        If True, emit one JSON object per line; otherwise use console key-value rendering.
    """
    level = getattr(logging, log_level.upper(), logging.INFO)
    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=level)

    timestamper = structlog.processors.TimeStamper(fmt="iso", utc=True)
    shared: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        timestamper,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if json_format:
        processors: list[Any] = [
            structlog.stdlib.filter_by_level,
            *shared,
            structlog.processors.JSONRenderer(),
        ]
        renderer: list[Any] = []
    else:
        processors = [
            structlog.stdlib.filter_by_level,
            *shared,
        ]
        renderer = [structlog.dev.ConsoleRenderer()]

    structlog.configure(
        processors=processors + renderer,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
        wrapper_class=structlog.stdlib.BoundLogger,
    )


def get_logger(*, service: str, query_id: str | None = None) -> structlog.stdlib.BoundLogger:
    """
    Return a structlog bound logger with ``service`` and ``query_id`` on every event.

    Parameters
    ----------
    service:
        Component name (e.g. ``master``, ``query``, ``rag``).
    query_id:
        Correlation id for the active user query, if any.
    """
    return structlog.get_logger().bind(service=service, query_id=query_id)
