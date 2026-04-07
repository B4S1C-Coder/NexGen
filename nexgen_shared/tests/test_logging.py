"""Structlog helpers."""

import logging

import structlog

from nexgen_shared.logging import configure_structlog, get_logger


def test_get_logger_bind_appears_in_event_dict() -> None:
    captured: list[dict] = []

    def capture(_logger: object, __method: str, event_dict: dict) -> dict:
        captured.append(event_dict.copy())
        return event_dict

    structlog.reset_defaults()
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            capture,
            structlog.dev.ConsoleRenderer(colors=False),
        ],
        logger_factory=structlog.PrintLoggerFactory(),
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    )

    log = get_logger(service="query", query_id="q-1")
    log.info("retrieved")
    assert captured[0]["service"] == "query"
    assert captured[0]["query_id"] == "q-1"
    assert captured[0]["event"] == "retrieved"


def test_configure_structlog_runs() -> None:
    structlog.reset_defaults()
    configure_structlog(log_level="INFO", json_format=False)
    log = get_logger(service="master", query_id=None)
    log.info("startup_ok")
