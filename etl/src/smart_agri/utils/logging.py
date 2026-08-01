"""Structured logging.

Task containers write to stdout and Airflow captures the stream, so logs are
rendered as JSON in every environment except an interactive terminal, where a
human-readable console renderer is used instead.
"""

from __future__ import annotations

import logging
import sys
from typing import TYPE_CHECKING, Any

import structlog

if TYPE_CHECKING:
    from structlog.typing import FilteringBoundLogger


def configure_logging(level: str = "INFO", *, force_json: bool | None = None) -> None:
    """Install the structlog pipeline. Safe to call more than once.

    Args:
        level: Standard logging level name.
        force_json: Override the automatic renderer choice. `None` selects JSON
            unless stdout is a TTY.
    """
    use_json = not sys.stdout.isatty() if force_json is None else force_json

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, level.upper(), logging.INFO),
        force=True,
    )

    renderer: structlog.types.Processor = (
        structlog.processors.JSONRenderer()
        if use_json
        else structlog.dev.ConsoleRenderer(colors=True)
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper(), logging.INFO)
        ),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str, **initial_values: Any) -> FilteringBoundLogger:
    """Return a bound logger, optionally pre-loaded with context.

    `bind` is always called, even with no values: `structlog.get_logger` hands
    back a lazy proxy that only becomes a real bound logger once bound, and
    callers should never have to care which one they got.
    """
    logger: FilteringBoundLogger = structlog.get_logger(name)
    return logger.bind(**initial_values)
