"""Logging configuration — call configure_logging() once at application startup."""

import logging
import sys

import structlog
from prefect.logging.configuration import setup_logging as setup_prefect_logging

from shared.config.settings import settings


def drop_color_message_key(_, __, event_dict: dict) -> dict:
    """Uvicorn logs 'color_message' to extra; drop it so it doesn't clutter structlog."""
    event_dict.pop("color_message", None)
    return event_dict


def configure_logging() -> None:
    """Configure a unified production-grade structlog system for FastAPI & Prefect."""

    # 1. Force prefect to initialize its logging first so we can hijack its loggers
    setup_prefect_logging()

    level = getattr(logging, settings.log_level.upper(), logging.INFO)

    # 2. Shared structlog processors
    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.ExtraAdder(),  # Captures `extra` kwargs (crucial for Prefect contexts)
        drop_color_message_key,  # Clean up Uvicorn's extra color_message
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    # 3. Environment-specific formatting
    if settings.environment == "production":
        formatter_processor = structlog.processors.JSONRenderer()
    else:
        formatter_processor = structlog.dev.ConsoleRenderer(colors=True)

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            formatter_processor,
        ],
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    # 4. Hijack standard logging Root
    root_logger = logging.getLogger()
    root_logger.handlers = [handler]
    root_logger.setLevel(level)

    # 5. Hijack all Prefect loggers
    # Prefect creates 'prefect', 'prefect.flow_runs', 'prefect.task_runs', etc.
    for name in list(logging.root.manager.loggerDict.keys()):
        if name.startswith("prefect"):
            logger = logging.getLogger(name)
            logger.handlers.clear()
            logger.propagate = True

    # 6. Hijack Uvicorn loggers
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logger = logging.getLogger(name)
        logger.handlers.clear()
        logger.propagate = True
        logger.setLevel(logging.INFO)

    # Silence noisy third-party loggers
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("pymongo").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    # 7. Configure structlog defaults
    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
