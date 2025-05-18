import structlog
import logging
import sys
from typing import Any, Dict


def configure_logging():
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=logging.DEBUG,
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer(indent=2),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.DEBUG),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.BoundLogger:
    return structlog.get_logger(name)


class LoggerContext:
    def __init__(self, **kwargs: Dict[str, Any]):
        self.context = kwargs

    def __enter__(self):
        for k, v in self.context.items():
            structlog.contextvars.bind_contextvars(**{k: v})
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        structlog.contextvars.clear_contextvars()
        if exc_type:
            logger = get_logger(__name__)
            logger.error("error", error=str(exc_val), exc_info=True)
