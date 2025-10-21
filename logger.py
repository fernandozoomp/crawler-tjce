import structlog
import logging
import sys
from typing import Any, Dict


def configure_logging():
    import os

    # Verifica se debug está habilitado via variável de ambiente
    debug_mode = os.getenv("DEBUG", "false").lower() == "true"

    # Configura logging básico
    log_level = logging.DEBUG if debug_mode else logging.INFO
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level,
    )

    # Configura filtros para ignorar logs de bibliotecas externas em produção
    class LibraryFilter(logging.Filter):
        def filter(self, record):
            # Filtra logs de bibliotecas que geram muito ruído
            if record.name.startswith(('boto3', 'urllib3', 'requests', 'werkzeug', 'asyncio', 'charset_normalizer')):
                return False
            return True

    # Só aplica filtro se não estiver em modo debug
    if not debug_mode:
        root_logger = logging.getLogger()
        root_logger.addFilter(LibraryFilter())

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
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
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
