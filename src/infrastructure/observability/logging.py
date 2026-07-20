"""Structured logging helper utilizing structlog with standard library fallback."""

import logging
from typing import Any


def setup_logger(name: str = "cpg_streaming", level: int = logging.INFO) -> Any:
    """Configures structured console logging helper."""
    try:
        import structlog
        
        # Configure structlog to write clean formatted outputs
        structlog.configure(
            processors=[
                structlog.processors.add_log_level,
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.dev.ConsoleRenderer()
            ],
            wrapper_class=structlog.make_filtering_bound_logger(level),
            context_class=dict,
            logger_factory=structlog.PrintLoggerFactory(),
            cache_logger_on_first_use=True,
        )
        return structlog.get_logger(name)
    except ImportError:
        # Standard library logging fallback
        logger = logging.getLogger(name)
        logger.setLevel(level)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "[%(asctime)s] %(levelname)s [%(name)s] %(message)s"
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger
