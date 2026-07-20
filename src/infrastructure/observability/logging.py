"""Configures standardized logging for CLI outputs and background services."""

import logging


def setup_logger(name: str = "cpg_streaming", level: int = logging.INFO) -> logging.Logger:
    """Configures structured console logging handler and formatters."""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "[%(asctime)s] %(levelname)s [%(name)s:%(lineno)s] %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger
