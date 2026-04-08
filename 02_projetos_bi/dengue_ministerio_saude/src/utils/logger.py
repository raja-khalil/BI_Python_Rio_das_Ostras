"""Logger central reutilizavel."""

from __future__ import annotations

import logging

from src.config.settings import get_settings


def get_logger(name: str) -> logging.Logger:
    """Retorna logger padronizado para o projeto."""
    settings = get_settings()

    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(settings.log_level.upper())
    logger.propagate = False

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    return logger
