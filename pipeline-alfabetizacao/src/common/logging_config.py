"""Configuração de logging padronizado para scripts locais e jobs AWS Glue."""

from __future__ import annotations

import logging


def setup_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
        force=True,
    )


def get_logger(name: str) -> logging.Logger:
    setup_logging()
    return logging.getLogger(name)
