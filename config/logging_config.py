"""
ATLAS ERP Pipeline — Logging Configuration
Structured logging with console + rotating file handlers.
"""
import logging
import logging.handlers
from pathlib import Path

from config.settings import LOG_FILE, LOG_LEVEL, LOGS_DIR


def setup_logging(name: str = "atlas") -> logging.Logger:
    """
    Configure and return a named logger with:
      - StreamHandler  → INFO   level, human-readable format
      - RotatingFileHandler → DEBUG level, machine-readable format (JSON-like)
    """
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    if logger.handlers:
        return logger

    # ------------------------------------------------------------------
    # Console handler  (INFO+)
    # ------------------------------------------------------------------
    console = logging.StreamHandler()
    console.setLevel(getattr(logging, LOG_LEVEL))
    console.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )

    # ------------------------------------------------------------------
    # Rotating file handler  (DEBUG+, max 5 MB × 3 backups)
    # ------------------------------------------------------------------
    file_handler = logging.handlers.RotatingFileHandler(
        LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter(
            fmt='{"time": "%(asctime)s", "level": "%(levelname)s", '
                '"logger": "%(name)s", "message": "%(message)s"}',
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
    )

    logger.addHandler(console)
    logger.addHandler(file_handler)
    return logger


def get_logger(module: str) -> logging.Logger:
    """Child logger factory — keeps hierarchy for easy filtering."""
    return logging.getLogger(f"atlas.{module}")
