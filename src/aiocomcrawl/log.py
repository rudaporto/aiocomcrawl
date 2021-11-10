import logging
from asyncio import Queue
from logging import config

from aiocomcrawl.config import settings


def build_and_set_log_config():
    """Build the log configuration."""
    log_config = {
        "version": 1,
        "formatters": {
            "detailed": {
                "class": "logging.Formatter",
                "format": "%(asctime)s %(name)s %(levelname)s %(processName)s %(message)s",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": settings.DEFAULT_LOG_LEVEL,
                "formatter": "detailed",
            },
            "file": {
                "class": "logging.FileHandler",
                "filename": "/tmp/aiocomcrawl.log",
                "mode": "w",
                "formatter": "detailed",
            },
            "errors": {
                "class": "logging.FileHandler",
                "filename": "/tmp/aiocomcrawl-error.log",
                "mode": "w",
                "level": "ERROR",
                "formatter": "detailed",
            },
        },
        "loggers": {
            "aiocomcrawl": {
                "level": settings.DEFAULT_LOG_LEVEL,
            }
        },
        "root": {
            "level": settings.DEFAULT_LOG_LEVEL,
            "handlers": ["console", "file", "errors"],
        },
    }
    config.dictConfig(log_config)


build_and_set_log_config()
logger = logging.getLogger("aiocomcrawl")


def log_queue_sizes(search_requests: Queue, results: Queue, to_persist: Queue):
    """Logs the queue sizes."""
    logger.debug(f"Search queue: size {search_requests.qsize()}")
    logger.debug(f"Results queue: size: {results.qsize()}")
    logger.debug(f"Persist queue: size {to_persist.qsize()}")
