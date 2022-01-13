from aiohttp import ClientSession, TCPConnector, TraceConfig

from aiocomcrawl.config import settings
from aiocomcrawl.log import logger


async def on_request_start(session, context, params):
    """Callback to log when the request started."""
    logger.info(f"Starting request <{params}>")


async def on_request_end(session, context, params):
    """Callback to log when the request ended."""
    logger.info(f"Ending request <{params}>")


def create_connector() -> TCPConnector:
    """Creates a new aiohttp tcp connector."""
    limit = max(
        settings.DOWNLOAD_BODY_WORKERS,
        settings.SEARCH_INDEX_WORKERS,
        settings.SEARCH_PAGES_WORKERS,
    )
    return TCPConnector(limit=limit)


def create_client() -> ClientSession:
    """Creates a new aiohttp client."""
    kwargs = {"raise_for_status": True, "connector": create_connector()}

    if settings.HTTP_CLIENT_TRACING:
        trace_config = TraceConfig()
        trace_config.on_request_start.append(on_request_start)
        trace_config.on_request_end.append(on_request_end)
        kwargs["trace_configs"] = [trace_config]

    return ClientSession(**kwargs)
