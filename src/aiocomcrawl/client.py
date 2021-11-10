from aiohttp import ClientSession, TraceConfig

from aiocomcrawl.config import settings
from aiocomcrawl.log import logger


async def on_request_start(session, context, params):
    logger.debug(f"Starting request <{params}>")


def create_client(enable_tracing: bool = settings.HTTP_CLIENT_TRACING) -> ClientSession:
    """Create a new client with tracing support."""
    if enable_tracing:
        trace_config = TraceConfig()
        trace_config.on_request_start.append(on_request_start)
        return ClientSession(
            raise_for_status=True,
            trace_configs=[trace_config],
        )
    else:
        return ClientSession(raise_for_status=True)
