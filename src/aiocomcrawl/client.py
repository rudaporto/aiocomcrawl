import logging

from aiohttp import ClientSession, TraceConfig


async def on_request_start(session, context, params):
    logging.getLogger("aiohttp.client").debug(f"Starting request <{params}>")


def create_client(enable_tracing: bool = True) -> ClientSession:
    """Create a new client with tracing support."""
    if enable_tracing:
        trace_config = TraceConfig()
        trace_config.on_request_start.append(on_request_start)
        return ClientSession(
            trace_configs=[trace_config],
        )
    else:
        return ClientSession()
