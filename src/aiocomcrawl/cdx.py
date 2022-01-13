import json
from typing import List

import backoff
from aiohttp import ClientError, ClientSession
from tabulate import tabulate

from aiocomcrawl.client import create_client
from aiocomcrawl.config import settings
from aiocomcrawl.log import logger
from aiocomcrawl.models import (
    Index,
    Result,
    SearchIndexRequest,
    SearchPagesRequest,
    SearchPagesResponse,
)


def gen_search_requests(response: SearchPagesResponse) -> List[SearchIndexRequest]:
    """Build all search requests based on the search pages response."""
    return [
        SearchIndexRequest(index=response.index, url=response.url, page=page)
        for page in range(response.pages)
    ]


def fatal_code(exc):
    """Do not retry 401, 403 and 404."""
    return 400 <= exc.status < 500


async def print_indexes(last_indexes=0, file=None):
    """Print all indexes in a tabular format."""
    async with create_client() as client:
        gateway = CDXGateway(client)
        indexes = await gateway.retrieve_indexes()

    if last_indexes:
        indexes = indexes[:last_indexes]

    table = []
    for n, index in enumerate(indexes):
        table.append((n + 1, index.name, index.cdx_api))
    print(tabulate(table, headers=["Position", "Name", "CDX API"]), file=file)


class CDXGateway:

    # TODO: add a simpler interface to interact with the index API

    def __init__(self, client: ClientSession):
        self.client = client

    @backoff.on_exception(
        backoff.expo,
        ClientError,
        jitter=backoff.full_jitter,
        giveup=fatal_code,
        max_time=settings.CDX_API_RETRY_MAX_WAIT_TIME,
    )
    async def retrieve_indexes(self) -> List[Index]:
        """Retrieve the common crawl indexes list."""
        async with self.client.get(settings.INDEXES_URI_JSON) as response:
            return [Index(**index) for index in await response.json()]

    @backoff.on_exception(
        backoff.expo,
        ClientError,
        jitter=backoff.full_jitter,
        giveup=fatal_code,
        max_time=settings.CDX_API_RETRY_MAX_WAIT_TIME,
    )
    async def search_pages(self, request: SearchPagesRequest) -> SearchPagesResponse:
        """Search available pages in the index for one url."""
        index = request.index
        params = request.dict(exclude={"index"}, by_alias=True)
        async with self.client.get(index.cdx_api, params=params) as response:
            body = await response.text(encoding="utf-8")
            data = json.loads(body)
            return SearchPagesResponse(
                index=index, url=request.url, pages=data["pages"]
            )

    @backoff.on_exception(
        backoff.expo,
        ClientError,
        jitter=backoff.full_jitter,
        giveup=fatal_code,
        max_time=settings.CDX_API_RETRY_MAX_WAIT_TIME,
    )
    async def search_index(self, request: SearchIndexRequest) -> List[Result]:
        """Search index for a give website url.

        The response body is a ndjson file with one result per line.

        For more details on how to search the index:
        - https://commoncrawl.org/2015/04/announcing-the-common-crawl-index/
        """
        index = request.index
        params = {"url": request.url, "output": request.output, "page": request.page}
        logger.info(
            f"Start searching index: {index.name} - url_key: {request.url} - params: {params}"
        )
        async with self.client.get(index.cdx_api, params=params) as response:
            body = await response.text(encoding="utf-8")
            return [Result.parse_raw(result) for result in body.splitlines()]
