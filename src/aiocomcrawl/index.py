import json
from typing import List

from aiohttp import ClientSession

from aiocomcrawl.config import settings
from aiocomcrawl.log import logger
from aiocomcrawl.models import (
    Index,
    Result,
    SearchPagesRequest,
    SearchPagesResponse,
    SearchRequest,
)


async def retrieve_indexes(client: ClientSession) -> List[Index]:
    """Retrieve the common crawl indexes list."""
    async with client.get(settings.INDEXES_URI_JSON) as response:
        return [Index(**index) for index in await response.json()]


async def search_pages(
    request: SearchPagesRequest, client: ClientSession
) -> SearchPagesResponse:
    """Search available pages in the index for one url."""
    index = request.index
    params = request.dict(exclude={"index"}, by_alias=True)
    async with client.get(index.cdx_api, params=params) as response:
        response.raise_for_status()
        body = await response.text(encoding="utf-8")
        data = json.loads(body)
        return SearchPagesResponse(index=index, url=request.url, pages=data["pages"])


def gen_search_requests(response: SearchPagesResponse) -> List[SearchRequest]:
    """Build all search requests based on the search pages response."""
    return [
        SearchRequest(index=response.index, url=response.url, page=page)
        for page in range(response.pages)
    ]


async def search_index(request: SearchRequest, client: ClientSession) -> List[Result]:
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
    async with client.get(index.cdx_api, params=params) as response:
        response.raise_for_status()
        body = await response.text(encoding="utf-8")
        return [Result.parse_raw(result) for result in body.splitlines()]
