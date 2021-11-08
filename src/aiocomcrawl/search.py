import asyncio
from typing import List

from aiohttp import ClientResponseError, ClientSession

from aiocomcrawl.config import settings
from aiocomcrawl.index import (
    gen_search_requests,
    retrieve_indexes,
    search_index,
    search_pages,
)
from aiocomcrawl.log import logger
from aiocomcrawl.models import Index, SearchPagesRequest


class SearchPagesExecutor:
    """Search concurrently all indexes for available data and number of pages.

    Based on number of pages build and search requests to retrieve all results.
    """

    def __init__(
        self,
        url: str,
        client: ClientSession,
        output_queue: asyncio.Queue,
    ):
        self.url = url
        self.client = client
        self.output_queue = output_queue
        self._search_pages_requests_queue = asyncio.Queue()
        self._workers = []

    async def worker(self, worker_id: int):
        input_queue = self._search_pages_requests_queue
        try:
            while search_pages_request := input_queue.get_nowait():
                try:
                    response = await search_pages(search_pages_request, self.client)
                except ClientResponseError as exc:
                    if exc.status in settings.RESPONSE_RETRY_CODES:
                        logger.warning(
                            f"{exc.status} response. Adding search pages request back to the queue"
                        )
                        # todo: we should use tenacity to control the retry strategy
                        # in the function that performance the request, ex: search_pages
                        await input_queue.put(search_pages_request)
                    elif exc.status == 404:
                        logger.debug(
                            f"Get search pages request returned 404: {search_pages_request.dict()}"
                        )
                    else:
                        logger.exception(
                            "Error when processing search page request. This request will not be retried",
                            exc_info=exc,
                        )
                else:
                    for item in gen_search_requests(response):
                        await self.output_queue.put(item)
                finally:
                    input_queue.task_done()

        except asyncio.QueueEmpty:
            logger.debug(f"Search pages queue is empty. Worker {worker_id} exiting")

    def start_workers(self):
        """Launch all the worker tasks."""

    async def run(
        self,
        indexes: List[Index],
        num_workers: int = settings.SEARCH_PAGES_WORKERS,
    ):
        """Run the search requests builder."""
        input_queue = self._search_pages_requests_queue
        for index in indexes:
            await input_queue.put(SearchPagesRequest(index=index, url=self.url))

        # start workers
        for worker_id in range(num_workers):
            self._workers.append(asyncio.create_task(self.worker(worker_id)))

        tasks_results = await asyncio.gather(*self._workers, return_exceptions=True)
        for task_result in tasks_results:
            if isinstance(task_result, BaseException):
                logger.exception("Search pages worker failed.", exc_info=task_result)


async def build_search_requests(
    url: str, indexes: List[Index], client: ClientSession, output_queue: asyncio.Queue
):
    """Build all search requests needed to crawl one url."""
    # use all indexes if they were not provided
    if not indexes:
        indexes = await retrieve_indexes(client)

    # build all search requests
    requests_builder = SearchPagesExecutor(url, client, output_queue)
    await requests_builder.run(indexes)


class SearchIndexesExecutor:
    """Execute all search requests to retried the results for one url and a list of indexes."""

    def __init__(
        self,
        client: ClientSession,
        results_queue: asyncio.Queue,
        search_requests_queue: asyncio.Queue,
    ):
        self.client = client
        self.results_queue = results_queue
        self.search_requests_queue = search_requests_queue
        self._workers = []

    async def run(self, num_workers: int = settings.SEARCH_INDEX_WORKERS):
        """Query all indexes using asyncio.

        This expects that the search_requests_queue is already populated
        by running build_search_requests first.
        """
        # start workers
        for worker_id in range(num_workers):
            self._workers.append(asyncio.create_task(self.worker(worker_id)))

        # wait them to complete
        tasks_results = await asyncio.gather(*self._workers, return_exceptions=True)
        for task_result in tasks_results:
            if isinstance(task_result, BaseException):
                logger.exception("Search index worked failed.", exc_info=task_result)

    async def worker(self, worker_id: int):
        """Search worker:
        - get search requests from the input queue
        - execute the search request
        - write search results into the output queue
        """
        input_queue = self.search_requests_queue
        output_queue = self.results_queue
        try:
            while search_request := input_queue.get_nowait():
                try:
                    items = await search_index(search_request, self.client)
                except ClientResponseError as exc:
                    if exc.status in settings.RESPONSE_RETRY_CODES:
                        # todo: we should use tenacity to control the retry strategy
                        # in the function that performance the request, ex: search_request
                        logger.warning(
                            f"{exc.status} response. Adding search index request back to the queue."
                        )
                        await input_queue.put(search_request)
                    elif exc.status == 404:
                        logger.debug(
                            f"Get search index request returned 404: {search_request.dict()}"
                        )
                    else:
                        logger.exception(
                            "Error when processing search index request. This request will not be retried",
                            exc_info=exc,
                        )

                else:
                    for item in items:
                        item.index_id = search_request.index.id
                        await output_queue.put(item)
                finally:
                    input_queue.task_done()

        except asyncio.QueueEmpty:
            logger.debug(f"Search request queue is empty. Worker {worker_id} exiting")
