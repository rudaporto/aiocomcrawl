import asyncio
from typing import List

from aiohttp import ClientError

from aiocomcrawl.cdx import CDXGateway, gen_search_requests
from aiocomcrawl.config import settings
from aiocomcrawl.log import logger
from aiocomcrawl.models import Index, SearchIndexRequest, SearchPagesRequest


class SearchPagesExecutor:
    """Search concurrently all indexes for available data and number of pages.

    Based on number of pages build and search requests to retrieve all results.
    """

    def __init__(
        self,
        url: str,
        gateway: CDXGateway,
        output_queue: asyncio.Queue,
    ):
        self.url = url
        self.gateway = gateway
        self.output_queue = output_queue
        self._search_pages_requests_queue = asyncio.Queue()
        self._workers = []

    async def worker(self, worker_id: int):
        input_queue = self._search_pages_requests_queue
        while not input_queue.empty():
            search_pages_request = await input_queue.get()
            try:
                response = await self.gateway.search_pages(search_pages_request)
            except ClientError:
                logger.exception(
                    f"Failed to get the pages for the search request: {search_pages_request}"
                )
            else:
                for item in gen_search_requests(response):
                    await self.output_queue.put(item)
            finally:
                input_queue.task_done()
        else:
            logger.debug(f"Search pages queue is empty. Worker {worker_id} exiting")

    async def start_workers(
        self,
        indexes: List[Index],
        num_workers: int = settings.SEARCH_PAGES_WORKERS,
    ):
        """Start workers (tasks)."""
        input_queue = self._search_pages_requests_queue
        for index in indexes:
            await input_queue.put(SearchPagesRequest(index=index, url=self.url))

        # start workers
        for worker_id in range(num_workers):
            self._workers.append(asyncio.create_task(self.worker(worker_id)))

    async def gather_tasks(self):
        """Gather running workers."""
        tasks_results = await asyncio.gather(*self._workers, return_exceptions=True)
        for task_result in tasks_results:
            if isinstance(task_result, BaseException):
                logger.exception("Search pages worker failed.", exc_info=task_result)

    async def run(
        self,
        indexes: List[Index],
        num_workers: int = settings.SEARCH_PAGES_WORKERS,
    ):
        """Run the search requests builder."""
        await self.start_workers(indexes, num_workers)
        await self.gather_tasks()


async def build_search_requests(
    url: str, indexes: List[Index], gateway: CDXGateway, output_queue: asyncio.Queue
):
    """Build all search requests needed to crawl one url."""
    # use all indexes if they were not provided
    if not indexes:
        indexes = await gateway.retrieve_indexes()

    # build all search requests
    requests_builder = SearchPagesExecutor(url, gateway, output_queue)
    await requests_builder.run(indexes)


class SearchIndexesExecutor:
    """Execute all search requests returning all results for one url and a list of indexes."""

    def __init__(
        self,
        gateway: CDXGateway,
        results_queue: asyncio.Queue,
        search_requests_queue: asyncio.Queue,
    ):
        self.gateway = gateway
        self.results_queue = results_queue
        self.search_requests_queue = search_requests_queue
        self._workers = []

    async def start_workers(
        self,
        num_workers: int = settings.SEARCH_PAGES_WORKERS,
    ):
        """Start workers (tasks)."""
        num_pages_to_retrieve = self.search_requests_queue.qsize()
        # make sure we do not launch more than needed workers
        if num_workers > num_pages_to_retrieve:
            num_workers = num_pages_to_retrieve

        # start workers
        for worker_id in range(num_workers):
            self._workers.append(asyncio.create_task(self.worker(worker_id)))

    async def gather_tasks(self):
        """Gather running workers."""
        tasks_results = await asyncio.gather(*self._workers, return_exceptions=True)
        for task_result in tasks_results:
            if isinstance(task_result, BaseException):
                logger.exception("Search index worked failed.", exc_info=task_result)

    async def run(self, num_workers: int = settings.SEARCH_INDEX_WORKERS):
        """Query all indexes using asyncio.

        This expects that the search_requests_queue is already populated
        by running build_search_requests first.
        """
        await self.start_workers(num_workers)
        await self.gather_tasks()

    async def worker(self, worker_id: int):
        """Search worker:
        - get search requests from the input queue
        - execute the search request
        - write search results into the output queue
        """
        input_queue = self.search_requests_queue
        output_queue = self.results_queue

        while not input_queue.empty():
            search_request: SearchIndexRequest = await input_queue.get()
            try:
                items = await self.gateway.search_index(search_request)
            except ClientError:
                logger.exception(
                    f"Failed to get results for the search request: {search_request}"
                )
            else:
                logger.info(
                    f"Total of {len(items)} results returned from index: "
                    f"{search_request.index.id} - page: {search_request.page}"
                )
                for item in items:
                    item.index_id = search_request.index.id
                    await output_queue.put(item)
            finally:
                input_queue.task_done()
        else:
            logger.debug(f"Search request queue is empty. Worker {worker_id} exiting")
