import asyncio
import os
import sys
from typing import List, Set, Union

import aiomultiprocess
import click

from aiocomcrawl.cdx import CDXGateway
from aiocomcrawl.client import create_client
from aiocomcrawl.config import settings
from aiocomcrawl.download import download_executor
from aiocomcrawl.log import log_queue_sizes, logger
from aiocomcrawl.models import SearchIndexRequest
from aiocomcrawl.search import SearchIndexesExecutor, build_search_requests
from aiocomcrawl.storage import store_results


async def start_workers(
    search_requests: Union[List[SearchIndexRequest], asyncio.Queue]
) -> Set[str]:
    """Runs the pipeline using asyncio concurrency with three main coroutines:
    - get results: fetch the search requests queue, perform the search and output the results
    - download and parse the body: fetch the results queue, download and parse the body and meta from S3
    - persist the data: fetch the 'to_persist' queue and serialize the data to ndjson files
    """
    results_queue = asyncio.Queue(maxsize=settings.MAX_RESULTS_QUEUE_SIZE)
    to_persist_queue = asyncio.Queue(maxsize=settings.MAX_PERSIST_QUEUE_SIZE)
    search_end_event = asyncio.Event()
    download_end_event = asyncio.Event()

    if isinstance(search_requests, asyncio.Queue):
        search_requests_queue = search_requests
    else:
        search_requests_queue = asyncio.Queue()
        for request in search_requests:
            await search_requests_queue.put(request)

    num_search_requests = search_requests_queue.qsize()
    logger.info(
        f"Starting pipeline. Total of {num_search_requests} search index requests to process."
    )

    async with create_client() as client:
        gateway = CDXGateway(client)
        search_indexes = SearchIndexesExecutor(
            gateway, results_queue, search_requests_queue
        )
        search_indexes_task = asyncio.create_task(search_indexes.run())
        download_task = asyncio.create_task(
            download_executor(
                results_queue,
                to_persist_queue,
                client,
                search_end_event,
            )
        )
        store_results_task = asyncio.create_task(
            store_results(to_persist_queue, download_end_event, os.getpid())
        )

        def log_queues():
            log_queue_sizes(search_requests_queue, results_queue, to_persist_queue)

        while not search_indexes_task.done():
            await asyncio.sleep(1)
            logger.debug(
                f"Search index requests pending: {search_requests_queue.qsize()}"
            )

        # set the search results end event
        search_end_event.set()

        while not download_task.done():
            await asyncio.sleep(1)

        # set the download end event
        download_end_event.set()

        while not store_results_task.done():
            await asyncio.sleep(1)

    if exc := search_indexes_task.exception():
        logger.exception(exc_info=exc)

    if exc := download_task.exception():
        logger.exception(exc_info=exc)

    if exc := store_results_task.exception():
        logger.exception(exc_info=exc)

    logger.info("Pipeline finished, exiting.")
    return store_results_task.result()


def chunk_search_requests(
    results_queue: asyncio.Queue, chunk_size: int
) -> List[List[SearchIndexRequest]]:
    """Chunk the search results to be distributed."""
    results_size = results_queue.qsize()
    if results_size < chunk_size:
        chunk_size = results_size

    chunks = []
    for i in range(chunk_size):
        chunks.append([])

    while results_queue.qsize() > 0:
        for i in range(chunk_size):
            try:
                search_request = results_queue.get_nowait()
            except asyncio.queues.QueueEmpty:
                break
            else:
                chunks[i].append(search_request)
                results_queue.task_done()

    return chunks


async def start_workers_multiprocess(
    search_requests_queue: asyncio.Queue, num_processes: int
) -> List[str]:
    """Start workers for each chunk using (aio)multiprocessing."""
    search_requests_chunks = chunk_search_requests(search_requests_queue, num_processes)
    async with aiomultiprocess.Pool(
        processes=len(search_requests_chunks), childconcurrency=1
    ) as pool:
        results = await pool.map(start_workers, search_requests_chunks)
    files = set()
    for r in results:
        files.update(r)
    return list(files)


async def run(
    url: str, last_indexes: int = 0, num_processes: int = settings.NUM_PROCESSES
) -> List[str]:
    """Runs the pipeline."""
    search_requests_queue = asyncio.Queue()

    async with create_client() as client:
        gateway = CDXGateway(client)
        indexes = await gateway.retrieve_indexes()
        if last_indexes and last_indexes < len(indexes):
            indexes = indexes[:last_indexes]

        # build all search requests and split the results in chunks
        await build_search_requests(url, indexes, gateway, search_requests_queue)

        if search_requests_queue.qsize() == 0:
            click.echo(f"No data found for the url: {url} - indexes : {indexes}")
            sys.exit()

    if num_processes > 1:
        files = await start_workers_multiprocess(search_requests_queue, num_processes)
    else:
        files = [await start_workers(search_requests_queue)]

    return files
