import asyncio
import os
import sys
from typing import List, Union

import aiomultiprocess
import click

from aiocomcrawl.client import create_client
from aiocomcrawl.config import settings
from aiocomcrawl.download import download_body
from aiocomcrawl.index import retrieve_indexes
from aiocomcrawl.log import log_queue_sizes, logger
from aiocomcrawl.models import SearchRequest
from aiocomcrawl.search import SearchIndexesExecutor, build_search_requests
from aiocomcrawl.storage import store_results


async def run(search_requests: Union[List[SearchRequest], asyncio.Queue]):
    """Runs the pipeline using asyncio concurrency with three main coroutines:
    - get results: fetch the search requests queue, perform the search and output the results
    - download and parse the body: fetch the results queue, download and parse the body and meta from S3
    - persist the data: fetch the 'to_persist' queue and serialize the data to ndjson files
    """
    results_queue = asyncio.Queue(maxsize=settings.MAX_RESULTS_QUEUE_SIZE)
    to_persist_queue = asyncio.Queue(maxsize=settings.MAX_PERSIST_QUEUE_SIZE)
    stop_event = asyncio.Event()

    if isinstance(search_requests, list):
        search_requests_queue = asyncio.Queue()
        for request in search_requests:
            await search_requests_queue.put(request)
    else:
        search_requests_queue = search_requests

    num_search_requests = search_requests_queue.qsize()
    logger.info(
        f"Starting pipeline. Total of {num_search_requests} search requests to process."
    )

    async with create_client() as client:
        search_indexes = SearchIndexesExecutor(
            client, results_queue, search_requests_queue
        )
        search_indexes_task = asyncio.create_task(search_indexes.run())
        download_task = asyncio.create_task(
            download_body(
                results_queue,
                to_persist_queue,
                client,
                stop_event,
            )
        )
        # todo: find a better way to split the data into files
        out_file = settings.OUTPUT_BASE_PATH / f"output-{os.getpid()}.json"
        store_results_task = asyncio.create_task(
            store_results(to_persist_queue, out_file, stop_event)
        )

        def log_queues():
            log_queue_sizes(search_requests_queue, results_queue, to_persist_queue)

        while not search_indexes_task.done():
            await asyncio.sleep(10)
            log_queues()

        while (
            search_requests_queue.qsize() != 0
            or results_queue.qsize() != 0
            or to_persist_queue.qsize() != 0
        ):
            await asyncio.sleep(10)
            log_queues()

        # set the stop event wait the download and store tasks to finish
        stop_event.set()
        while not download_task.done() or not store_results_task.done():
            await asyncio.sleep(1)

    if exc := search_indexes_task.exception():
        logger.exception(exc_info=exc)

    if exc := download_task.exception():
        logger.exception(exc_info=exc)

    if exc := store_results_task.exception():
        logger.exception(exc_info=exc)

    logger.info("Pipeline finished, exiting.")


def chunk_search_requests(
    results_queue: asyncio.Queue, num_processes: int
) -> List[List[SearchRequest]]:
    """Chunk the search results to be distributed."""
    chunks = []
    num_search_requests = results_queue.qsize()
    if num_search_requests == 0:
        return chunks

    chunk_size = num_search_requests // num_processes
    rest = num_search_requests % num_processes

    for _ in range(num_processes):
        chunk = []
        for _ in range(chunk_size):
            chunk.append(results_queue.get_nowait())
            results_queue.task_done()
        chunks.append(chunk)

    for _ in range(rest):
        results_queue.qsize()
        chunks[-1].append(results_queue.get_nowait())
        results_queue.task_done()

    return chunks


async def run_pipeline_single_process(url: str, last_indexes: int = 0):
    """Runs the pipeline without multiprocessing."""
    search_requests_queue = asyncio.Queue()

    async with create_client() as client:
        indexes = await retrieve_indexes(client)
        if last_indexes and last_indexes < len(indexes):
            indexes = indexes[:last_indexes]

        # build all search requests and split the results in chunks
        await build_search_requests(url, indexes, client, search_requests_queue)
        await run(search_requests_queue)


async def run_pipeline_multiprocess(
    url: str, last_indexes: int = 0, num_processes: int = settings.NUM_PROCESSES
):
    """Runs the pipeline using (aio)multiprocessing."""
    search_requests_queue = asyncio.Queue()

    async with create_client() as client:
        indexes = await retrieve_indexes(client)
        if last_indexes and last_indexes < len(indexes):
            indexes = indexes[:last_indexes]

        # build all search requests and split the results in chunks
        await build_search_requests(url, indexes, client, search_requests_queue)
        search_requests_chunks = chunk_search_requests(
            search_requests_queue, num_processes
        )
        if not search_requests_chunks:
            click.echo(f"Not data found for the url: {url} - indexes : {indexes}")
            sys.exit()

        # start multiple processes
        async with aiomultiprocess.Pool(
            processes=num_processes, childconcurrency=1
        ) as pool:
            await pool.map(run, search_requests_chunks)
