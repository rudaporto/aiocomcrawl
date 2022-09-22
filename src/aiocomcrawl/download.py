import asyncio
from typing import Optional

import backoff
from aiohttp import ClientError, ClientSession

from aiocomcrawl.config import settings
from aiocomcrawl.log import logger
from aiocomcrawl.models import Result
from aiocomcrawl.parser import parse_body_and_meta


async def download_body_from_s3(result: Result, client: ClientSession) -> bytes:
    """Download the crawled data for one search result."""
    start = result.offset
    end = start + result.length - 1
    headers = {"Range": f"bytes={start}-{end}"}
    url = f"{settings.S3_BUCKET_BASE_URI}/{result.filename}"
    async with client.get(url, headers=headers) as response:
        return await response.content.read()


async def get_item(input_queue: asyncio.Queue) -> Optional[Result]:
    """Get one item from the input queue."""
    item = None
    try:
        item = input_queue.get_nowait()
    except asyncio.QueueEmpty:
        await asyncio.sleep(settings.QUEUE_EMPTY_SLEEP_TIME)
    finally:
        return item


def fatal_code(exc):
    """Do not retry 401, 403 and 404."""
    return 400 <= exc.status < 500


class DownloadWorker:
    """Consume the search results queue to download, process each page's body."""

    def __init__(
        self,
        input_queue: asyncio.Queue,
        output_queue: asyncio.Queue,
        client: ClientSession,
        worker_id: int,
        stop_event: asyncio.Event,
    ):
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.client = client
        self.worker_id = worker_id
        self.stop_event = stop_event

    @backoff.on_exception(
        backoff.expo,
        ClientError,
        jitter=backoff.full_jitter,
        giveup=fatal_code,
        max_time=settings.CDX_API_RETRY_MAX_WAIT_TIME,
    )
    async def process_item(self, item: Result) -> tuple:
        """Process one item and return the result."""
        body = await download_body_from_s3(item, self.client)
        result_body, result_meta = parse_body_and_meta(body, item.mime_detected)
        result = item, result_body, result_meta
        return result

    async def store_results(self, result: tuple):
        """Store the result in the output queue."""
        await self.output_queue.put(result)
        self.input_queue.task_done()

    async def run(self):
        """Keep working until the stop event is set and the input queue is empty."""
        while not (self.stop_event.is_set() and self.input_queue.empty()):
            item = await get_item(self.input_queue)
            if not item:
                continue
            try:
                result = await self.process_item(item)
            except ClientError:
                logger.exception(f"Failed to download item: {item}")
            else:
                await self.store_results(result)
        else:
            logger.debug(f"Stop event set. Exiting download worker {self.worker_id}")


async def download_executor(
    input_queue: asyncio.Queue,
    output_queue: asyncio.Queue,
    client: ClientSession,
    stop_event: asyncio.Event,
    workers=50,
):
    """Download executor: launch multiple download workers."""
    tasks = []
    for worker_id in range(workers):
        worker = DownloadWorker(
            input_queue, output_queue, client, worker_id, stop_event
        )
        tasks.append(asyncio.create_task(worker.run()))
    tasks_results = await asyncio.gather(*tasks, return_exceptions=True)

    for task_id, task_result in enumerate(tasks_results):
        if isinstance(task_result, BaseException):
            logger.exception(
                f"Download worker {task_id} failed: {task_result}", exc_info=task_result
            )
