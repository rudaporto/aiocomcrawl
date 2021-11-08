import asyncio

from aiohttp import ClientResponseError, ClientSession

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


async def download_worker(
    input_queue: asyncio.Queue,
    output_queue: asyncio.Queue,
    client: ClientSession,
    worker_id: int,
    stop_event: asyncio.Event,
):
    """Download the html and parse it into text for each SearchResult in the queue."""
    while True:
        try:
            result = input_queue.get_nowait()
            try:
                body = await download_body_from_s3(result, client)
            except ClientResponseError as exc:
                if exc.status in settings.RESPONSE_RETRY_CODES:
                    # todo: we should use tenacity to control the retry strategy
                    # in the function that performance the request, ex: download_body
                    logger.warning(
                        f"{exc.status} response. Adding download request back to the queue."
                    )
                    await input_queue.put(result)
                else:
                    logger.exception(
                        "Error when processing search index request. This request will not be retried",
                        exc_info=exc,
                    )
            else:
                result_body, result_meta = parse_body_and_meta(
                    body, result.mime_detected
                )
                await output_queue.put((result, result_body, result_meta))

        except asyncio.QueueEmpty:
            await asyncio.sleep(settings.QUEUE_EMPTY_SLEEP_TIME)
        else:
            input_queue.task_done()
        finally:
            if stop_event.is_set():
                logger.debug(f"Stop event set. Exiting download worker {worker_id}")
                break


async def download_body(
    input_queue: asyncio.Queue,
    output_queue: asyncio.Queue,
    client: ClientSession,
    stop_event: asyncio.Event,
    workers=50,
):
    """Download the body from S3, parse, and update the result instance."""
    tasks = []
    for worker_id in range(workers):
        tasks.append(
            asyncio.create_task(
                download_worker(
                    input_queue, output_queue, client, worker_id, stop_event
                )
            )
        )
    tasks_results = await asyncio.gather(*tasks, return_exceptions=True)

    for task_result in tasks_results:
        if isinstance(task_result, BaseException):
            logger.exception(
                f"Download worker failed: {task_result}", exc_info=task_result
            )
