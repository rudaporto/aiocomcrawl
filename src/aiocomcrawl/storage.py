import asyncio
from pathlib import Path
from typing import Any

import aiofiles

from aiocomcrawl.config import settings
from aiocomcrawl.log import logger
from aiocomcrawl.models import Result, ResultBody, ResultMeta


async def write_record(
    out_file: Any, result: Result, result_body: ResultBody, result_meta: ResultMeta
):
    """Write one json record per line into the output file."""
    try:
        if result_body:
            result.body = result_body
        if result_meta:
            result.meta = result_meta
        await out_file.write(f"{result.json(exclude_none=True)}\n")
        # help GC to release memory
        result.body = None
        result.meta = None
    except Exception:
        logger.exception(
            f"Failed to serialize and write record. "
            f"Result: {result} - Body: {result_body} - Meta: {result_meta}"
        )
    finally:
        del result_body
        del result_meta
        del result


async def store_results(
    input_queue: asyncio.Queue, output_file_path: Path, stop_event: asyncio.Event
):
    """Consume the queue with the downloaded items and persist the results into a gzip file."""
    async with aiofiles.open(output_file_path, "at") as out_file:
        while True:
            try:
                result, result_body, result_meta = input_queue.get_nowait()
            except asyncio.QueueEmpty:
                await asyncio.sleep(settings.QUEUE_EMPTY_SLEEP_TIME)
            else:
                await write_record(out_file, result, result_body, result_meta)
                input_queue.task_done()
            finally:
                if stop_event.is_set():
                    logger.info(
                        f"Stop event set. Flushing {output_file_path} and exiting storage worker."
                    )
                    await out_file.flush()
                    break
