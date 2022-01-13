import asyncio
from pathlib import Path
from typing import Any, Optional, Set

import aiofiles
import orjson

from aiocomcrawl.config import settings
from aiocomcrawl.log import logger
from aiocomcrawl.models import Result, ResultBody, ResultMeta


async def write_record(
    out_file: Any,
    result: Result,
    result_meta: ResultMeta,
    result_body: Optional[ResultBody],
):
    """Write one json record per line into the output file.

    # todo: support file compression
    """
    result.body = result_body
    result.meta = result_meta
    try:
        data = result.dict(exclude_none=True, by_alias=True)
        payload = orjson.dumps(data).decode("utf-8")
    except ValueError:
        logger.exception(
            f"Failed to serialize and write record. "
            f"Result: {result} - Body: {result_body} - Meta: {result_meta}"
        )
    else:
        await out_file.write(f"{payload}\n")
    finally:
        # help GC to release memory
        result.body = None
        result.meta = None


async def store_results(
    input_queue: asyncio.Queue, stop_event: asyncio.Event, pid: int
) -> Set[str]:
    """Consume the queue with the downloaded items and persist the results into a file."""
    files = set()
    while not (stop_event.is_set() and input_queue.empty()):
        try:
            result, result_body, result_meta = input_queue.get_nowait()
            domain = result.url_key.split(")")[0].replace(",", "-")
            output_file_path = Path(
                f"/tmp/output_{result.index_id}_{domain}_{pid}.ndjson"
            )
            output_file_path.touch()
        except asyncio.QueueEmpty:
            await asyncio.sleep(settings.QUEUE_EMPTY_SLEEP_TIME)
        else:
            files.add(str(output_file_path))
            async with aiofiles.open(output_file_path, "at") as out_file:
                await write_record(out_file, result, result_body, result_meta)

    logger.info("Finished processing the storage tasks.")
    return files
