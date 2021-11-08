import asyncio

import click
import uvloop

from aiocomcrawl.config import settings
from aiocomcrawl.pipeline import run_pipeline_multiprocess, run_pipeline_single_process


@click.command()
@click.argument("url")
@click.option(
    "--last-indexes",
    default=0,
    type=click.INT,
    help="Number of most recent indexes to search on. "
    "The default value is zero and it means: query all available indexes.",
)
@click.option(
    "--num-processes",
    type=click.INT,
    default=settings.NUM_PROCESSES,
    help="Number of processes that will be used to fetch and process the results.",
)
def cli(
    url: str,
    last_indexes: int,
    num_processes: int,
):
    """This is the aiocrawler utility.

    It uses asyncio and multiprocessing to quickly search and download
    crawl data from the Common Crawl public dataset [https://commoncrawl.org].

    When accessing Common Crawl , please you should follow the
    existing Terms of Use [https://commoncrawl.org/terms-of-use].
    """

    uvloop.install()
    if num_processes > 1:
        # update the log config
        asyncio.run(
            run_pipeline_multiprocess(
                url, last_indexes=last_indexes, num_processes=num_processes
            )
        )
    else:
        asyncio.run(run_pipeline_single_process(url, last_indexes=last_indexes))


if __name__ == "__main__":
    cli()
