import asyncio

import click
import uvloop

from aiocomcrawl import pipeline
from aiocomcrawl.config import settings


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

    When accessing Common Crawl, you should follow the
    existing Terms of Use [https://commoncrawl.org/terms-of-use].
    """

    uvloop.install()
    asyncio.run(
        pipeline.run(url, last_indexes=last_indexes, num_processes=num_processes)
    )


if __name__ == "__main__":
    cli()
