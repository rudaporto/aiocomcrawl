import asyncio

import click
import uvloop

from aiocomcrawl import cdx, pipeline
from aiocomcrawl.config import settings


@click.group()
def cli():
    """This is the aiocrawler utility.

    It uses asyncio and multiprocessing to quickly search and download
    crawl data from the Common Crawl public dataset: https://commoncrawl.org

    When accessing Common Crawl, you should follow the
    existing Terms of Use: https://commoncrawl.org/terms-of-use
    """
    pass


@cli.command("pipeline")
@click.argument("url")
@click.option(
    "--last-indexes",
    default=1,
    type=click.INT,
    help="Number of most recent indexes to search on."
    "The default value is 1 and it will only query the latest index.",
)
@click.option(
    "--num-processes",
    type=click.INT,
    default=settings.NUM_PROCESSES,
    help="Number of processes that will be used to fetch and process the results.",
)
def pipeline_run(
    url: str,
    last_indexes: int,
    num_processes: int,
):
    """Run the pipeline: query indexes and download data."""

    uvloop.install()
    files = asyncio.run(
        pipeline.run(url, last_indexes=last_indexes, num_processes=num_processes)
    )
    click.echo(f"Files created: {files}")


@cli.command()
@click.option(
    "--last-indexes",
    default=0,
    type=click.INT,
    help="Number of most recent indexes to show.",
)
def show_indexes(last_indexes: int):
    """List all available CDX indexes."""
    asyncio.run(cdx.print_indexes(last_indexes=last_indexes))


if __name__ == "__main__":
    cli()
