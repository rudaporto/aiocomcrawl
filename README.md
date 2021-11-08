# aiocomcrawl

This is an experimental work to create a new utility to download data from the
[Common Crawl][1] dataset.

## Motivation

I found two main available utilities to search and download crawl data from the [Common Crawl][1] dataset:
- [cdx_toolkit][2]: more complete utility, exports data into WARC format
- [comcrawl][3]: more recent utility, simpler to use as a library and supports multithreading

Both tools have good support to search the [Common Crawl][1] dataset.

They both use the [Common Crawl Index Server][4] as a backend to find existing
pages in each index.
(There is another [index in Parquet format][5], but it would limit to users with AWS access)

When trying both tools I liked more [comcrawl][3] and it's simplicity.
But it seems to be not build to scale and download huge amounts of data:
- the only supports the threaded concurrency model (what about asyncio?)
- the threads it helps to query the results (io bound), but can slow down the download (cpu bound)
- it keeps all the results in memory (what about getting [wikipedia.de] data?)
- it does not check for the amount of pages in the index (small sites will not notice :)
- it does not support multiprocessing
- it does not support data persistence

The last points could be implemented by the consumer code, that's true.
But since it lacks a more robust support to search and process data at scale,
I decided to implement this new tool.

The main differences in `aiocomcrawl` are:
- it's focused on search and download the data into files, as fast as possible
- it uses asyncio for as the concurrent model (more efficient than threads for io bound)
- it supports multiprocessing (thanks to [aiomultiprocessing][6])
- it can use all your machine cores to speed up the processing :)
- it parses the html body when available into text using [selectolax][7]

## Install

`aiocomcrawl` uses `poetry` to manage it's dependencies.

To install, first make sure you have `poetry` installed.
Then you can get the code and install it using the following commands:
```shell
git clone https://github.com/rudaporto/aiocomcrawl.git
cd aiocomcrawl
poetry shell 
poetry install
```

## Usage:

After installing `aiocomcrawl` is will provide one script called `aiocrawler`.

The `aiocrawler` has only one command with the following arguments and options:

```shell
$ aiocrawler --help

Usage: aiocrawler [OPTIONS] URL

  This is the aiocrawler utility.

  It uses asyncio and multiprocessing to quickly search and download crawl
  data from the Common Crawl public dataset [https://commoncrawl.org].

  When accessing Common Crawl , please you should follow the existing Terms of
  Use [https://commoncrawl.org/terms-of-use].

Options:
  --last-indexes INTEGER   Number of most recent indexes to search on. The
                           default value is zero and it means: query all
                           available indexes.
  --num-processes INTEGER  Number of processes that will be used to fetch and
                           process the results.
```

Here one example:
 - to download all the pages from [de.wikipedia.org]
 - use only one last index (October 2021)
 - using four process to parallelize the download (it will also split the outpur into four files)

```shell
aiocrawler "de.wikipedia.org/*" --num-processes=4 --last-indexes=1
```

## Config

Additional configuration can be changed by overriding the supported environment variables.
Here are the default values defined in `aiocomcrawl.config`:

```python
INDEXES_URI_JSON: str = "https://index.commoncrawl.org/collinfo.json"
S3_BUCKET_BASE_URI: str = "https://commoncrawl.s3.amazonaws.com"
RESPONSE_RETRY_CODES: tuple = (503, 504)
OUTPUT_BASE_PATH: Path = "/tmp"
MAX_RESULTS_QUEUE_SIZE: int = 10 ** 2  # 100
MAX_PERSIST_QUEUE_SIZE: int = 10 ** 2  # 1ßß
QUEUE_EMPTY_SLEEP_TIME: float = 1  # seconds
DOWNLOAD_BODY_WORKERS: int = 50
SEARCH_PAGES_WORKERS: int = 5
SEARCH_INDEX_WORKERS: int = 1
NUM_PROCESSES: int = 1
DEFAULT_LOG_LEVEL: str = "INFO"
```

## Stage

This software is still in an **experimental** stage, and
it's not intent for production.

## Licensing

This code is licensed using the Apache License, Version 2.0 license.

# Terms of use
When accessing [Common Crawl][1], please you should follow the existing [Terms of Use][8].

[1]: https://commoncrawl.org
[2]: https://github.com/cocrawler/cdx_toolkit
[3]: https://github.com/michaelharms/comcrawl
[4]: https://index.commoncrawl.org
[5]: https://commoncrawl.org/2018/03/index-to-warc-files-and-urls-in-columnar-format/
[6]: https://aiomultiprocess.omnilib.dev/en/stable/
[7]: https://github.com/rushter/selectolax
[8]: https://commoncrawl.org/terms-of-use/
