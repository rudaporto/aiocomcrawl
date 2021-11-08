import gzip
from io import BytesIO
from typing import Tuple

from selectolax.parser import HTMLParser

from aiocomcrawl.models import ResultBody, ResultMeta


def parse_body_and_meta(
    body: bytes, mime_detected: str
) -> Tuple[ResultBody, ResultMeta]:
    """Parse the context body and update the result instance."""
    warc_request_meta, response_header, response_body = bytes_to_components(
        BytesIO(body)
    )
    if mime_detected == "text/html":
        text = text_from_html(response_body)
        text_items = [item for item in text.split("\n") if item]
    elif mime_detected == "text/plain":
        text_items = [item for item in response_body.split("\n") if item]
    else:
        text_items = None

    result_body = ResultBody(
        mime_detected=mime_detected,
        data=response_body,
        text=text_items,
    )
    result_meta = ResultMeta(
        warc_request_meta=warc_request_meta,
        response_header=response_header,
    )
    return result_body, result_meta


def bytes_to_components(content: BytesIO) -> Tuple[str, str, str]:
    """Convert the response body (bytes) to the possible components:
     - warc_request_meta: WARC request metadata
     - response_header: response headers and metadata
     - html: response body, usually html

    This should work for all records with status code == 200.
    """
    data_unzipped = gzip.GzipFile(fileobj=content).read()
    data = data_unzipped.decode("utf-8")
    try:
        warc_request_meta, response_header, response_body = data.strip().split(
            "\r\n\r\n", 2
        )
    except ValueError:
        # value error means that the raw data only contains two components
        # this is the case for any crawl request that did not returned 200
        warc_request_meta, response_header = data.strip().split("\r\n\r\n", 1)
        response_body = ""
    return warc_request_meta, response_header, response_body


def text_from_html(html: str) -> str:
    """Transform the html document into a text file.

    This will clean up all the script and style tags too.
    """
    tree = HTMLParser(html)
    if tree.body is None:
        return ""
    for tag in tree.css("script"):
        tag.decompose()
    for tag in tree.css("style"):
        tag.decompose()
    return tree.body.text(strip=True, separator="\n")
