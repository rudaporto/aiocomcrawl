from datetime import datetime
from typing import Any, List, Optional, Union

from pydantic import BaseModel, Field, HttpUrl, validator
from pydantic.dataclasses import dataclass


class Index(BaseModel):
    id: str
    name: str
    time_gate: HttpUrl = Field(alias="timegate")
    cdx_api: HttpUrl = Field(alias="cdx-api")


@dataclass(frozen=True)
class ResultBody:
    mime_detected: Optional[str]
    data: Optional[str]
    text: Optional[List[str]]


@dataclass(frozen=True)
class ResultMeta:
    # todo: these are still raw strings
    warc_request_meta: Optional[str]
    response_header: Optional[str]


class Result(BaseModel):
    url_key: str = Field(alias="urlkey")
    timestamp: datetime
    url: str
    mime: str
    mime_detected: str = Field(alias="mime-detected")
    status: int
    digest: str
    length: int
    offset: int
    filename: str
    languages: Optional[str]
    encoding: Optional[str]
    index_id: Optional[str]
    body: Optional[ResultBody]
    meta: Optional[ResultMeta]

    @validator("timestamp", pre=True)
    def parse_timestamp(cls, value: Any) -> Union[datetime, Any]:
        if isinstance(value, str):
            datetime_value = datetime.strptime(value, "%Y%m%d%H%M%S")
            return datetime_value
        return value


class SearchPagesRequest(BaseModel):
    """Request existing pages on one index for a given url."""

    index: Index
    url: str
    show_num_pages: str = Field(alias="showNumPages", default="true", const=True)
    output: str = "json"


class SearchPagesResponse(BaseModel):
    """Response with the total number of pages in this index for a given url."""

    index: Index
    url: str
    pages: int


class SearchIndexRequest(BaseModel):
    """One page that contains records to be fetched."""

    index: Index
    url: str
    page: int
    output: str = "json"
