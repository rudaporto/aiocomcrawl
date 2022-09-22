from pathlib import Path

from pydantic import BaseSettings


class Settings(BaseSettings):

    INDEXES_URI_JSON: str = "https://index.commoncrawl.org/collinfo.json"
    S3_BUCKET_BASE_URI: str = "http://data.commoncrawl.org"
    RESPONSE_RETRY_CODES: tuple = (500, 503, 504)
    OUTPUT_BASE_PATH: Path = "/tmp"
    MAX_RESULTS_QUEUE_SIZE: int = 10000  # 10K
    MAX_PERSIST_QUEUE_SIZE: int = 10000  # 10K
    QUEUE_EMPTY_SLEEP_TIME: float = 0.1  # seconds
    DOWNLOAD_BODY_WORKERS: int = 10
    SEARCH_PAGES_WORKERS: int = 1
    SEARCH_INDEX_WORKERS: int = 10
    NUM_PROCESSES: int = 1
    DEFAULT_LOG_LEVEL: str = "INFO"
    HTML_TO_TEXT: bool = False
    HTTP_CLIENT_TRACING: bool = False
    CDX_API_RETRY_MAX_WAIT_TIME: int = 10
    DOWNLOAD_BODY_RETRY_MAX_WAIT_TIME: int = 60


settings = Settings()
