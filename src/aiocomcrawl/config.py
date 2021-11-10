from pathlib import Path

from pydantic import BaseSettings


class Settings(BaseSettings):

    # TODO: better document each setting
    INDEXES_URI_JSON: str = "https://index.commoncrawl.org/collinfo.json"
    S3_BUCKET_BASE_URI: str = "https://commoncrawl.s3.amazonaws.com"
    RESPONSE_RETRY_CODES: tuple = (503, 504)
    OUTPUT_BASE_PATH: Path = "/tmp"
    MAX_RESULTS_QUEUE_SIZE: int = 10 ** 3  # 1000
    MAX_PERSIST_QUEUE_SIZE: int = 10 ** 3  # 100
    QUEUE_EMPTY_SLEEP_TIME: float = 1  # seconds
    DOWNLOAD_BODY_WORKERS: int = 50
    SEARCH_PAGES_WORKERS: int = 5
    SEARCH_INDEX_WORKERS: int = 1
    NUM_PROCESSES: int = 1
    DEFAULT_LOG_LEVEL: str = "DEBUG"
    HTML_TO_TEXT: bool = False
    HTTP_CLIENT_TRACING: bool = False


settings = Settings()