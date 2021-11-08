from pathlib import Path

from pydantic import BaseSettings


class Settings(BaseSettings):

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


settings = Settings()
