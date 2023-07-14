import sys
import uuid
from pathlib import Path
from typing import Literal

from loguru import logger
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class SpeedLimit(BaseSettings):
    max_concurrent: int = Field(300, alias="MAX_CONCURRENT")


class Retry(BaseSettings):
    timeout: float = Field(1800, alias="TIMEOUT")
    max_retries: int | None = Field(None, alias="MAX_RETRIES")
    min_interval: float = Field(0.1, alias="MIN_INTERVAL")
    max_interval: float = Field(3600, alias="MAX_INTERVAL")
    max_doubling: int = Field(16, alias="MAX_DOUBLING")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(extra="allow", env_file=Path(".env"))

    log_level: Literal[
        "CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "TRACE"
    ] = Field("INFO", alias="LOG_LEVEL")

    redis_dsn: str = Field("redis://localhost:6379/0", alias="REDIS")

    api_token: str | None = Field(None, alias="API_TOKEN")

    consumer_name: str = Field(
        default_factory=lambda: uuid.uuid4().hex, alias="CONSUMER_NAME"
    )

    speed_limit: SpeedLimit = Field(default_factory=SpeedLimit)  # type: ignore

    retry: Retry = Field(default_factory=Retry)  # type: ignore


settings = Settings()  # type: ignore

# setup logging
logger.remove()
logger.add(sys.stdout, level=settings.log_level)
