from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    """ Master service configuration loaded from enviornment. """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )
    
    master_port: int = Field(default=8000, validation_alias="MASTER_PORT")
    log_level: str = Field(default="INFO", validation_alias="LOG_LEVEL")

    query_service: str = Field(
        default="http://localhost:8001",
        validation_alias="QUERY_SERVICE_URL"
    )
    rag_service_url: str = Field(
        default="http://localhost:8002",
        validation_alias="RAG_SERVICE_URL"
    )
    redis_url: str = Field(
        default="redis://localhost:6379/0",
        validation_alias="REDIS_URL"
    )
    
    http_timeout_seconds: float = Field(
        default=30.0,
        validation_alias="HTTP_TIMEOUT_SECONDS"
    )
