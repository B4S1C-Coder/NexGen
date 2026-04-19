from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """RAG service settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    rag_port: int = Field(default=8002, validation_alias="RAG_PORT")
    log_level: str = Field(default="INFO", validation_alias="LOG_LEVEL")
