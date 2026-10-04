from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# master/ directory, so files resolve the same no matter where the process starts.
MASTER_DIR = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    """Master service configuration, read from environment variables or master/.env."""

    model_config = SettingsConfigDict(
        env_file=MASTER_DIR / ".env", env_file_encoding="utf-8", extra="ignore"
    )

    master_port: int = Field(default=8000, validation_alias="MASTER_PORT")
    log_level: str = Field(default="INFO", validation_alias="LOG_LEVEL")

    # Downstream services. With MOCK_SERVICES=true, data/scenarios.json is used instead.
    query_service_url: str = Field(default="http://localhost:8001", validation_alias="QUERY_SERVICE_URL")
    rag_service_url: str = Field(default="http://localhost:8002", validation_alias="RAG_SERVICE_URL")
    mock_services: bool = Field(default=False, validation_alias="MOCK_SERVICES")
    http_timeout_seconds: float = Field(default=30.0, validation_alias="HTTP_TIMEOUT_SECONDS")

    # Any OpenAI-compatible endpoint (Groq, OpenAI, Ollama, llama.cpp). Empty key = rules only.
    openai_api_key: str = Field(default="", validation_alias="OPENAI_API_KEY")
    openai_base_url: str = Field(default="https://api.groq.com/openai/v1", validation_alias="OPENAI_BASE_URL")
    openai_model_name: str = Field(default="openai/gpt-oss-20b", validation_alias="OPENAI_MODEL_NAME")

    redis_url: str = Field(default="redis://localhost:6379/0", validation_alias="REDIS_URL")
    session_ttl_seconds: int = Field(default=7200, validation_alias="SESSION_TTL_SECONDS")
    max_synthesis_tokens: int = Field(default=6000, validation_alias="MAX_SYNTHESIS_TOKENS")
    topology_path: Path = Field(default=MASTER_DIR / "config" / "topology.json", validation_alias="TOPOLOGY_CONFIG_PATH")
