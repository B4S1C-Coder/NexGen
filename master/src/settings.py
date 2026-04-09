from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    """Master service configuration loaded from environment."""

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
        default="redis://localhost:6379",
        validation_alias="REDIS_URL"
    )

    http_timeout_seconds: float = Field(
        default=30.0,
        validation_alias="HTTP_TIMEOUT_SECONDS"
    )

    # LLM (served by llama.cpp)
    llamacpp_server_url: str = Field(
        default="http://localhost:8080",
        validation_alias="LLAMACPP_SERVER_URL",
    )
    master_llm_model: str = Field(
        default="qwen-3.5_4B_Q4_K_M",
        validation_alias="MASTER_LLM_MODEL",
    )
    master_llm_temperature: float = Field(
        default=0.2,
        validation_alias="MASTER_LLM_TEMPERATURE",
    )
    master_llm_max_tokens: int = Field(
        default=2048,
        validation_alias="MASTER_LLM_MAX_TOKENS",
    )

    # Session / orchestration
    session_ttl_seconds: int = Field(
        default=7200,
        validation_alias="SESSION_TTL_SECONDS",
    )
    max_dag_iterations: int = Field(
        default=3,
        validation_alias="MAX_DAG_ITERATIONS",
    )
    max_tot_branches: int = Field(
        default=3,
        validation_alias="MAX_TOT_BRANCHES",
    )
    max_validator_cycles: int = Field(
        default=3,
        validation_alias="MAX_VALIDATOR_CYCLES",
    )
    max_synthesis_tokens: int = Field(
        default=6000,
        validation_alias="MAX_SYNTHESIS_TOKENS",
    )
    topology_config_path: str = Field(
        default="config/topology.json",
        validation_alias="TOPOLOGY_CONFIG_PATH",
    )
