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
    llamacpp_embed_server_url: str = Field(
        default="http://localhost:8082",
        validation_alias="LLAMACPP_EMBED_SERVER_URL",
    )
    embedding_model: str = Field(
        default="nomic-embed-text",
        validation_alias="EMBEDDING_MODEL",
    )
    qdrant_url: str = Field(
        default="http://localhost:6333",
        validation_alias="QDRANT_URL",
    )
    dense_collection: str = Field(
        default="nexgen_dense",
        validation_alias="DENSE_COLLECTION",
    )
    sparse_collection: str = Field(
        default="nexgen_bm25_terms",
        validation_alias="SPARSE_COLLECTION",
    )
    docs_path: str = Field(default="data/docs")
