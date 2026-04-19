from src.settings import Settings


def test_settings_defaults():
    settings = Settings()

    assert settings.rag_port == 8002
    assert settings.log_level == "INFO"
    assert settings.llamacpp_embed_server_url == "http://localhost:8082"
    assert settings.embedding_model == "nomic-embed-text"
    assert settings.qdrant_url == "http://localhost:6333"
    assert settings.dense_collection == "nexgen_dense"
    assert settings.sparse_collection == "nexgen_bm25_terms"
    assert settings.docs_path == "data/docs"
