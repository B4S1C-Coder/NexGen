from src.settings import Settings


def test_settings_defaults():
    settings = Settings()

    assert settings.rag_port == 8002
    assert settings.log_level == "INFO"
