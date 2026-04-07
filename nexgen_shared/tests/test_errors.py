"""Error hierarchy smoke tests."""

from nexgen_shared.errors import (
    E001SchemaLinkingFailure,
    E002KqlSyntaxError,
    E003ElasticsearchTimeout,
    E004VectorStoreUnreachable,
    E005LlmInferenceTimeout,
    E006ContextWindowExceeded,
    E007KnowledgeConflictUnresolved,
    E008TopologyVerificationRejected,
    NexGenError,
)


def test_all_error_codes_and_messages() -> None:
    errors: list[type[NexGenError]] = [
        E001SchemaLinkingFailure,
        E002KqlSyntaxError,
        E003ElasticsearchTimeout,
        E004VectorStoreUnreachable,
        E005LlmInferenceTimeout,
        E006ContextWindowExceeded,
        E007KnowledgeConflictUnresolved,
        E008TopologyVerificationRejected,
    ]
    for i, err_cls in enumerate(errors, start=1):
        code = f"E{i:03d}"
        exc = err_cls("detail")
        assert exc.code == code
        assert "detail" in str(exc)
    assert str(E001SchemaLinkingFailure()) == "[E001]"
