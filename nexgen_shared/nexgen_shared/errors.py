"""Structured errors; codes align with AGENTS.md §7."""


class NexGenError(Exception):
    """Base class for NexGen pipeline failures carrying a stable error code."""

    code: str

    def __init__(self, message: str = "") -> None:
        """Create an error; ``message`` is human-readable detail for logs and APIs."""
        super().__init__(message)
        self.message = message

    def __str__(self) -> str:
        if self.message:
            return f"[{self.code}] {self.message}"
        return f"[{self.code}]"


class E001SchemaLinkingFailure(NexGenError):
    """E001 — Schema linking failure — no matching index found."""

    code = "E001"


class E002KqlSyntaxError(NexGenError):
    """E002 — KQL syntax error after maximum refinement attempts."""

    code = "E002"


class E003ElasticsearchTimeout(NexGenError):
    """E003 — Elasticsearch connection timeout."""

    code = "E003"


class E004VectorStoreUnreachable(NexGenError):
    """E004 — Vector store unreachable."""

    code = "E004"


class E005LlmInferenceTimeout(NexGenError):
    """E005 — LLM inference timeout."""

    code = "E005"


class E006ContextWindowExceeded(NexGenError):
    """E006 — Context window exceeded — compression failed."""

    code = "E006"


class E007KnowledgeConflictUnresolved(NexGenError):
    """E007 — Knowledge conflict unresolved after multi-agent debate."""

    code = "E007"


class E008TopologyVerificationRejected(NexGenError):
    """E008 — Topology verification rejected LLM claim."""

    code = "E008"
