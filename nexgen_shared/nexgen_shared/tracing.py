import functools
from typing import Any, Callable

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource, SERVICE_NAME
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


def configure_tracer(service_name: str) -> trace.Tracer:
    """
    Initialises OpenTelemetry with an OTLP gRPC exporter.

    Args:
        service_name: The name of the service to register.

    Returns:
        An OpenTelemetry Tracer instance.
    """
    resource = Resource(attributes={SERVICE_NAME: service_name})
    provider = TracerProvider(resource=resource)

    exporter = OTLPSpanExporter()
    processor = BatchSpanProcessor(exporter)
    provider.add_span_processor(processor)

    trace.set_tracer_provider(provider)
    return trace.get_tracer(service_name)


def traced(span_name: str) -> Callable[..., Any]:
    """
    Decorator that wraps an async function in an OpenTelemetry span.

    Args:
        span_name: String name for the created span.

    Returns:
        The wrapped async function.
    """
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            tracer = trace.get_tracer(__name__)
            with tracer.start_as_current_span(span_name):
                return await func(*args, **kwargs)
        return wrapper
    return decorator
