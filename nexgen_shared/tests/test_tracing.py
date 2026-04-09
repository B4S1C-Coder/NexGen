import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.sdk.resources import Resource, SERVICE_NAME

from nexgen_shared.tracing import traced

@pytest.fixture
def memory_exporter():
    return InMemorySpanExporter()

@pytest.fixture(autouse=True)
def setup_tracing(memory_exporter):
    """Set up the tracer provider with an InMemorySpanExporter for testing."""
    resource = Resource(attributes={SERVICE_NAME: "test-service"})
    provider = TracerProvider(resource=resource)
    processor = SimpleSpanProcessor(memory_exporter)
    provider.add_span_processor(processor)
    # Note: we don't strictly use trace.set_tracer_provider if it warns,
    # but the simplest way is to overwrite and reset or just use it.
    trace._TRACER_PROVIDER_SET_ONCE._is_set = False # Reset to allow overwrite in pytest
    trace.set_tracer_provider(provider)
    
    yield
    
    memory_exporter.clear()

@pytest.mark.asyncio
async def test_traced_decorator(memory_exporter):
    """Verify span is created and contains service.name attribute."""
    
    @traced("test_span")
    async def sample_function():
        return "successfully traced"
        
    result = await sample_function()
    assert result == "successfully traced"
    
    spans = memory_exporter.get_finished_spans()
    assert len(spans) == 1
    
    span = spans[0]
    assert span.name == "test_span"
    
    # Check that service.name attribute is present in the resource
    assert span.resource.attributes.get(SERVICE_NAME) == "test-service"
