"""Bootstrap OTel condiviso — equivalente Python di InitTracing (Go,
SPEC-011) / init_tracing (Rust, SPEC-010): stesso exporter
stdout/console, stessa filosofia."""

from opentelemetry import trace
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import ConsoleSpanExporter, SimpleSpanProcessor


def init_tracing(service_name: str) -> TracerProvider:
    """Inizializza il TracerProvider OTel con un ConsoleSpanExporter.
    Il chiamante è responsabile di invocare .shutdown() sul provider
    ritornato alla chiusura del processo (blocco finally o
    atexit.register), per il flush pulito degli span in buffer —
    stesso pattern di guard/shutdown già usato in Rust (SPEC-010) e
    Go (SPEC-011)."""
    provider = TracerProvider(resource=Resource.create({SERVICE_NAME: service_name}))
    provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))
    trace.set_tracer_provider(provider)
    return provider
