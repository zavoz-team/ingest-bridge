import logging
import os

from flask import Flask
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.confluent_kafka import ConfluentKafkaInstrumentor
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

logger = logging.getLogger(__name__)

def setup_otel(app: Flask, service_name: str = "ingest-bridge"):
    otel_enabled = os.environ.get("OTEL_ENABLED", "false").lower() in ("true", "1", "yes")

    if not otel_enabled:
        logger.info("OpenTelemetry is disabled via OTEL_ENABLED.")
        provider = TracerProvider()
        trace.set_tracer_provider(provider)
        return

    logger.info(f"Setting up OpenTelemetry for service: {service_name}")

    resource = Resource.create({
        SERVICE_NAME: service_name
    })

    provider = TracerProvider(resource=resource)
    trace.set_tracer_provider(provider)

    try:
        otlp_exporter = OTLPSpanExporter()
        span_processor = BatchSpanProcessor(otlp_exporter)
        provider.add_span_processor(span_processor)
    except Exception as e:
         logger.warning(f"Failed to initialize OTLP Span Exporter: {e}. Traces will not be exported.")

    FlaskInstrumentor().instrument_app(app)

    ConfluentKafkaInstrumentor().instrument()

    logger.info("OpenTelemetry setup complete.")
