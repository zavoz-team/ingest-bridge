import logging
import os

from flask import Flask
from opentelemetry import metrics, trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.confluent_kafka import ConfluentKafkaInstrumentor
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


def _setup_metrics(resource: Resource) -> None:
    reader = PeriodicExportingMetricReader(OTLPMetricExporter())
    meter_provider = MeterProvider(resource=resource, metric_readers=[reader])
    metrics.set_meter_provider(meter_provider)


def _setup_logging(resource: Resource) -> None:
    logger_provider = LoggerProvider(resource=resource)
    logger_provider.add_log_record_processor(BatchLogRecordProcessor(OTLPLogExporter()))
    set_logger_provider(logger_provider)
    handler = LoggingHandler(level=logging.NOTSET, logger_provider=logger_provider)
    logging.getLogger().addHandler(handler)


def setup_otel(app: Flask, service_name: str = 'ingest-bridge') -> None:
    otel_enabled = os.environ.get('OTEL_ENABLED', 'false').lower() in (
        'true',
        '1',
        'yes',
    )

    if not otel_enabled:
        print('OpenTelemetry is disabled via OTEL_ENABLED')
        provider = TracerProvider()
        trace.set_tracer_provider(provider)
        return

    print(f'Setting up OpenTelemetry for service: {service_name}')

    resource = Resource.create({SERVICE_NAME: service_name})

    provider = TracerProvider(resource=resource)
    trace.set_tracer_provider(provider)

    try:
        otlp_exporter = OTLPSpanExporter()
        span_processor = BatchSpanProcessor(otlp_exporter)
        provider.add_span_processor(span_processor)
    except Exception as e:
        print(
            f'Failed to initialize OTLP Span Exporter: {e} Traces will not be exported'
        )

    _setup_metrics(resource)
    _setup_logging(resource)

    FlaskInstrumentor().instrument_app(app)
    ConfluentKafkaInstrumentor().instrument()

    print('OpenTelemetry setup complete')
