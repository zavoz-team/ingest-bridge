import json
import logging
import time
from dataclasses import asdict
from datetime import datetime
from typing import Any

from confluent_kafka import KafkaException, Producer
from opentelemetry import metrics, trace
from opentelemetry.trace.status import Status, StatusCode

from adapter.config.loader import KafkaConfig
from domain.error import PublishError
from domain.event import IngestEventEnvelope

logger = logging.getLogger(__name__)

tracer = trace.get_tracer(__name__)


class KafkaPublisher:
    # kafka publisher implementation

    def __init__(self, config: KafkaConfig) -> None:
        self._topic = config.topic
        self._timeout = config.publish_timeout_seconds
        self._producer = Producer(
            {
                'bootstrap.servers': config.bootstrap_servers,
                'client.id': 'ingest-bridge-producer',
                'acks': 'all',
            }
        )
        _meter = metrics.get_meter(__name__)
        self._publish_counter = _meter.create_counter(
            'kafka.publish.total',
            description='Number of Kafka publish attempts',
        )
        self._error_counter = _meter.create_counter(
            'kafka.publish.errors',
            description='Number of Kafka publish errors',
        )
        self._duration = _meter.create_histogram(
            'kafka.publish.duration_ms',
            description='Kafka publish duration in milliseconds',
            unit='ms',
        )

    def publish(self, envelope: IngestEventEnvelope) -> None:
        # serialize and publish
        start = time.time()
        with tracer.start_as_current_span('kafka.publish') as span:
            span.set_attribute('messaging.destination', self._topic)
            span.set_attribute('messaging.message_id', envelope.event_id)
            try:
                payload = self._serialize(envelope)
                headers = self._extract_headers(envelope)

                self._producer.produce(
                    topic=self._topic,
                    key=envelope.event_id,
                    value=payload,
                    headers=headers,
                    on_delivery=self._delivery_report,
                )

                self._producer.poll(0)

                remaining = self._producer.flush(timeout=self._timeout)
                if remaining > 0:
                    raise PublishError('timeout')

                self._publish_counter.add(1, {'topic': self._topic})
                self._duration.record(
                    int((time.time() - start) * 1000), {'topic': self._topic}
                )

            except KafkaException as e:
                logger.error(f'kafka error {e}')
                self._error_counter.add(1, {'topic': self._topic})
                self._duration.record(
                    int((time.time() - start) * 1000), {'topic': self._topic}
                )
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise PublishError(f'kafka error {e}') from e
            except Exception as e:
                logger.exception('publish error')
                self._error_counter.add(1, {'topic': self._topic})
                self._duration.record(
                    int((time.time() - start) * 1000), {'topic': self._topic}
                )
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise PublishError(f'publish error {e}') from e

    def ready(self) -> bool:
        # check kafka readiness
        try:
            self._producer.list_topics(timeout=1.0)
            return True
        except KafkaException:
            return False

    def close(self) -> None:
        # close producer
        logger.info('closing producer')
        self._producer.flush(timeout=self._timeout)

    def _serialize(self, envelope: IngestEventEnvelope) -> bytes:
        def default(obj: Any) -> Any:
            if isinstance(obj, datetime):
                return obj.isoformat()
            return obj

        return json.dumps(asdict(envelope), default=default).encode('utf-8')

    def _extract_headers(
        self, envelope: IngestEventEnvelope
    ) -> list[tuple[str, str | bytes | None]]:
        return [(k, v.encode('utf-8')) for k, v in envelope.trace_context.items()]

    def _delivery_report(self, err: Any, msg: Any) -> None:
        if err is not None:
            logger.error(f'delivery failed {err}')
        else:
            logger.debug(f'delivered {msg.topic()} {msg.partition()}')
