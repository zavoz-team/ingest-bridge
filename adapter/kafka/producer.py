import json
import logging
from dataclasses import asdict
from datetime import datetime
from typing import Any

from confluent_kafka import KafkaException, Producer

from adapter.config.loader import KafkaConfig
from domain.error import PublishError
from domain.event import IngestEventEnvelope

logger = logging.getLogger(__name__)


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

    def publish(self, envelope: IngestEventEnvelope) -> None:
        # serialize and publish
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

        except KafkaException as e:
            logger.error(f'kafka error {e}')
            raise PublishError(f'kafka error {e}') from e
        except Exception as e:
            logger.exception('publish error')
            raise PublishError(f'publish error {e}') from e

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
