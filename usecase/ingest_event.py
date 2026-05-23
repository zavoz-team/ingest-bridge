from datetime import datetime

from domain.error import InvalidPayloadError, MissingFieldError
from domain.event import EventIdentifiers, IngestEventEnvelope
from usecase.error import PublishFailedError
from usecase.interface import KafkaPublisher, TimeProvider


class IngestEvent:
    """Валидирует входящий payload, нормализует в envelope, публикует в Kafka"""

    def __init__(self, publisher: KafkaPublisher, time_provider: TimeProvider) -> None:
        self._publisher = publisher
        self._time_provider = time_provider

    def execute(
        self,
        raw: dict[str, object],
        trace_context: dict[str, str] | None = None,
    ) -> IngestEventEnvelope:
        # Валидация обязательных полей
        event_id = self._require_str(raw, 'event_id')
        event_type = self._require_str(raw, 'event_type')
        source = self._require_str(raw, 'source')
        occurred_at = self._require_datetime(raw, 'occurred_at')

        # Валидация identifiers
        raw_identifiers = raw.get('identifiers')
        if not isinstance(raw_identifiers, dict):
            raise MissingFieldError('identifiers')

        identifiers = EventIdentifiers(
            email=self._opt_str(raw_identifiers, 'email'),
            phone=self._opt_str(raw_identifiers, 'phone'),
            external_user_id=self._opt_str(raw_identifiers, 'external_user_id'),
            anonymous_id=self._opt_str(raw_identifiers, 'anonymous_id'),
        )

        # Нормализация
        raw_attributes = raw.get('attributes')
        attributes = raw_attributes if isinstance(raw_attributes, dict) else {}

        raw_payload = raw.get('payload')
        payload = raw_payload if isinstance(raw_payload, dict) else {}

        envelope = IngestEventEnvelope(
            event_id=event_id,
            event_type=event_type,
            source=source,
            occurred_at=occurred_at,
            received_at=self._time_provider.now(),
            identifiers=identifiers,
            attributes=attributes,
            payload=payload,
            trace_context=trace_context or {},
        )

        # Публикация
        try:
            self._publisher.publish(envelope)
        except Exception as exc:
            raise PublishFailedError(f'Failed to publish event {event_id}') from exc

        return envelope

    def _require_str(self, data: dict[str, object], field: str) -> str:
        value = data.get(field)
        if not isinstance(value, str) or not value:
            raise MissingFieldError(field)
        return value

    def _require_datetime(self, data: dict[str, object], field: str) -> datetime:
        value = data.get(field)
        if not isinstance(value, str) or not value:
            raise MissingFieldError(field)
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            raise InvalidPayloadError(f'Invalid datetime format for field: {field}')

    def _opt_str(self, data: dict[str, object], field: str) -> str | None:
        value = data.get(field)
        if isinstance(value, str) and value:
            return value
        return None
