from datetime import datetime, timezone

import pytest

from domain.error import InvalidPayloadError, MissingFieldError
from domain.event import IngestEventEnvelope
from usecase.error import PublishFailedError
from usecase.ingest_event import IngestEvent
from usecase.interface import KafkaPublisher, TimeProvider


class FakeTimeProvider(TimeProvider):
    def __init__(self, fixed: datetime) -> None:
        self._fixed = fixed

    def now(self) -> datetime:
        return self._fixed


class FakePublisher(KafkaPublisher):
    def __init__(self, *, should_fail: bool = False) -> None:
        self.published: list[IngestEventEnvelope] = []
        self._should_fail = should_fail

    def publish(self, envelope: IngestEventEnvelope) -> None:
        if self._should_fail:
            raise RuntimeError('Kafka unavailable')
        self.published.append(envelope)


VALID_RAW: dict[str, object] = {
    'event_id': 'evt_1',
    'event_type': 'page_view',
    'source': 'demo_shop',
    'occurred_at': '2026-03-24T10:00:00+00:00',
    'identifiers': {
        'email': 'user@example.com',
    },
    'attributes': {'page': '/home'},
    'payload': {'url': 'https://shop.example.com'},
}

FIXED_NOW = datetime(2026, 3, 24, 10, 0, 1, tzinfo=timezone.utc)


class TestIngestEventHappyPath:
    def test_publishes_envelope(self):
        publisher = FakePublisher()
        uc = IngestEvent(publisher, FakeTimeProvider(FIXED_NOW))

        envelope = uc.execute(dict(VALID_RAW))

        assert len(publisher.published) == 1
        assert envelope.event_id == 'evt_1'
        assert envelope.received_at == FIXED_NOW
        assert envelope.identifiers.email == 'user@example.com'
        assert envelope.attributes == {'page': '/home'}

    def test_trace_context_passed(self):
        publisher = FakePublisher()
        uc = IngestEvent(publisher, FakeTimeProvider(FIXED_NOW))

        envelope = uc.execute(dict(VALID_RAW), trace_context={'traceparent': '00-abc'})

        assert envelope.trace_context == {'traceparent': '00-abc'}


class TestIngestEventValidation:
    def _execute(self, raw: dict[str, object]) -> None:
        publisher = FakePublisher()
        uc = IngestEvent(publisher, FakeTimeProvider(FIXED_NOW))
        uc.execute(raw)

    def test_missing_event_id(self):
        raw = dict(VALID_RAW)
        del raw['event_id']
        with pytest.raises(MissingFieldError, match='event_id'):
            self._execute(raw)

    def test_missing_event_type(self):
        raw = dict(VALID_RAW)
        del raw['event_type']
        with pytest.raises(MissingFieldError, match='event_type'):
            self._execute(raw)

    def test_missing_source(self):
        raw = dict(VALID_RAW)
        del raw['source']
        with pytest.raises(MissingFieldError, match='source'):
            self._execute(raw)

    def test_missing_occurred_at(self):
        raw = dict(VALID_RAW)
        del raw['occurred_at']
        with pytest.raises(MissingFieldError, match='occurred_at'):
            self._execute(raw)

    def test_invalid_occurred_at_format(self):
        raw = dict(VALID_RAW)
        raw['occurred_at'] = 'not-a-date'
        with pytest.raises(InvalidPayloadError):
            self._execute(raw)

    def test_missing_identifiers(self):
        raw = dict(VALID_RAW)
        del raw['identifiers']
        with pytest.raises(MissingFieldError, match='identifiers'):
            self._execute(raw)

    def test_empty_identifiers(self):
        raw = dict(VALID_RAW)
        raw['identifiers'] = {}
        with pytest.raises(InvalidPayloadError):
            self._execute(raw)


class TestIngestEventPublishFailure:
    def test_raises_publish_failed_error(self):
        publisher = FakePublisher(should_fail=True)
        uc = IngestEvent(publisher, FakeTimeProvider(FIXED_NOW))

        with pytest.raises(PublishFailedError):
            uc.execute(dict(VALID_RAW))
