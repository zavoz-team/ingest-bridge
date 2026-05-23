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
    def __init__(self, *, should_fail: bool = False, is_ready: bool = True) -> None:
        self.published: list[IngestEventEnvelope] = []
        self._should_fail = should_fail
        self._is_ready = is_ready

    def publish(self, envelope: IngestEventEnvelope) -> None:
        if self._should_fail:
            raise RuntimeError('error')
        self.published.append(envelope)

    def ready(self) -> bool:
        return self._is_ready


VALID_RAW: dict[str, object] = {
    'event_id': 'evt_1',
    'event_type': 'page_view',
    'source': 'demo_shop',
    'occurred_at': '2026-03-24T10:00:00+00:00',
    'identifiers': {'email': 'user@example.com'},
}

FIXED_NOW = datetime(2026, 3, 24, 10, 0, 1, tzinfo=timezone.utc)


def test_ingest_valid_event():
    publisher = FakePublisher()
    uc = IngestEvent(publisher, FakeTimeProvider(FIXED_NOW))
    envelope = uc.execute(dict(VALID_RAW))
    assert len(publisher.published) == 1
    assert envelope.event_id == 'evt_1'


def test_ingest_with_anonymous_id_only():
    raw = dict(VALID_RAW)
    raw['identifiers'] = {'anonymous_id': 'anon_1'}
    publisher = FakePublisher()
    uc = IngestEvent(publisher, FakeTimeProvider(FIXED_NOW))
    envelope = uc.execute(raw)
    assert envelope.identifiers.anonymous_id == 'anon_1'


def test_missing_event_id():
    raw = dict(VALID_RAW)
    del raw['event_id']
    uc = IngestEvent(FakePublisher(), FakeTimeProvider(FIXED_NOW))
    with pytest.raises(MissingFieldError):
        uc.execute(raw)


def test_missing_identifiers():
    raw = dict(VALID_RAW)
    del raw['identifiers']
    uc = IngestEvent(FakePublisher(), FakeTimeProvider(FIXED_NOW))
    with pytest.raises(MissingFieldError):
        uc.execute(raw)


def test_empty_identifiers():
    raw = dict(VALID_RAW)
    raw['identifiers'] = {}
    uc = IngestEvent(FakePublisher(), FakeTimeProvider(FIXED_NOW))
    with pytest.raises(InvalidPayloadError):
        uc.execute(raw)


def test_publish_failure():
    publisher = FakePublisher(should_fail=True)
    uc = IngestEvent(publisher, FakeTimeProvider(FIXED_NOW))
    with pytest.raises(PublishFailedError):
        uc.execute(dict(VALID_RAW))
