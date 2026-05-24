from datetime import datetime, timezone

import pytest

from adapter.flask.app import create_app
from adapter.flask.auth import SourceAuthVerifier
from domain.error import PublishError
from domain.event import IngestEventEnvelope
from usecase.ingest_event import IngestEvent
from usecase.interface import KafkaPublisher, TimeProvider

TOKEN = 'test-token'


class FakeTimeProvider(TimeProvider):
    def now(self) -> datetime:
        return datetime(2026, 3, 24, 10, 0, 0, tzinfo=timezone.utc)


class FakePublisher(KafkaPublisher):
    def __init__(self, *, should_fail: bool = False, is_ready: bool = True) -> None:
        self.published: list[IngestEventEnvelope] = []
        self._should_fail = should_fail
        self._is_ready = is_ready

    def publish(self, envelope: IngestEventEnvelope) -> None:
        if self._should_fail:
            raise PublishError('error')
        self.published.append(envelope)

    def ready(self) -> bool:
        return self._is_ready


@pytest.fixture
def app_factory():
    def _factory(publisher: KafkaPublisher):
        auth = SourceAuthVerifier(TOKEN)
        uc = IngestEvent(publisher, FakeTimeProvider())
        return create_app(uc, auth, publisher)

    return _factory


@pytest.fixture
def client(app_factory):
    publisher = FakePublisher()
    app = app_factory(publisher)
    return app.test_client()


VALID_PAYLOAD = {
    'event_id': 'evt_1',
    'event_type': 'page_view',
    'source': 'shop',
    'occurred_at': '2026-03-24T10:00:00Z',
    'identifiers': {'email': 'user@example.com'},
}


def test_ingest_event_accepted(client):
    resp = client.post(
        '/webhooks/v1/events',
        json=VALID_PAYLOAD,
        headers={'Authorization': f'Bearer {TOKEN}'},
    )
    assert resp.status_code == 202
    assert resp.get_json() == {'status': 'accepted', 'event_id': 'evt_1'}


def test_invalid_token(client):
    resp = client.post(
        '/webhooks/v1/events',
        json=VALID_PAYLOAD,
        headers={'Authorization': 'Bearer wrong'},
    )
    assert resp.status_code == 401


def test_missing_auth_header(client):
    resp = client.post('/webhooks/v1/events', json=VALID_PAYLOAD)
    assert resp.status_code == 401


def test_kafka_publish_failure(app_factory):
    publisher = FakePublisher(should_fail=True)
    client = app_factory(publisher).test_client()
    resp = client.post(
        '/webhooks/v1/events',
        json=VALID_PAYLOAD,
        headers={'Authorization': f'Bearer {TOKEN}'},
    )
    assert resp.status_code == 503


def test_health_live(client):
    resp = client.get('/health/live')
    assert resp.status_code == 200
    assert resp.get_json() == {'status': 'ok'}


def test_health_ready_available(client):
    resp = client.get('/health/ready')
    assert resp.status_code == 200
    assert resp.get_json() == {'status': 'ready'}


def test_health_ready_unavailable(app_factory):
    publisher = FakePublisher(is_ready=False)
    client = app_factory(publisher).test_client()
    resp = client.get('/health/ready')
    assert resp.status_code == 503
    assert resp.get_json() == {'status': 'unavailable'}
