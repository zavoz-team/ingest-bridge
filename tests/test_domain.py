from datetime import datetime, timezone

import pytest

from domain.error import InvalidPayloadError
from domain.event import EventIdentifiers, IngestEventEnvelope


class TestEventIdentifiers:
    def test_valid_with_email(self):
        ids = EventIdentifiers(email='user@example.com')
        assert ids.email == 'user@example.com'

    def test_valid_with_anonymous_id(self):
        ids = EventIdentifiers(anonymous_id='anon_123')
        assert ids.anonymous_id == 'anon_123'

    def test_valid_with_multiple(self):
        ids = EventIdentifiers(email='a@b.com', phone='+79990000000')
        assert ids.email == 'a@b.com'
        assert ids.phone == '+79990000000'

    def test_raises_when_all_none(self):
        with pytest.raises(InvalidPayloadError):
            EventIdentifiers()

    def test_raises_when_all_empty_strings(self):
        with pytest.raises(InvalidPayloadError):
            EventIdentifiers(email='', phone='', external_user_id='', anonymous_id='')


class TestIngestEventEnvelope:
    def test_creates_envelope(self):
        now = datetime.now(timezone.utc)
        ids = EventIdentifiers(email='u@e.com')
        envelope = IngestEventEnvelope(
            event_id='evt_1',
            event_type='page_view',
            source='shop',
            occurred_at=now,
            received_at=now,
            identifiers=ids,
        )
        assert envelope.event_id == 'evt_1'
        assert envelope.attributes == {}
        assert envelope.payload == {}
        assert envelope.trace_context == {}

    def test_envelope_is_frozen(self):
        now = datetime.now(timezone.utc)
        ids = EventIdentifiers(email='u@e.com')
        envelope = IngestEventEnvelope(
            event_id='evt_1',
            event_type='page_view',
            source='shop',
            occurred_at=now,
            received_at=now,
            identifiers=ids,
        )
        with pytest.raises(Exception):
            envelope.event_id = 'changed'  # type: ignore[misc]
