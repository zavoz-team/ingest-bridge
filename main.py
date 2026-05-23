from datetime import datetime, timezone

from flask import Flask, jsonify

from adapter.config.loader import load_config
from adapter.otel.setup import setup_otel
from domain.event import IngestEventEnvelope
from usecase.ingest_event import IngestEvent


class _TokenSourceAuthVerifier:
    def __init__(self, token: str) -> None:
        self._token = token

    def verify(self, token: str) -> bool:
        return token == self._token


class _UtcTimeProvider:
    def now(self) -> datetime:
        return datetime.now(tz=timezone.utc)


class _StubKafkaPublisher:
    """Placeholder until the real Kafka producer adapter is wired in."""

    def publish(self, envelope: IngestEventEnvelope) -> None:
        raise NotImplementedError


def _create_app(ingest_event: IngestEvent, auth_verifier: _TokenSourceAuthVerifier) -> Flask:
    app = Flask(__name__)
    setup_otel(app)
    app.config['ingest_event'] = ingest_event
    app.config['auth_verifier'] = auth_verifier

    @app.get('/health/live')
    def health_live():
        return jsonify({'status': 'ok'}), 200

    return app


def main() -> None:
    config = load_config()

    auth_verifier = _TokenSourceAuthVerifier(config.source_token)
    time_provider = _UtcTimeProvider()
    publisher = _StubKafkaPublisher()

    ingest_event = IngestEvent(publisher=publisher, time_provider=time_provider)

    app = _create_app(ingest_event, auth_verifier)
    app.run(host=config.host, port=config.port)


if __name__ == '__main__':
    main()
