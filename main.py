from datetime import datetime, timezone

from adapter.config.loader import load_config
from adapter.flask.app import create_app
from adapter.flask.auth import SourceAuthVerifier
from domain.event import IngestEventEnvelope
from usecase.ingest_event import IngestEvent


class _UtcTimeProvider:
    def now(self) -> datetime:
        return datetime.now(tz=timezone.utc)


class _StubKafkaPublisher:
    def publish(self, envelope: IngestEventEnvelope) -> None:
        raise NotImplementedError

    def ready(self) -> bool:
        return True


def main() -> None:
    config = load_config()

    auth_verifier = SourceAuthVerifier(config.source_token)
    time_provider = _UtcTimeProvider()
    publisher = _StubKafkaPublisher()

    ingest_event = IngestEvent(publisher=publisher, time_provider=time_provider)

    app = create_app(ingest_event, auth_verifier, publisher)
    app.run(host=config.host, port=config.port)


if __name__ == '__main__':
    main()
