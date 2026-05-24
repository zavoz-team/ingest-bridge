from datetime import datetime, timezone

from flask import Flask

from adapter.config.loader import load_config
from adapter.flask.app import create_app
from adapter.flask.auth import SourceAuthVerifier
from adapter.kafka.producer import KafkaPublisher
from adapter.otel.setup import setup_otel
from usecase.ingest_event import IngestEvent


class _UtcTimeProvider:
    def now(self) -> datetime:
        return datetime.now(tz=timezone.utc)


def main() -> None:
    config = load_config()

    flask_app = Flask(__name__)
    setup_otel(flask_app)

    auth_verifier = SourceAuthVerifier(config.source_token)
    time_provider = _UtcTimeProvider()
    publisher = KafkaPublisher(config.kafka)

    ingest_event = IngestEvent(publisher=publisher, time_provider=time_provider)

    app = create_app(ingest_event, auth_verifier, publisher, flask_app=flask_app)
    app.run(host=config.host, port=config.port)


if __name__ == '__main__':
    main()
