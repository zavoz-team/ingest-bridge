from flask import Flask

from adapter.flask.auth import SourceAuthVerifier
from adapter.flask.errors import register_error_handlers
from adapter.flask.routes import bp
from adapter.otel.setup import setup_otel
from usecase.ingest_event import IngestEvent


def create_app(ingest_event: IngestEvent, auth_verifier: SourceAuthVerifier) -> Flask:
    app = Flask(__name__)
    setup_otel(app)

    app.config['ingest_event'] = ingest_event
    app.config['auth_verifier'] = auth_verifier

    app.register_blueprint(bp)
    register_error_handlers(app)

    return app
