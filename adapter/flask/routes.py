from typing import Any

from flask import Blueprint, current_app, jsonify, request
from opentelemetry.propagate import inject

from adapter.flask.auth import require_auth
from domain.error import InvalidPayloadError
from usecase.ingest_event import IngestEvent

bp = Blueprint('api', __name__)


@bp.post('/webhooks/v1/events')
@require_auth
def ingest_event() -> tuple[Any, int]:
    usecase: IngestEvent = current_app.config['ingest_event']
    raw_data = request.get_json(force=True, silent=True)
    if not isinstance(raw_data, dict):
        raise InvalidPayloadError()

    trace_context: dict[str, str] = {}
    inject(trace_context)

    envelope = usecase.execute(raw_data, trace_context)
    return jsonify({'status': 'accepted', 'event_id': envelope.event_id}), 202


@bp.get('/health/live')
def health_live() -> tuple[Any, int]:
    return jsonify({'status': 'ok'}), 200


@bp.get('/health/ready')
def health_ready() -> tuple[Any, int]:
    return jsonify({'status': 'ready'}), 200
