from typing import Any

from flask import Blueprint, current_app, jsonify, request
from opentelemetry import metrics, trace
from opentelemetry.propagate import inject

from adapter.flask.auth import require_auth
from domain.error import InvalidPayloadError
from usecase.ingest_event import IngestEvent
from usecase.interface import KafkaPublisher

bp = Blueprint('api', __name__)

_tracer = trace.get_tracer(__name__)
_meter = metrics.get_meter(__name__)
_events_counter = _meter.create_counter(
    'http.events.accepted', description='Accepted events via HTTP'
)


@bp.post('/webhooks/v1/events')
@require_auth
def ingest_event() -> tuple[Any, int]:
    usecase: IngestEvent = current_app.config['ingest_event']
    raw_data = request.get_json(force=True, silent=True)
    if not isinstance(raw_data, dict):
        raise InvalidPayloadError()

    trace_context: dict[str, str] = {}
    inject(trace_context)

    with _tracer.start_as_current_span('usecase.ingest_event') as span:
        span.set_attribute('event.id', raw_data.get('event_id', ''))
        span.set_attribute('event.type', raw_data.get('event_type', ''))
        span.set_attribute('event.source', raw_data.get('source', ''))
        envelope = usecase.execute(raw_data, trace_context)

    _events_counter.add(1, {'source': envelope.source})
    return jsonify({'status': 'accepted', 'event_id': envelope.event_id}), 202


@bp.get('/health/live')
def health_live() -> tuple[Any, int]:
    return jsonify({'status': 'ok'}), 200


@bp.get('/health/ready')
def health_ready() -> tuple[Any, int]:
    publisher: KafkaPublisher = current_app.config['publisher']
    if not publisher.ready():
        return jsonify({'status': 'unavailable'}), 503
    return jsonify({'status': 'ready'}), 200
