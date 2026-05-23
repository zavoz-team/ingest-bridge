from dataclasses import dataclass, field
from datetime import datetime

from domain.error import InvalidPayloadError


@dataclass(frozen=True)
class EventIdentifiers:
    email: str | None = None
    phone: str | None = None
    external_user_id: str | None = None
    anonymous_id: str | None = None

    def __post_init__(self) -> None:
        has_any = any(
            [
                self.email,
                self.phone,
                self.external_user_id,
                self.anonymous_id,
            ]
        )
        if not has_any:
            raise InvalidPayloadError('At least one identifier must be provided')


@dataclass(frozen=True)
class IngestEventEnvelope:
    """Нормализованное событие для публикации в Kafka"""

    event_id: str
    event_type: str
    source: str
    occurred_at: datetime
    received_at: datetime
    identifiers: EventIdentifiers
    attributes: dict[str, object] = field(default_factory=dict)
    payload: dict[str, object] = field(default_factory=dict)
    trace_context: dict[str, str] = field(default_factory=dict)
