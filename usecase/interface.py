from datetime import datetime
from typing import Protocol

from domain.event import IngestEventEnvelope


class KafkaPublisher(Protocol):
    def publish(self, envelope: IngestEventEnvelope) -> None: ...


class SourceAuthVerifier(Protocol):
    def verify(self, token: str) -> bool: ...


class TimeProvider(Protocol):
    def now(self) -> datetime: ...
