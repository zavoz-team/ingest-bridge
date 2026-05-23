import os
from dataclasses import dataclass
from pathlib import Path

import yaml

_CONFIG_YAML = Path(__file__).parent.parent.parent / 'config' / 'app.yaml'


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str
    topic: str
    publish_timeout_seconds: int


@dataclass(frozen=True)
class AppConfig:
    name: str
    env: str
    host: str
    port: int
    source_token: str
    kafka: KafkaConfig


def load_config(yaml_path: Path | None = None) -> AppConfig:
    path = yaml_path or _CONFIG_YAML
    with open(path) as f:
        raw: dict[str, object] = yaml.safe_load(f)

    kafka_raw = raw.get('kafka') or {}
    if not isinstance(kafka_raw, dict):
        kafka_raw = {}

    kafka = KafkaConfig(
        bootstrap_servers=os.environ.get(
            'INGEST_BRIDGE_KAFKA_BOOTSTRAP_SERVERS',
            str(kafka_raw.get('bootstrap_servers', 'localhost:9092')),
        ),
        topic=os.environ.get(
            'INGEST_BRIDGE_KAFKA_TOPIC',
            str(kafka_raw.get('topic', 'cdp.events.v1')),
        ),
        publish_timeout_seconds=int(
            os.environ.get('INGEST_BRIDGE_KAFKA_PUBLISH_TIMEOUT_SECONDS') or str(kafka_raw.get('publish_timeout_seconds', 10))
        ),
    )

    return AppConfig(
        name=str(raw.get('name', 'ingest-bridge')),
        env=str(raw.get('env', 'development')),
        host=os.environ.get('INGEST_BRIDGE_HOST') or str(raw.get('host', '0.0.0.0')),
        port=int(os.environ.get('INGEST_BRIDGE_PORT') or str(raw.get('port', 8080))),
        source_token=os.environ.get('INGEST_BRIDGE_SOURCE_TOKEN') or str(raw.get('source_token', '')),
        kafka=kafka,
    )
