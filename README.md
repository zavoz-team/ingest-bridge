# ingest-bridge

Входной сервис CDP на Flask + Kafka

Принимает внешние события от источников (demo shop, CRM, мобильное приложение), валидирует и нормализует их, публикует во внутренний Kafka topic `cdp.events.v1` для обработки `cdp-core`

## Оглавление

* [Назначение](#назначение)
* [Быстрый старт](#быстрый-старт)
* [Структура проекта](#структура-проекта)
* [API](#api)
* [Kafka envelope](#kafka-envelope)
* [Переменные окружения](#переменные-окружения)
* [Команды разработки](#команды-разработки)
* [Observability](#observability)
* [Ограничения MVP](#ограничения-mvp)

---

## Назначение

`ingest-bridge` отвечает за:

* приём внешних HTTP запросов с событиями
* аутентификацию источника по API token
* валидацию структуры входящего payload
* нормализацию payload во внутренний event envelope
* публикацию нормализованного события в Kafka
* health endpoints для инфраструктуры

Сервис не хранит состояние, не обрабатывает события, не занимается identity resolution - это ответственность `cdp-core`

---

## Быстрый старт

### Локальный запуск через Docker Compose

```bash
cp .env.example .env
# задать INGEST_BRIDGE_SOURCE_TOKEN в .env
docker compose up
```

Compose поднимает:

* `kafka` - Kafka broker (KRaft mode, apache/kafka:3.9.0)
* `kafka-setup` - создаёт topic `cdp.events.v1`
* `otel-collector` - Grafana LGTM (трейсы, метрики, логи), доступен на `http://localhost:3000`
* `ingest-bridge` - сам сервис на `http://localhost:8080`

### Проверка

```bash
# liveness
curl http://localhost:8080/health/live

# отправить тестовое событие
curl -X POST http://localhost:8080/webhooks/v1/events \
  -H "Authorization: Bearer dev-secret-token" \
  -H "Content-Type: application/json" \
  -d '{
    "event_id": "evt_001",
    "event_type": "page_view",
    "source": "demo_shop",
    "occurred_at": "2026-05-24T10:00:00Z",
    "identifiers": { "email": "user@example.com" }
  }'
```

Ожидаемый ответ: `202 Accepted`

---

## Структура проекта

```text
ingest-bridge/
├── domain/                     # сущности и доменные ошибки
│   ├── event.py                # IngestEventEnvelope, EventIdentifiers
│   └── error.py                # IngestError, InvalidPayloadError, AuthenticationError
├── usecase/                    # порты и use cases
│   ├── interface.py            # KafkaPublisher, SourceAuthVerifier, TimeProvider
│   ├── ingest_event.py         # основной use case
│   └── error.py                # ValidationError, PublishFailedError
├── adapter/                    # внешние интеграции
│   ├── flask/                  # HTTP layer
│   │   ├── app.py              # Flask app factory
│   │   ├── routes.py           # webhook и health routes
│   │   ├── auth.py             # token verification
│   │   └── errors.py           # error handlers → HTTP codes
│   ├── kafka/
│   │   └── producer.py         # KafkaPublisher implementation
│   ├── otel/
│   │   └── setup.py            # OTEL instrumentation setup
│   └── config/
│       └── loader.py           # YAML + env config loader
├── repository/                 # пусто в MVP (нет persistence)
├── config/
│   └── app.yaml                # базовый конфиг
├── docs/                       # ADR и архитектурные решения
├── tests/                      # unit и integration тесты
├── main.py                     # точка входа
├── Makefile
├── Dockerfile
└── docker-compose.yml
```

### Dependency direction

```
domain/ <- usecase/ <- adapter/
```

`domain/` не импортирует ничего внутреннего
`usecase/` импортирует только `domain/`
`adapter/` импортирует `usecase/` и `domain/`
Flask, Kafka, OTEL, config - только в `adapter/`

---

## API

### `POST /webhooks/v1/events`

Принять одно событие от внешнего источника

**Headers**

| Header | Значение |
|---|---|
| `Authorization` | `Bearer <source-token>` |
| `Content-Type` | `application/json` |

**Request body**

```json
{
  "event_id": "evt_abc123",
  "event_type": "page_view",
  "source": "demo_shop",
  "occurred_at": "2026-05-24T10:00:00Z",
  "identifiers": {
    "email": "user@example.com",
    "phone": "+79990000000",
    "external_user_id": "shop_user_123",
    "anonymous_id": "anon_456"
  },
  "attributes": {},
  "payload": {}
}
```

Обязательные поля: `event_id`, `event_type`, `source`, `occurred_at`, `identifiers`
В `identifiers` хотя бы одно поле должно быть заполнено

**Responses**

| Код | Описание |
|---|---|
| `202 Accepted` | событие принято и опубликовано в Kafka |
| `400 Bad Request` | невалидный payload |
| `401 Unauthorized` | отсутствует или невалидный token |
| `422 Unprocessable Entity` | payload не соответствует contract |
| `503 Service Unavailable` | Kafka publish failure |

---

### `GET /health/live`

Liveness probe - сервис запущен

```json
{"status": "ok"}
```

---

### `GET /health/ready`

Readiness probe - проверяет доступность Kafka broker

```json
{"status": "ok"}
```

При недоступном Kafka: `503 Service Unavailable`

---

## Kafka envelope

Topic: `cdp.events.v1`
Key: `event_id`

```json
{
  "event_id": "evt_abc123",
  "event_type": "page_view",
  "source": "demo_shop",
  "occurred_at": "2026-05-24T10:00:00Z",
  "received_at": "2026-05-24T10:00:01Z",
  "identifiers": {
    "email": "user@example.com",
    "phone": "+79990000000",
    "external_user_id": "shop_user_123",
    "anonymous_id": "anon_456"
  },
  "attributes": {},
  "payload": {},
  "trace_context": {}
}
```

`received_at` добавляется сервисом при получении
`trace_context` пробрасывается из входящего HTTP request для сквозной трассировки

---

## Переменные окружения

| Переменная | Обязательная | Описание |
|---|---|---|
| `INGEST_BRIDGE_SOURCE_TOKEN` | да | API token для аутентификации источника |
| `INGEST_BRIDGE_KAFKA_BOOTSTRAP_SERVERS` | нет | адрес Kafka broker (default: `kafka:9092`) |
| `INGEST_BRIDGE_KAFKA_TOPIC` | нет | Kafka topic (default: `cdp.events.v1`) |
| `INGEST_BRIDGE_HOST` | нет | bind host (default: `0.0.0.0`) |
| `INGEST_BRIDGE_PORT` | нет | bind port (default: `8080`) |
| `OTEL_ENABLED` | нет | включить OTEL instrumentation (default: `false`) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | нет | OTLP gRPC endpoint (default: `http://otel-collector:4317`) |

Пример: см `.env.example`

---

## Команды разработки

```bash
make install       # установить зависимости через uv
make run           # запустить сервис локально
make test          # pytest
make lint          # ruff check
make typecheck     # mypy
make pre-commit    # lint + typecheck + test
make docker-build  # собрать образ локально
make docker-push   # собрать multi-platform и запушить в Docker Hub
```

Зависимости управляются через `uv`

---

## Observability

Сервис инструментирован OpenTelemetry:

* HTTP server spans для всех requests (Flask instrumentation)
* Publish spans для Kafka producer
* Trace context propagation: входящий HTTP request → Kafka message headers
* Метрики: request count, latency, publish errors
* Service name: `ingest-bridge`

Трейсы, метрики и логи экспортируются через OTLP в `otel-collector`
Grafana UI доступна на `http://localhost:3000` при локальном запуске через Docker Compose

При отсутствии collector сервис работает в штатном режиме (graceful degradation)

---

## Ограничения MVP

* Один источник с одним API token (multi-source auth не в scope)
* Нет rate limiting
* Нет batch ingestion (один запрос = одно событие)
* Нет persistent storage
* Нет retry при Kafka publish failure (возвращает 503)
* DLQ логика - ответственность `cdp-core worker`
