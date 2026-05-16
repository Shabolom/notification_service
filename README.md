# notification_service

Notification service на Go для обработки событий авторизации через RabbitMQ, отправки email-уведомлений через Resend и публикации событий в Kafka.

## Что делает сервис

Сервис читает события из RabbitMQ:

- регистрация пользователя
- логин пользователя

После получения события сервис:

1. валидирует сообщение;
2. отправляет email-уведомление пользователю;
3. публикует событие в Kafka;
4. подтверждает сообщение в RabbitMQ через `Ack`.

## Стек

- Go
- RabbitMQ
- Kafka
- Confluent Schema Registry
- Resend
- Zap Logger
- Docker Compose
- envconfig

## Архитектура

```text
Auth Service
    |
    | publish event
    v
RabbitMQ
    |
    | consume
    v
notification_service
    |
    | send email
    v
Resend

notification_service
    |
    | produce event
    v
Kafka
```

## Тип события

```go
type Event struct {
    Time  time.Time `json:"time"`
    Email string    `json:"email"`
    Type  string    `json:"type"`
}
```

Пример JSON:

```json
{
  "time": "2026-05-16T12:00:00Z",
  "email": "user@example.com",
  "type": "register"
}
```

## RabbitMQ

Используется exchange:

```text
auth.events
```

Очереди:

```text
auth.register
auth.login.logs
```

Сервис запускает отдельные worker-pool'ы для обработки событий регистрации и логина.

## Kafka

Сервис публикует обработанные события в Kafka topic.

Producer работает асинхронно. Delivery reports читаются через:

```go
producer.Events()
```

Это позволяет логировать успешную доставку сообщения или ошибку Kafka.

## Email-уведомления

Для отправки email используется Resend.

Сервис отправляет уведомления:

- об успешной регистрации;
- об успешном входе в аккаунт.

## Переменные окружения

Пример `.env`:

```env
APP_NAME=notification_service
APP_DEBUG=true

RABBITMQ_HOST=localhost
RABBITMQ_PORT=5672
RABBITMQ_USER=guest
RABBITMQ_PASSWORD=guest
RABBITMQ_VHOST=/

KAFKA_BROKERS=localhost:9092
KAFKA_TOPIC=auth.events
SCHEMA_REGISTRY_URL=http://localhost:8081

RESEND_API_KEY=re_xxxxxxxxx
```

## Запуск инфраструктуры

```bash
cd build/local
docker compose up -d
```

## Запуск сервиса

```bash
go run ./cmd/notification_service
```

## Основные особенности

- обработка сообщений из RabbitMQ;
- отдельные очереди для login/register событий;
- worker pool для конкурентной обработки сообщений;
- rate limiter на основе token channel;
- отправка email через Resend;
- публикация событий в Kafka;
- JSON Schema serialization через Schema Registry;
- context timeout на запись одного сообщения в Kafka;
- structured logging через Zap;
- ручной `Ack/Nack` для RabbitMQ-сообщений.

## Поведение при обработке сообщения

Успешный сценарий:

```text
RabbitMQ message
    -> validate content type
    -> send email
    -> publish event to Kafka
    -> Ack
```

Сценарий ошибки:

```text
RabbitMQ message
    -> error
    -> Nack
```

## Возможные улучшения

- добавить Dead Letter Queue;
- добавить retry-limit для сообщений;
- добавить Prometheus-метрики;
- добавить healthcheck endpoint;
- добавить OpenTelemetry tracing;
- добавить graceful shutdown для Kafka/RabbitMQ;
- добавить unit и integration тесты;
- вынести timeout и rate-limit настройки в конфиг.

## Назначение проекта

Проект демонстрирует базовую событийную микросервисную архитектуру:

- RabbitMQ consumer;
- Kafka producer;
- email notification provider;
- worker pool;
- rate limiting;
- context timeout;
- structured logging;
- Docker-based local infrastructure.
