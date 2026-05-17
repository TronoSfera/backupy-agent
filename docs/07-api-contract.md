# 07. API-контракты

## 1. Транспорт Agent ↔ Server

- **Протокол**: WebSocket Secure (WSS) на :443, бинарные фреймы с Protobuf-payload'ами.
- **Альтернатива** для Phase 3+: gRPC bidirectional stream.
- Длинноживущее соединение, инициируемое агентом.
- Heartbeat: PING каждые 30 секунд, RTO 90 секунд.
- Reconnect: exponential backoff 1s → 2s → 4s → … → 60s cap, jitter ±20%.

## 2. Аутентификация WSS

1. Агент открывает WSS-соединение с заголовком `Authorization: Bearer <agent_key>`.
2. Сервер валидирует key (argon2-хэш в БД), достаёт agent_id.
3. Первое сообщение от агента — `Register`, сервер отвечает `RegisterAck` с config snapshot и assigned `session_id`.
4. Все последующие сообщения содержат monotonic `seq` для дедупликации.

## 3. Envelope

```protobuf
syntax = "proto3";
package backup.v1;

message Envelope {
  uint64 seq = 1;              // monotonic, per-direction
  uint64 ts_ms = 2;            // unix ms, отправителя
  string correlation_id = 3;   // для request/response пар
  oneof payload {
    // ----- agent → server -----
    Register register = 10;
    Heartbeat heartbeat = 11;
    DiscoveryReport discovery = 12;
    JobUpdate job_update = 13;
    BackupCompleted backup_completed = 14;
    HealthCheckResult health_result = 15;
    LogEvent log = 16;
    RestoreUpdate restore_update = 17;
    Ack ack = 18;

    // ----- server → agent -----
    RegisterAck register_ack = 50;
    ConfigUpdate config_update = 51;
    RunBackup run_backup = 52;
    CancelJob cancel_job = 53;
    RunHealthCheck run_health_check = 54;
    SelfUpdate self_update = 56;
    Ping ping = 57;
  }
}
```

> **Изменение относительно v1:** `RunRestore` удалён из MVP. В Phase 1 нет restore-в-БД, только download через REST endpoint (`GET /runs/:id/download-url` + локальная расшифровка CLI-утилитой). См. [04-server-spec.md → Download](./04-server-spec.md).

## 4. Сообщения Agent → Server

### Register

```protobuf
message Register {
  string agent_version = 1;
  string hostname = 2;
  string os = 3;                    // "linux"
  string arch = 4;                  // "amd64"
  string docker_version = 5;
  repeated string capabilities = 6; // ["pg_dump", "mysqldump", "docker_discovery"]
  uint64 last_known_config_version = 7;
}
```

### Heartbeat

```protobuf
message Heartbeat {
  uint64 config_version = 1;
  AgentMetrics metrics = 2;
  repeated string active_job_ids = 3;
}
message AgentMetrics {
  float cpu_percent = 1;
  uint64 mem_used_bytes = 2;
  uint64 mem_total_bytes = 3;
  uint64 disk_used_bytes = 4;
  uint64 disk_total_bytes = 5;
  uint32 queue_depth = 6;
}
```

Частота: каждые 30 секунд.

### DiscoveryReport

```protobuf
message DiscoveryReport {
  repeated DiscoveredContainer containers = 1;
}
message DiscoveredContainer {
  string container_id = 1;
  string name = 2;
  string image = 3;              // "postgres:16"
  string detected_db_type = 4;   // "postgresql"
  repeated string networks = 5;
  map<string,string> env_hints = 6; // отфильтрованные env без секретов
  repeated PortBinding ports = 7;
}
message PortBinding {
  uint32 container_port = 1;
  uint32 host_port = 2;
  string protocol = 3;
}
```

Отправляется при старте и при изменениях docker events.

### JobUpdate

```protobuf
message JobUpdate {
  string job_id = 1;
  JobStatus status = 2;
  uint32 progress_percent = 3;
  string current_step = 4;        // "dumping", "compressing", "uploading"
  string error_message = 5;       // если status=FAILED
}
enum JobStatus {
  JOB_STATUS_UNSPECIFIED = 0;
  QUEUED = 1;
  RUNNING = 2;
  SUCCESS = 3;
  FAILED = 4;
  CANCELLED = 5;
}
```

### BackupCompleted

```protobuf
message BackupCompleted {
  string job_id = 1;
  string run_id = 2;
  string s3_key = 3;
  uint64 size_bytes = 4;
  string sha256 = 5;
  uint64 duration_ms = 6;
  string dek_kms_id = 7;
  bytes encrypted_dek = 8;
  string compression = 9;         // "zstd"
  string db_engine_version = 10;  // "PostgreSQL 16.2"
}
```

### HealthCheckResult

```protobuf
message HealthCheckResult {
  string check_id = 1;
  uint64 ts_ms = 2;
  bool ok = 3;
  uint32 latency_ms = 4;
  string error = 5;
  uint32 status_code = 6;         // для HTTP
}
```

### LogEvent

```protobuf
message LogEvent {
  uint64 ts_ms = 1;
  LogLevel level = 2;
  string job_id = 3;              // опционально
  string message = 4;
  map<string,string> fields = 5;
}
enum LogLevel { TRACE=0; DEBUG=1; INFO=2; WARN=3; ERROR=4; }
```

### RestoreUpdate

```protobuf
message RestoreUpdate {
  string restore_id = 1;
  JobStatus status = 2;
  uint32 progress_percent = 3;
  string current_step = 4;
  string error_message = 5;
}
```

### Ack

```protobuf
message Ack {
  string correlation_id = 1;
  bool accepted = 2;
  string reason = 3;              // если accepted=false
}
```

## 5. Сообщения Server → Agent

### RegisterAck

```protobuf
message RegisterAck {
  string session_id = 1;
  AgentConfig config = 2;
  uint32 heartbeat_interval_sec = 3;
  string server_time_iso = 4;
}
```

### AgentConfig (используется в RegisterAck и ConfigUpdate)

```protobuf
message AgentConfig {
  uint64 version = 1;
  repeated Target targets = 2;
  repeated BackupJobSpec jobs = 3;
  repeated HealthCheckSpec health_checks = 4;
  MaintenanceWindow maintenance = 5;
  AdvancedSettings advanced = 6;
}

message Target {
  string id = 1;
  DbType type = 2;
  string display_name = 3;
  ConnectionConfig connection = 4;
}

enum DbType {
  DB_UNSPECIFIED = 0;
  POSTGRESQL = 1;
  MYSQL = 2;
  MARIADB = 3;
  MONGODB = 4;
  REDIS = 5;
  SQLITE = 6;
}

message ConnectionConfig {
  string host = 1;
  uint32 port = 2;
  string database = 3;
  string username = 4;
  string password_secret_ref = 5; // ссылка на зашифрованный секрет
  string container_id = 6;         // для docker exec strategy
  ConnectionStrategy strategy = 7;
}
enum ConnectionStrategy { TCP = 0; DOCKER_EXEC = 1; UNIX_SOCKET = 2; }

message BackupJobSpec {
  string id = 1;
  string target_id = 2;
  string cron = 3;                 // UTC
  uint32 retention_days = 4;
  string compression = 5;          // "zstd" | "gzip" | "none"
  bool encryption_enabled = 6;
  repeated string pre_hooks = 7;
  repeated string post_hooks = 8;
  RetryPolicy retry = 9;
  uint32 timeout_sec = 10;
}

message RetryPolicy {
  uint32 max_attempts = 1;
  repeated uint32 backoff_seconds = 2;  // [60, 300, 1800]
}

message HealthCheckSpec {
  string id = 1;
  string name = 2;
  CheckType type = 3;
  string target = 4;               // URL или host:port
  uint32 interval_sec = 5;
  uint32 timeout_sec = 6;
  uint32 expected_status = 7;      // для HTTP
}
enum CheckType { HTTP = 0; HTTPS = 1; TCP = 2; }

message MaintenanceWindow {
  string start_utc = 1;            // "02:00"
  string end_utc = 2;              // "05:00"
}

message AdvancedSettings {
  uint32 max_parallel_jobs = 1;
  string log_level = 2;
  bool auto_update_enabled = 3;
}
```

### RunBackup

```protobuf
message RunBackup {
  string job_id = 1;
  string run_id = 2;
  bool manual_trigger = 3;
  S3UploadCreds upload_creds = 4;
  bytes encrypted_dek = 5;         // DEK для шифрования, обёрнутый KMS
}
message S3UploadCreds {
  string presigned_put_url = 1;
  uint64 expires_at_ms = 2;
  string final_s3_key = 3;
}
```

### CancelJob, RunHealthCheck

```protobuf
message CancelJob { string job_id = 1; }
message RunHealthCheck { string check_id = 1; }
```

### ~~RunRestore~~ — удалено в MVP

В Phase 1 функция «получить данные обратно» работает через REST API (см. ниже эндпоинты `/runs/:id/download-url` и `/runs/:id/decryption-token`) + локальную CLI-утилиту `backupy-decrypt`. Агент в этом потоке не участвует.

Restore-в-БД через агент (RESTORE_MODE: NEW_DATABASE / ANOTHER_AGENT) — Phase 3. К тому моменту в proto будет добавлен новый message `RunRestore` (proto v2).

### SelfUpdate

```protobuf
message SelfUpdate {
  string target_version = 1;
  string binary_url = 2;
  string sha256 = 3;
  bytes cosign_signature = 4;
  bool force = 5;
}
```

---

## 6. REST API

База: `https://api.backupy.ru/v1`.
Auth: `Authorization: Bearer <token>` (session-token из UI или API-key).

### Auth & Account

| Метод | Путь | Назначение |
|---|---|---|
| POST | `/auth/signup` | email+password регистрация |
| POST | `/auth/login` | email+password |
| POST | `/auth/magic-link/request` | запрос magic link |
| GET | `/auth/magic-link/verify?token=` | подтверждение |
| GET | `/auth/oauth/github` | OAuth redirect |
| GET | `/auth/oauth/github/callback` | OAuth callback |
| GET | `/auth/oauth/google` | OAuth redirect |
| GET | `/auth/oauth/google/callback` | OAuth callback |
| POST | `/auth/logout` | |
| POST | `/auth/password-reset/request` | |
| POST | `/auth/password-reset/confirm` | |
| GET | `/me` | текущий пользователь |
| PATCH | `/me` | обновить профиль |
| DELETE | `/me` | account deletion (start grace period) |
| GET | `/me/export` | data export JSON |
| POST | `/me/2fa/enable` | включить TOTP |
| POST | `/me/2fa/disable` | |

### Agents

| Метод | Путь | Назначение |
|---|---|---|
| GET | `/agents` | список агентов |
| POST | `/agents` | создать (возвращает раз secret key) |
| GET | `/agents/:id` | детали |
| PATCH | `/agents/:id` | изменить имя/настройки |
| DELETE | `/agents/:id` | |
| POST | `/agents/:id/rotate-key` | новый ключ |
| GET | `/agents/:id/config` | текущий config (видимый пользователю) |
| PATCH | `/agents/:id/config` | изменить (создаёт новую version) |
| GET | `/agents/:id/discovered` | то, что обнаружил Docker socket |
| GET | `/agents/:id/metrics` | последние метрики |

### Targets & Jobs

| Метод | Путь | Назначение |
|---|---|---|
| GET | `/agents/:id/targets` | список target'ов |
| POST | `/agents/:id/targets` | создать target |
| PATCH | `/targets/:id` | |
| DELETE | `/targets/:id` | |
| POST | `/targets/:id/test-connection` | проверка коннекта |
| GET | `/agents/:id/jobs` | jobs |
| POST | `/agents/:id/jobs` | новый job |
| GET | `/jobs/:id` | детали |
| PATCH | `/jobs/:id` | редактирование |
| DELETE | `/jobs/:id` | |
| POST | `/jobs/:id/run` | manual run, `Idempotency-Key` поддерживается |

### Backup Runs

| Метод | Путь | Назначение |
|---|---|---|
| GET | `/runs` | каталог с фильтрами `?agent_id=&target_id=&from=&to=&status=` |
| GET | `/runs/:id` | детали |
| GET | `/runs/:id/download-url` | presigned GET URL для скачивания зашифрованного объекта (TTL 15 min) |
| POST | `/runs/:id/decryption-token` | временный JWT с plaintext DEK для CLI-утилиты (TTL 15 min) |
| DELETE | `/runs/:id` | удалить бэкап вручную |

> Restore-в-БД (`POST /runs/:id/restore` + `/restores/:id`) — **Phase 3**, не реализуется в MVP.

### Health Checks

| Метод | Путь | Назначение |
|---|---|---|
| GET | `/health-checks` | |
| POST | `/health-checks` | |
| PATCH | `/health-checks/:id` | |
| DELETE | `/health-checks/:id` | |
| GET | `/health-checks/:id/results` | history с временным фильтром |
| GET | `/health-checks/:id/uptime?period=24h\|7d\|30d\|90d` | aggregated |

### Storage Profiles

| Метод | Путь | Назначение |
|---|---|---|
| GET | `/storage-profiles` | managed + BYO |
| POST | `/storage-profiles` | добавить BYO-S3 |
| PATCH | `/storage-profiles/:id` | |
| DELETE | `/storage-profiles/:id` | |
| POST | `/storage-profiles/:id/test` | тестовое PUT/GET |
| GET | `/storage-profiles/usage` | сколько занято |

### Status Page

| Метод | Путь | Назначение |
|---|---|---|
| GET | `/status-page` | конфиг страницы |
| PATCH | `/status-page` | |
| POST | `/status-page/verify-dns` | проверка CNAME |
| POST | `/status-page/incidents` | создать incident |
| PATCH | `/incidents/:id` | обновить status (Investigating → Resolved) |
| GET | `/incidents` | список |
| GET | `/status-page/subscribers` | |

### Public (без auth)

| Метод | Путь | Назначение |
|---|---|---|
| GET | `/public/status/:subdomain` | публичная страница |
| GET | `/public/status/:subdomain/rss` | RSS feed |
| POST | `/public/status/:subdomain/subscribe` | подписка |
| GET | `/public/status/:subdomain/unsubscribe?token=` | |

### Notifications

| Метод | Путь | Назначение |
|---|---|---|
| GET | `/notification-channels` | |
| POST | `/notification-channels` | email/telegram/webhook |
| PATCH | `/notification-channels/:id` | |
| DELETE | `/notification-channels/:id` | |
| POST | `/notification-channels/:id/test` | отправить тестовое |
| GET | `/notification-preferences` | |
| PATCH | `/notification-preferences` | |

### Audit Log & API Tokens

| Метод | Путь | Назначение |
|---|---|---|
| GET | `/audit-log` | с фильтрами |
| GET | `/audit-log/export` | CSV |
| GET | `/api-tokens` | список (без plaintext) |
| POST | `/api-tokens` | создать |
| DELETE | `/api-tokens/:id` | |

---

## 6a. Admin REST API (`/admin/v1/`)

Базовый путь: `https://api.backupy.ru/admin/v1`.
Auth: тот же Bearer token, но проверяется `role=admin`. Не-admin запрос → `403 PERMISSION_DENIED`.

### Companies (admin only)

| Метод | Путь | Назначение | Phase |
|---|---|---|---|
| GET | `/companies` | список с фильтрами `?plan=&status=&country=&search=` | 1 |
| POST | `/companies` | создать вручную (для enterprise-онбординга) | 1 |
| GET | `/companies/:id` | детальная карточка (users, agents count, payment history placeholder, internal notes) | 1 |
| PATCH | `/companies/:id` | обновить (имя, контактный email, страна, plan, status) | 1 |
| POST | `/companies/:id/suspend` | приостановить — все агенты компании перестают получать команды | 1 |
| POST | `/companies/:id/reactivate` | вернуть в active | 1 |
| POST | `/companies/:id/impersonate` | создать временную user-session под owner-юзером компании | 1 |
| GET | `/companies/:id/notes` | internal notes (admin-only) | 1 |
| POST | `/companies/:id/notes` | добавить заметку | 1 |
| POST | `/companies/:id/change-plan` | сменить план (без биллинга — биллинг Phase 3) | 2 |

### Users (admin only)

| Метод | Путь | Назначение | Phase |
|---|---|---|---|
| GET | `/users` | все юзеры с фильтрами `?company_id=&role=&status=&search=` | 1 |
| POST | `/users` | создать юзера в конкретной компании (или с auto-создаваемой) | 1 |
| GET | `/users/:id` | детали юзера | 1 |
| POST | `/users/:id/suspend` | заблокировать sign-in и API | 1 |
| POST | `/users/:id/reactivate` | | 1 |
| POST | `/users/:id/impersonate` | временная user-session | 1 |
| POST | `/users/:id/reset-password` | отправить reset-email | 1 |
| DELETE | `/users/:id` | начать GDPR-удаление (полный flow — Phase 3) | 1 (start) / 3 (full) |

### Plans (admin only)

| Метод | Путь | Назначение | Phase |
|---|---|---|---|
| GET | `/plans` | все тарифы (active + inactive) | 1 |
| POST | `/plans` | создать новый | 2 |
| PATCH | `/plans/:id` | обновить (цена, лимиты, фичи, active) | 2 |
| DELETE | `/plans/:id` | удалить (если customers=0; иначе ошибка) | 2 |
| GET | `/plans/distribution` | сколько customers на каждом плане + MRR | 2 |

### Infrastructure (admin only, read-only)

| Метод | Путь | Назначение | Phase |
|---|---|---|---|
| GET | `/infra/s3` | список наших S3 bucket'ов: размер, объекты, стоимость | 1 |
| GET | `/infra/kms` | KMS keys active, operations today, cost | 1 |
| GET | `/infra/cluster` | K8s cluster CPU/memory | 1 |
| GET | `/infra/db` | PostgreSQL size, replication lag, connections | 1 |
| GET | `/infra/redis` | Redis memory usage | 1 |
| GET | `/infra/cost-breakdown` | детализация месячных расходов + gross margin расчёт | 2 |

### Admin audit log

| Метод | Путь | Назначение | Phase |
|---|---|---|---|
| GET | `/admin-audit` | список admin-actions с фильтрами | 1 |
| GET | `/admin-audit/export` | CSV | 2 |

### System settings (admin only)

| Метод | Путь | Назначение | Phase |
|---|---|---|---|
| GET | `/system-settings` | branding, signup config, feature flags | 2 |
| PATCH | `/system-settings` | обновить | 2 |
| GET | `/system-settings/feature-flags` | список фич с per-company overrides | 2 |
| POST | `/system-settings/feature-flags/:flag/enable` | включить глобально | 2 |
| POST | `/system-settings/feature-flags/:flag/disable` | выключить глобально | 2 |

### Admin team

| Метод | Путь | Назначение | Phase |
|---|---|---|---|
| GET | `/admin-team` | список админов с ролями | 2 |
| POST | `/admin-team/invite` | пригласить нового админа | 2 |
| PATCH | `/admin-team/:id` | сменить роль (super_admin / support / billing) | 2 |
| DELETE | `/admin-team/:id` | удалить админа | 2 |

### Payments & Revenue (Phase 3, заглушки в Phase 2)

| Метод | Путь | Назначение | Phase |
|---|---|---|---|
| GET | `/payments` | все транзакции с фильтрами | 3 |
| POST | `/payments/:id/refund` | refund-action | 3 |
| POST | `/payments/:id/retry` | повторить failed charge | 3 |
| GET | `/analytics/revenue` | MRR, ARR, churn, conversion, cohorts | 3 |

---

## 7. Модель ошибок

```json
{
  "error": {
    "code": "RESOURCE_NOT_FOUND",
    "message": "Agent not found",
    "details": { "agent_id": "agt_..." },
    "trace_id": "01HM..."
  }
}
```

Коды:
- `AUTH_FAILED`
- `PERMISSION_DENIED`
- `RESOURCE_NOT_FOUND`
- `VALIDATION_ERROR`
- `RATE_LIMITED`
- `CONFLICT`
- `PRECONDITION_FAILED`
- `INTERNAL`
- `SERVICE_UNAVAILABLE`

## 8. Идемпотентность

- `POST /jobs/:id/run` и `POST /runs/:id/restore` принимают `Idempotency-Key` header.
- Сервер кеширует ответ по ключу на 24 часа.
- `BackupCompleted` от агента дедуплицируется по `run_id` на сервере (upsert).

## 9. Pagination

- Query params: `?limit=50&cursor=<opaque>`.
- Response:
```json
{
  "items": [...],
  "next_cursor": "...",
  "has_more": true
}
```
- Limit max: 100.

## 10. Real-time (для UI)

- SSE endpoint: `GET /events?topics=jobs,agents,restores` (auth required).
- События:
  - `agent.connected`, `agent.disconnected`, `agent.config.applied`
  - `job.started`, `job.progress`, `job.completed`, `job.failed`
  - `restore.started`, `restore.progress`, `restore.completed`, `restore.failed`
  - `incident.opened`, `incident.resolved`

## 11. Версионирование

- API: `/v1/...`. Breaking changes → `/v2/`.
- Proto: семантическое версионирование пакета (`backup.v1`, `backup.v2`).
- Агент шлёт `agent_version`, сервер договаривается о минимальной поддерживаемой версии.
- Минимум 6 месяцев backward compatibility между proto-версиями.

## 12. Rate limits (defaults)

| Endpoint | Limit |
|---|---|
| `/auth/login` | 5/min/IP |
| `/auth/signup` | 3/hour/IP |
| `/auth/magic-link/request` | 3/hour/email |
| `/auth/password-reset/request` | 3/hour/email |
| API (auth required) | 60/min/token |
| `/public/status/*` | 60/min/IP |

Превышение → 429 с `Retry-After`.
