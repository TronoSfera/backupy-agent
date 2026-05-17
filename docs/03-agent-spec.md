# 03. Agent — спецификация

## Назначение

Open-source Docker-сервис (MIT/Apache), который пользователь добавляет в свой `docker-compose.yml` рядом с приложением. Получает команды от сервера, делает бэкапы БД, шлёт в S3.

## Запуск

```yaml
services:
  backup-agent:
    image: backupservice/agent:latest
    environment:
      BACKUP_SERVER_URL: https://backupy.ru
      BACKUP_AGENT_KEY: ${BACKUP_KEY}
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
      - backup-agent-state:/var/lib/backup-agent
    restart: unless-stopped

volumes:
  backup-agent-state:
```

### Env-переменные (bootstrap)

| Имя | Назначение | Required |
|---|---|---|
| `BACKUP_SERVER_URL` | Адрес control plane | да |
| `BACKUP_AGENT_KEY` | Ключ агента (секрет) | да |
| `BACKUP_LOG_LEVEL` | trace/debug/info/warn/error, default info | нет |
| `BACKUP_STATE_DIR` | Путь к state, default `/var/lib/backup-agent` | нет |

Всё остальное (targets, schedules, S3 creds, retention, hooks) — приходит с сервера через `ConfigUpdate`.

## Возможности

### Auto-discovery БД через Docker socket
- Сканирует контейнеры по docker.sock (read-only).
- Распознаёт `postgres`, `mysql`, `mariadb`, `mongo`, `redis` по образам и `EXPOSE` портам.
- SQLite — обнаруживает файлы `.db`/`.sqlite` в смонтированных volume'ах подключённых контейнеров.
- Парсит env-переменные обнаруженных контейнеров (`POSTGRES_USER`, `POSTGRES_PASSWORD`, `MYSQL_ROOT_PASSWORD`, etc.) — для предзаполнения форм в UI.
- Не передаёт plaintext-пароли на сервер. Отправляет только hint-список и имена переменных.
- Перепроверка discovery: раз в час + по событию docker events.

### Persistent state в volume
- SQLite или BoltDB в `/var/lib/backup-agent/state.db`.
- Хранит: текущий config, очередь jobs, локальные логи, последний known config_version.
- Шифрование state опционально (key derived из BACKUP_AGENT_KEY).

### WSS-канал
- Один long-lived connection на agent_id.
- Heartbeat каждые 30 сек.
- Reconnect: exponential backoff 1s → 2s → 4s → … → 60s (jitter ±20%).
- На разрыве — jobs не теряются, доделываются после reconnect (idempotent через run_id).

### Backup pipeline
1. `pre_hooks` (опционально) — shell-команды до начала.
2. Dump БД через bundled tool.
3. Stream → zstd compression.
4. Stream → AES-256-GCM шифрование с DEK, полученным от сервера.
5. Stream → S3 multipart upload по presigned URL.
6. Smoke-validation: попытаться открыть только что загруженный файл, прочитать первые байты, валидировать заголовок (для pg_dump custom format — magic bytes `PGDMP`).
7. Расчёт SHA-256 целого файла (стримово, на лету).
8. `post_hooks` (опционально).
9. Отправка `BackupCompleted` со всей метой.

### Поддерживаемые драйверы БД

| Driver | Tool в образе | Стратегия | Phase |
|---|---|---|---|
| PostgreSQL | pg_dump (custom format) | Logical dump через TCP / unix socket | 1 |
| MySQL/MariaDB | mysqldump | Logical dump, `--single-transaction` | 1 |
| MongoDB | mongodump | Logical dump (archive format) | 2 |
| Redis | BGSAVE + copy RDB | Через `docker exec` или TCP `BGSAVE` + чтение dump.rdb | 2 |
| SQLite | `VACUUM INTO` или копия с rsync | File-based | 2 |

### Health checks (Phase 2)
- HTTP/HTTPS: GET URL, проверка status code и опционально body match.
- TCP: connect-then-close на host:port.
- Custom interval per check (default 60s).
- Timeout per check (default 10s).
- Результат сразу шлётся серверу.

### Pre/Post hooks (Phase 2, opt-in)
- Shell-команды, выполняемые в контексте контейнера агента.
- Опции: `docker exec <container> <cmd>` или прямой shell.
- Timeout per hook (default 30s).
- В UI явный warning: «Это выполнит код на вашем хосте. Используйте только команды, которым доверяете».

### Auto-update (Phase 2)
- Сервер шлёт `SelfUpdate{target_version, binary_url, sha256, cosign_signature}`.
- Агент валидирует cosign-подпись (публичный ключ зашит в бинарь).
- Скачивает в `/var/lib/backup-agent/bin/<version>`.
- Graceful binary swap: запускает новый бинарь, передаёт открытые сокеты через fd-passing, новый агент шлёт `HealthyAfterUpdate`, старый exit'ит.
- Откат: если новый не пришёл healthy за 60 сек — старый продолжает работать, шлёт alert.
- Opt-out через config (`auto_update_enabled=false`).

## Требования к образу

| Параметр | Значение |
|---|---|
| Базовый образ | distroless или alpine |
| Размер | < 50 MB |
| Архитектуры | linux/amd64, linux/arm64 |
| Pinned binaries | pg_dump, mysqldump, mongodump, redis-cli — версии явные |
| User | non-root (uid 1000) |
| Healthcheck | `HEALTHCHECK CMD agent health-check` |
| Tags | `1.x.y`, `1.x`, `1`, `latest` |
| Подпись | cosign sign-blob |

## Безопасность агента

- TLS 1.3 ко всем endpoint'ам.
- Pinning публичного ключа сервера (зашит в бинарь).
- Docker socket монтируется read-only.
- `BACKUP_AGENT_KEY` никогда не пишется в логи.
- Локальный state шифруется (опционально включается).
- Healthcheck endpoint (если будет) — только на localhost.
- Capabilities контейнера: drop ALL.

## Метрики (Prometheus, localhost)

```
backup_agent_up
backup_agent_config_version
backup_agent_jobs_total{status="success|failed"}
backup_agent_backup_size_bytes
backup_agent_backup_duration_seconds
backup_agent_queue_depth
backup_agent_reconnects_total
backup_agent_health_check_duration_seconds{check_id="..."}
```

## CLI агента (внутренние команды, не для пользователя)

```bash
agent run                # default, запускает service loop
agent version            # вывести версию
agent health-check       # для Docker HEALTHCHECK
agent dump-state         # debug: вывести state в stdout
agent self-update <ver>  # ручной триггер обновления
```
