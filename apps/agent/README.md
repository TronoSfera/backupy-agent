# backupy-agent

The Backupy agent — open-source (MIT) Docker service that runs alongside
your application and ships encrypted database backups to S3.

> Full spec: [`docs/03-agent-spec.md`](../../docs/03-agent-spec.md).
> Wire protocol: [`docs/07-api-contract.md`](../../docs/07-api-contract.md).

## Current status

This directory is the **D-01 + D-03 (partial) skeleton**. What works today:

- Compiles to a tiny static binary (`make build`).
- Loads & validates bootstrap config from env.
- Opens an encrypted-at-rest BoltDB state file (AES-256-GCM, HKDF from key).
- Cobra-based CLI: `run`, `version`, `health-check`, `dump-state`.
- Distroless multi-arch Dockerfile (< 50 MB image, non-root).

What's stubbed:

- WSS protocol exchange — skeleton only (full impl: **D-02**).
- Register / Heartbeat — interfaces in place (**D-04**).
- Docker socket discovery — interface only (**D-05**).
- Backup drivers (pg_dump, mysqldump, …) — interfaces only (**D-06+**).
- Auto-update — not started (**D-15**).

## Quickstart (Docker)

```yaml
services:
  backup-agent:
    image: backupy/agent:1
    environment:
      BACKUP_SERVER_URL: https://api.backupy.ru
      BACKUP_AGENT_KEY: ${BACKUP_KEY}
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
      - backup-agent-state:/var/lib/backup-agent
    restart: unless-stopped

volumes:
  backup-agent-state:
```

## Env vars

| Var | Required | Default | Notes |
|---|---|---|---|
| `BACKUP_SERVER_URL` | yes | `https://api.backupy.ru` | Must be `https://` (override with `BACKUP_DEV_ALLOW_INSECURE=true` for local dev). |
| `BACKUP_AGENT_KEY` | yes | — | Format `bkpy_(live\|test)_<32 alnum>`. Never logged. |
| `BACKUP_STATE_DIR` | no | `/var/lib/backup-agent` | Volume-mounted. Must be writable by uid 65532 (distroless `nonroot`). |
| `BACKUP_LOG_LEVEL` | no | `info` | `trace`/`debug`/`info`/`warn`/`error`. |
| `BACKUP_DOCKER_SOCKET` | no | `/var/run/docker.sock` | Mounted read-only. |
| `BACKUP_DEV_ALLOW_INSECURE` | no | `false` | Allows `http://` server URL — dev only. |

Everything else (targets, schedules, retention, S3 creds, hooks) comes from
the server via `ConfigUpdate` after registration.

## CLI

```text
agent run            # default; starts the service loop
agent version        # print version / commit / build date  (--json for machine output)
agent health-check   # used by Docker HEALTHCHECK; exits 0 when healthy
agent dump-state     # debug: pretty-print state.db as JSON
                     # add --allow-secrets to include decrypted payloads
```

## Filesystem layout

```
/var/lib/backup-agent/
└── state.db          # BoltDB, AES-256-GCM encrypted values (HKDF from agent key)
```

Buckets inside `state.db`:

| Bucket | Contents |
|---|---|
| `config` | last applied `AgentConfig` + version |
| `queue` | pending `RunBackup` envelopes keyed by `run_id` |
| `registry` | `session_id`, last server time, last heartbeat |
| `logs_buffer` | rate-limited `LogEvent` buffer when server unreachable |

## Build

```sh
# Generate protobuf bindings first (from repo root).
make proto

# Local binary
cd apps/agent
make tidy
make build      # → bin/agent

# Local docker image (uses repo root as build context)
make image

# Multi-arch image
make image-multiarch
```

> **Important:** `make proto` from the repo root must run at least once
> before `go build` succeeds. The agent's `go.mod` uses a `replace`
> directive pointing at `packages/proto/gen/go/v1`, which is created by
> `buf generate` — see [`packages/proto/README.md`](../../packages/proto/README.md).

## Tests

```sh
make test       # go test -race -cover ./...
```

Test packages that pass standalone (no proto codegen needed):

- `internal/config` — env validation, agent-key regex, state-dir probe.
- `internal/state` — BoltDB roundtrip, AES-GCM encryption-at-rest check.
- `internal/logging` — level parsing, secret redaction.
- `internal/wss` — exponential backoff schedule + jitter band.

Packages that require generated proto code to compile:

- `internal/proto`
- `internal/wss/client.go`
- `cmd/agent/run.go`

## Security notes

- TLS 1.3 to all server endpoints (enforced by `coder/websocket` defaults).
- `BACKUP_AGENT_KEY` is never logged (`slog` ReplaceAttr redacts known keys
  defensively; the value is also `json:"-"` in `Config`).
- State at rest is AES-256-GCM keyed by HKDF-SHA256 of the agent key.
- Docker socket is mounted read-only.
- Container runs as uid 65532 (distroless `nonroot`).
- Image is distroless static — no shell, no package manager, no busybox.

## License

MIT — see top-level `LICENSE` once added.
