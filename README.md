# Backupy Agent

Open-source backup agent for the [Backupy](https://backupy.tronosfera.ru) backup-as-a-service platform.

- Auto-discovers databases inside your Docker stack (PostgreSQL, MySQL, MongoDB, Redis, SQLite)
- Streams dumps to your cloud bucket, encrypted client-side with AES-256-GCM
- Keeps a persistent local queue so a brief network blip can't lose a run
- Talks to the cloud over WebSocket; no inbound ports on your host
- Apache-2.0 licensed; runs on the source code in this repo, end to end

## Quick start

1. Sign up at https://backupy.tronosfera.ru
2. Create an agent in **Dashboard → Agents → Add agent**. Copy the one-time key.
3. Add the snippet below to your `docker-compose.yml` (alongside the database you want to back up):

```yaml
services:
  backupy-agent:
    image: ghcr.io/tronosfera/backupy-agent:0.1.0
    restart: unless-stopped
    environment:
      BACKUPY_SERVER_URL: wss://backupy.tronosfera.ru/agents/connect
      BACKUPY_AGENT_KEY: ${BACKUPY_AGENT_KEY}
    volumes:
      # Read-only socket for Docker discovery — required if you want
      # auto-detection of running containers (recommended).
      - /var/run/docker.sock:/var/run/docker.sock:ro
      # Persistent state (BoltDB queue + last-seen offsets).
      - backupy_agent:/var/lib/backupy

volumes:
  backupy_agent:
```

Put the key in your `.env`:

```
BACKUPY_AGENT_KEY=bk_agent_xxxxxxxxxxxxxxxxxxxxxxxx
```

```
docker compose up -d backupy-agent
```

The agent connects, registers, and shows up in your dashboard. Configure the first backup job from there.

## Build from source

```
make proto      # regenerate Go bindings from packages/proto/
make agent      # builds the binary at apps/agent/bin/backupy-agent
make agent-image # builds the Docker image as backupy-agent:dev
```

## What's in this repo

| Path | What |
|---|---|
| `apps/agent/` | The Go agent itself (cmd + internal). Multi-arch Docker image is published to `ghcr.io/tronosfera/backupy-agent`. |
| `apps/backupy-decrypt/` | Standalone CLI to decrypt a downloaded backup locally. You never need to upload the decryption key — it's handed to you in a one-time JWT signed by the server. |
| `packages/proto/` | Protobuf wire format between agent and server. The generated Go files (`.pb.go`) are committed so the repo builds clean without `protoc`. |
| `docs/` | Subset of the architectural docs that apply to the agent + the wire protocol. |

## Releasing

Push a tag matching `v*` to trigger the GHCR release workflow (`.github/workflows/release.yml`). It builds multi-arch (`linux/amd64` + `linux/arm64`) and publishes:

- `ghcr.io/tronosfera/backupy-agent:vX.Y.Z`
- `ghcr.io/tronosfera/backupy-agent:vX.Y`
- `ghcr.io/tronosfera/backupy-agent:latest` (only for non-pre-release tags)

## Security

The agent has read-only access to the Docker socket (when mounted) and SHELL exec rights inside its own container for `mongodump`, `pg_dump`, etc. It never reaches outside your host except to:

- `wss://backupy.tronosfera.ru/agents/connect` — control channel
- Presigned S3 PUT URLs returned by the server — to upload encrypted dump chunks

If you set `BACKUPY_DISABLE_DISCOVERY=true`, the agent ignores the Docker socket and operates purely on explicit job configuration.

## License

Apache-2.0. See `LICENSE`.
