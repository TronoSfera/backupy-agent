# syntax=docker/dockerfile:1.7
#
# Backupy agent image — multi-stage, multi-arch (linux/amd64, linux/arm64).
# Bundles client binaries the agent shells out to during dump:
#   pg_dump, mysqldump, mongodump, redis-cli, sqlite3.
#
# Built from the REPO ROOT so the protobuf generated code in
# packages/proto/gen/go/backupv1 is in build context (referenced by the
# `replace` directive in apps/agent/go.mod).
#
# Example local build (multi-arch):
#   docker buildx build \
#     --platform linux/amd64,linux/arm64 \
#     --build-arg VERSION=$(git describe --tags --always) \
#     --build-arg COMMIT=$(git rev-parse --short HEAD) \
#     --build-arg BUILD_DATE=$(date -u +%Y-%m-%dT%H:%M:%SZ) \
#     -f apps/agent/Dockerfile \
#     -t ghcr.io/tronosfera/backupy-agent:dev .

# -----------------------------------------------------------------------------
# Stage 1 — Go build
# -----------------------------------------------------------------------------
FROM --platform=$BUILDPLATFORM golang:1.23-alpine AS builder
RUN apk add --no-cache git

WORKDIR /src

# Module files first so dep download is cached independently of source.
COPY apps/agent/go.mod apps/agent/go.sum* ./apps/agent/
COPY packages/proto/gen/go/backupv1 ./packages/proto/gen/go/backupv1

RUN --mount=type=cache,target=/go/pkg/mod \
    cd apps/agent && go mod download

# Source last for better cache hit rate.
COPY apps/agent ./apps/agent

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG COMMIT=none
ARG BUILD_DATE=unknown

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    cd apps/agent && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build \
      -trimpath \
      -ldflags "-s -w \
        -X github.com/backupy/backupy/apps/agent/internal/version.Version=${VERSION} \
        -X github.com/backupy/backupy/apps/agent/internal/version.Commit=${COMMIT} \
        -X github.com/backupy/backupy/apps/agent/internal/version.BuildDate=${BUILD_DATE}" \
      -o /out/agent ./cmd/agent

# -----------------------------------------------------------------------------
# Stage 2 — Source the database client binaries from Alpine packages.
# Doing this in a separate stage keeps the apk cache out of the final image.
# -----------------------------------------------------------------------------
FROM alpine:3.20 AS dbclients
RUN apk add --no-cache \
      postgresql16-client \
      mariadb-client \
      mongodb-tools \
      redis \
      sqlite

# -----------------------------------------------------------------------------
# Stage 3 — runtime (alpine, non-root uid 1000)
# -----------------------------------------------------------------------------
FROM alpine:3.20 AS runtime

RUN apk add --no-cache \
      ca-certificates \
      tzdata \
      libpq \
      mariadb-connector-c \
      openssl \
      sqlite-libs \
    && addgroup -S backupy -g 1000 \
    && adduser  -S backupy -G backupy -u 1000

ARG VERSION=dev
ARG COMMIT=none
LABEL org.opencontainers.image.title="backupy-agent" \
      org.opencontainers.image.description="Open-source backup agent for backupy.tronosfera.ru" \
      org.opencontainers.image.source="https://github.com/TronoSfera/backupy-agent" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${COMMIT}"

COPY --from=builder    /out/agent              /usr/local/bin/agent
COPY --from=dbclients  /usr/bin/pg_dump        /usr/bin/pg_dump
COPY --from=dbclients  /usr/bin/mysqldump      /usr/bin/mysqldump
COPY --from=dbclients  /usr/bin/mongodump      /usr/bin/mongodump
COPY --from=dbclients  /usr/bin/redis-cli      /usr/bin/redis-cli
COPY --from=dbclients  /usr/bin/sqlite3        /usr/bin/sqlite3

USER backupy:backupy

VOLUME ["/var/lib/backupy"]

ENTRYPOINT ["/usr/local/bin/agent"]
CMD ["run"]
