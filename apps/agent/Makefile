# Backupy agent — local developer targets.
#
# Quick start:
#   make tidy                # populate go.sum (requires `make proto` from repo root first)
#   make test                # unit tests with -race
#   make build               # local arch binary into bin/agent
#   make image               # docker build (local arch)
#
# All targets are phony — there are no on-disk artefacts to track besides
# bin/agent which is rebuilt on every `make build`.

VERSION    ?= dev
COMMIT     ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
BUILD_DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

PKG_PREFIX := github.com/backupy/backupy/apps/agent/internal/version
LDFLAGS := -s -w \
  -X $(PKG_PREFIX).Version=$(VERSION) \
  -X $(PKG_PREFIX).Commit=$(COMMIT) \
  -X $(PKG_PREFIX).BuildDate=$(BUILD_DATE)

GO       ?= go
PKGS     := ./...
BIN_DIR  := bin
IMAGE    ?= backupy/agent
IMG_TAG  ?= dev

# Repo root for docker buildx (build context includes packages/proto).
REPO_ROOT := $(shell git rev-parse --show-toplevel 2>/dev/null || echo ../..)

.PHONY: tidy build build-linux-amd64 build-linux-arm64 test vet lint fmt image image-multiarch clean

tidy:
	$(GO) mod tidy

build:
	mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/agent ./cmd/agent

build-linux-amd64:
	mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
	  $(GO) build -trimpath -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/agent-linux-amd64 ./cmd/agent

build-linux-arm64:
	mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 \
	  $(GO) build -trimpath -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/agent-linux-arm64 ./cmd/agent

test:
	$(GO) test -race -cover $(PKGS)

vet:
	$(GO) vet $(PKGS)

# Requires golangci-lint installed locally; CI installs it separately.
lint:
	golangci-lint run $(PKGS)

fmt:
	$(GO) fmt $(PKGS)

# Single-arch local image build. Run from anywhere; uses repo root context.
image:
	docker build \
	  --build-arg VERSION=$(VERSION) \
	  --build-arg COMMIT=$(COMMIT) \
	  --build-arg BUILD_DATE=$(BUILD_DATE) \
	  -f $(REPO_ROOT)/apps/agent/Dockerfile \
	  -t $(IMAGE):$(IMG_TAG) \
	  $(REPO_ROOT)

# Multi-arch via buildx. Requires `docker buildx create --use` once.
image-multiarch:
	docker buildx build \
	  --platform linux/amd64,linux/arm64 \
	  --build-arg VERSION=$(VERSION) \
	  --build-arg COMMIT=$(COMMIT) \
	  --build-arg BUILD_DATE=$(BUILD_DATE) \
	  -f $(REPO_ROOT)/apps/agent/Dockerfile \
	  -t $(IMAGE):$(IMG_TAG) \
	  $(REPO_ROOT)

clean:
	rm -rf $(BIN_DIR)
