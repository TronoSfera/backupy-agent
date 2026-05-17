# backupy-decrypt — Makefile
#
# Building:
#   make build         # builds for the host OS/arch into ./bin/
#   make release       # multi-platform via goreleaser (requires goreleaser)
#   make test          # go test -race
#   make tidy          # go mod tidy
#   make install       # go install into $GOBIN
#
# Versioning: the binary's --version output is overridden via -ldflags.

BINARY := backupy-decrypt
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)
GO ?= go

.PHONY: all build test tidy release install clean

all: build

build:
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o bin/$(BINARY) ./cmd/backupy-decrypt

test:
	$(GO) test -race -count=1 ./...

tidy:
	$(GO) mod tidy

install:
	CGO_ENABLED=0 $(GO) install -trimpath -ldflags '$(LDFLAGS)' ./cmd/backupy-decrypt

release:
	goreleaser release --clean --snapshot

clean:
	rm -rf bin dist
