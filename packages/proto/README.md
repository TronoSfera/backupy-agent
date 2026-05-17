# `packages/proto`

Canonical Protobuf wire format for Backupy.

Both the Go agent (`apps/agent`) and the Go server (`apps/server`) consume the
generated bindings produced from these `.proto` files. The contract itself is
described in [`docs/07-api-contract.md`](../../docs/07-api-contract.md) — this
package is the executable form of that document.

## Layout

```
packages/proto/
├── buf.yaml              # module config: lint (STANDARD), breaking (FILE)
├── buf.gen.yaml          # codegen plugin config (protoc-gen-go)
├── v1/
│   ├── common.proto          # shared enums + messages
│   ├── envelope.proto        # Envelope wrapper (oneof of all payloads)
│   ├── agent_to_server.proto # Register, Heartbeat, JobUpdate, ...
│   └── server_to_agent.proto # RegisterAck, ConfigUpdate, RunBackup, ...
└── gen/
    └── go/v1/                # generated Go bindings (committed)
```

Package name: `backup.v1`.
Go import path: `github.com/backupy/backupy/packages/proto/gen/go/v1`
(`backupv1` short name).

## Prerequisites

Install [buf](https://buf.build/docs/installation) and the Go plugin once
per workstation:

```sh
# from the repo root
make tools
```

That installs `buf` and `protoc-gen-go` into `$GOBIN` (or `$GOPATH/bin`).

## Regenerating code

From the repository root:

```sh
make gen
```

This `cd`s into `packages/proto` and runs `buf generate`. The output is
written to `packages/proto/gen/go/v1/`.

Alternatively, from this directory:

```sh
buf generate
```

## Linting and breaking-change detection

```sh
# from the repo root
make lint                 # includes `buf lint`

# or directly
cd packages/proto
buf lint
buf breaking --against '.git#branch=main,subdir=packages/proto'
```

`buf.yaml` enables the `STANDARD` lint set and `FILE`-level breaking-change
detection. Field renames or tag-number changes in any committed `.proto`
will fail CI.

## Adding a new message

1. Pick the right file (`agent_to_server.proto` vs `server_to_agent.proto`),
   or add a new shared type to `common.proto`.
2. Use a fresh field number — never reuse a `reserved` tag.
3. If it is a new top-level message that flows over WSS, also add it to the
   `Envelope.payload` `oneof` in `envelope.proto`, picking a stable tag in
   the matching direction's range (agent→server: 10..49, server→agent:
   50..99).
4. Run `make gen` and commit both the `.proto` change and the regenerated
   Go files in the same commit.
5. Run `buf breaking` locally before pushing.

## Versioning

The package is `backup.v1`. Any breaking change requires a new package
(`backup.v2`) plus at least six months of dual-support — see
[`docs/07-api-contract.md` §11](../../docs/07-api-contract.md).
