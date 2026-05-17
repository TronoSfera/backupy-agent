module github.com/backupy/backupy/apps/backupy-decrypt

go 1.22

require (
	github.com/klauspost/compress v1.17.9
	github.com/stretchr/testify v1.9.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	google.golang.org/protobuf v1.34.2 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// Integration tests (build tag: integration) cross-link to the agent's
// pipeline package to prove the encrypt/decrypt formats match byte-for-byte.
// The replace directives point at local sibling modules; the requires
// use the zero-version pseudo-tag that go-mod accepts for replace targets.
// The proto/gen replace must be declared here too because Go's module
// resolution does not transitively honour the agent module's own replace.
require (
	github.com/backupy/backupy/apps/agent v0.0.0-00010101000000-000000000000
	github.com/backupy/backupy/packages/proto/gen/go/backupv1 v0.0.0-00010101000000-000000000000 // indirect
)

replace (
	github.com/backupy/backupy/apps/agent => ../agent
	github.com/backupy/backupy/packages/proto/gen/go/backupv1 => ../../packages/proto/gen/go/backupv1
)
