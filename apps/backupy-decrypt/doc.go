// Package backupydecrypt is the root of the offline backup-decryption CLI.
//
// The compiled binary lives under cmd/backupy-decrypt; the reusable
// streaming decrypt logic is in internal/decrypt. This file exists so
// that integration tests at the module root (e.g. integration_test.go,
// guarded by the `integration` build tag) have a real package to attach
// to — Go requires at least one non-test file per directory.
package backupydecrypt
