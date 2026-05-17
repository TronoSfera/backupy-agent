// Package pipeline executes the agent's backup pipeline end-to-end.
//
// Stages (see docs/03-agent-spec.md → "Backup pipeline"):
//
//  1. Driver.Validate         — smoke-test connectivity, fail fast.
//  2. Driver.Dump             — produce plaintext bytes.
//  3. zstd compression        — streaming.
//  4. AES-256-GCM encryption  — streaming, framed chunks.
//  5. SHA-256 over the ciphertext (the blob actually uploaded to S3).
//  6. HTTP PUT via presigned URL.
//  7. Build BackupCompleted with all metadata.
//
// All interfaces live in this file. Concrete drivers (pg_dump, mysqldump)
// and the streaming primitives (compress, encrypt, upload, runner) live
// in sibling files.
package pipeline

import (
	"context"
	"io"

	backupv1 "github.com/backupy/backupy/packages/proto/gen/go/backupv1"
)

// Driver produces a database dump as a stream of bytes and reports the
// engine version string surfaced in BackupCompleted.db_engine_version.
type Driver interface {
	// Name identifies the driver in logs and metrics ("pg_dump", "mysqldump").
	Name() string

	// Validate performs a fast smoke test: can we even reach the
	// database with the supplied credentials? Implementations should
	// NOT execute a full dump here — a `SELECT 1` (or equivalent) is
	// enough. Returns nil on success.
	Validate(ctx context.Context, target *backupv1.Target) error

	// Dump streams a logical-or-physical backup to out. It MUST honour
	// ctx cancellation promptly so CancelJob is responsive.
	Dump(ctx context.Context, target *backupv1.Target, out io.Writer) (DumpInfo, error)
}

// DumpInfo carries metadata produced during the dump stage.
type DumpInfo struct {
	// EngineVersion is the human-readable backend version string, e.g.
	// "PostgreSQL 16.2" or "MySQL 8.0.36". Used verbatim for the
	// BackupCompleted.db_engine_version field.
	EngineVersion string
}
