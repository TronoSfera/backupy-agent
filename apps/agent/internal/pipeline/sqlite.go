// B14: SQLite driver.
//
// Streams a consistent snapshot of a SQLite database file using
// `sqlite3 <path> ".backup '/dev/stdout'"`. The `.backup` command takes
// an online-safe copy of the database — readers continue to see the
// previous state while we drain. The resulting bytes are a complete
// SQLite database file (not SQL text), which we pipe through gzip so
// the stream is self-describing and (modestly) smaller before the
// pipeline's zstd stage layers on top.
//
// Configuration:
//
//   - target.Connection.Database — REQUIRED. Absolute path to the .db
//     file. (Host/Port/Username are ignored.)
//
// The driver validates that the file exists and is readable at
// construction-time. WAL-mode databases are safe to back up with the
// `.backup` command — SQLite quiesces concurrent writers internally.
package pipeline

import (
	"compress/gzip"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	backupv1 "github.com/backupy/backupy/packages/proto/gen/go/backupv1"
)

// sqliteDriver implements Driver against the `sqlite3` CLI binary plus
// optional pure-Go metadata probing.
type sqliteDriver struct {
	binary string
	runner cmdRunner
	// statFn is overridable for tests.
	statFn func(path string) (os.FileInfo, error)
	// metaFn is overridable for tests. Returns (user_version, table_count, err).
	metaFn func(ctx context.Context, path string) (int64, int, error)
}

// NewSqliteDriver constructs the default driver wired to the bundled
// sqlite3 binary on $PATH.
func NewSqliteDriver() Driver {
	return &sqliteDriver{
		binary: "sqlite3",
		runner: streamingRunner{},
		statFn: os.Stat,
		metaFn: probeSqliteMetadata,
	}
}

// Name implements Driver.Name.
func (s *sqliteDriver) Name() string { return "sqlite" }

// Validate verifies sqlite3 is installed and the configured database
// file exists and is readable.
func (s *sqliteDriver) Validate(ctx context.Context, target *backupv1.Target) error {
	if target == nil || target.Connection == nil {
		return errors.New("pipeline: sqlite: nil target/connection")
	}
	path := target.Connection.Database
	if path == "" {
		return errors.New("pipeline: sqlite: target.connection.database must be the path to the .db file")
	}
	versionOut, err := s.runner.Output(ctx, s.binary, []string{"--version"}, nil)
	if err != nil {
		return fmt.Errorf("pipeline: sqlite3 version probe failed (is sqlite3 installed?): %w", err)
	}
	if !looksLikeSqliteVersion(string(versionOut)) {
		return fmt.Errorf("pipeline: unexpected sqlite3 --version output: %q", string(versionOut))
	}
	if _, err := s.statFn(path); err != nil {
		return fmt.Errorf("pipeline: sqlite: cannot stat database %q: %w", path, err)
	}
	return nil
}

// Dump streams a gzip-wrapped binary SQLite backup to out.
//
// We invoke sqlite3 with `.backup '/dev/stdout'`. This is the canonical
// way to take an online-safe snapshot — concurrent readers see the old
// state, concurrent writers are not blocked, and the resulting file is
// a fully self-contained .db that `sqlite3 restored.db` can open.
func (s *sqliteDriver) Dump(ctx context.Context, target *backupv1.Target, out io.Writer) (DumpInfo, error) {
	if target == nil || target.Connection == nil {
		return DumpInfo{}, errors.New("pipeline: sqlite: nil target/connection")
	}
	path := target.Connection.Database
	if path == "" {
		return DumpInfo{}, errors.New("pipeline: sqlite: connection.database must be the path to the .db file")
	}
	if _, err := s.statFn(path); err != nil {
		return DumpInfo{}, fmt.Errorf("pipeline: sqlite: cannot stat database %q: %w", path, err)
	}

	gz := gzip.NewWriter(out)
	defer gz.Close()

	// sqlite3 expects a single positional argument (the database path)
	// followed by a dot-command. `.backup` writes a consistent snapshot
	// to the supplied filename; we pass /dev/stdout so the bytes flow
	// through stdout into our gzip writer.
	args := []string{path, ".backup '/dev/stdout'"}
	if err := s.runner.RunStream(ctx, s.binary, args, nil, gz); err != nil {
		return DumpInfo{}, fmt.Errorf("pipeline: sqlite3 .backup exec: %w", err)
	}
	if err := gz.Close(); err != nil {
		return DumpInfo{}, fmt.Errorf("pipeline: sqlite: close gzip: %w", err)
	}

	info := DumpInfo{EngineVersion: s.versionString(ctx)}
	return info, nil
}

// versionString turns `sqlite3 --version` output into a canonical
// "SQLite <semver>" string. The raw output is e.g.
// "3.45.1 2024-01-30 16:01:20 e876e51a04…"; we keep the first token.
func (s *sqliteDriver) versionString(ctx context.Context) string {
	out, err := s.runner.Output(ctx, s.binary, []string{"--version"}, nil)
	if err != nil {
		return "SQLite"
	}
	fields := strings.Fields(string(out))
	if len(fields) == 0 {
		return "SQLite"
	}
	return "SQLite " + fields[0]
}

// looksLikeSqliteVersion accepts any --version banner whose first token
// looks like a dotted version number (e.g. "3.45.1").
func looksLikeSqliteVersion(s string) bool {
	fields := strings.Fields(s)
	if len(fields) == 0 {
		return false
	}
	for _, part := range strings.Split(fields[0], ".") {
		if _, err := strconv.Atoi(part); err != nil {
			return false
		}
	}
	return true
}

// probeSqliteMetadata opens the database file read-only and returns
// (user_version, table_count, err). Used for metadata enrichment when
// the agent has a registered sqlite driver in database/sql.
//
// To keep the agent dependency-free for MVP we only attempt the probe
// when a "sqlite" or "sqlite3" driver is registered with database/sql
// at runtime. Production builds may register one via blank-import in
// cmd/agent if richer metadata is required; the MVP build skips it.
func probeSqliteMetadata(ctx context.Context, path string) (int64, int, error) {
	for _, name := range []string{"sqlite", "sqlite3"} {
		if !sqlDriverRegistered(name) {
			continue
		}
		db, err := sql.Open(name, "file:"+path+"?mode=ro")
		if err != nil {
			return 0, 0, err
		}
		defer db.Close()
		var uv int64
		if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&uv); err != nil {
			return 0, 0, err
		}
		var tc int
		if err := db.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master WHERE type='table'").Scan(&tc); err != nil {
			return 0, 0, err
		}
		return uv, tc, nil
	}
	return 0, 0, errors.New("no sqlite driver registered with database/sql")
}

// sqlDriverRegistered reports whether `name` is registered via sql.Register.
func sqlDriverRegistered(name string) bool {
	for _, d := range sql.Drivers() {
		if d == name {
			return true
		}
	}
	return false
}

// IsSqliteGzMagic reports whether head looks like the gzip header that
// every sqliteDriver.Dump stream begins with.
func IsSqliteGzMagic(head []byte) bool {
	return len(head) >= 2 && head[0] == 0x1f && head[1] == 0x8b
}
