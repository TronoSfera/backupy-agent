package pipeline

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	backupv1 "github.com/backupy/backupy/packages/proto/gen/go/backupv1"
)

func TestSqlite_Name(t *testing.T) {
	t.Parallel()
	require.Equal(t, "sqlite", (&sqliteDriver{}).Name())
}

func TestSqlite_LooksLikeVersion(t *testing.T) {
	t.Parallel()
	require.True(t, looksLikeSqliteVersion("3.45.1 2024-01-30 16:01:20"))
	require.True(t, looksLikeSqliteVersion("3.42.0"))
	require.False(t, looksLikeSqliteVersion("totally bogus"))
	require.False(t, looksLikeSqliteVersion(""))
}

func TestSqlite_Validate_MissingDatabasePath(t *testing.T) {
	t.Parallel()
	d := &sqliteDriver{
		binary: "sqlite3",
		runner: &mockRunner{outputResp: map[string][]byte{"--version": []byte("3.45.1\n")}},
		statFn: os.Stat,
	}
	err := d.Validate(context.Background(), &backupv1.Target{Type: backupv1.DbType_SQLITE, Connection: &backupv1.ConnectionConfig{}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "database must be the path")
}

func TestSqlite_Validate_FileNotFound(t *testing.T) {
	t.Parallel()
	d := &sqliteDriver{
		binary: "sqlite3",
		runner: &mockRunner{outputResp: map[string][]byte{"--version": []byte("3.45.1\n")}},
		statFn: os.Stat,
	}
	err := d.Validate(context.Background(), &backupv1.Target{
		Type:       backupv1.DbType_SQLITE,
		Connection: &backupv1.ConnectionConfig{Database: "/path/does/not/exist.db"},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot stat database")
}

func TestSqlite_Validate_BinaryMissing(t *testing.T) {
	t.Parallel()
	d := &sqliteDriver{
		binary: "sqlite3",
		runner: &errOutputRunner{err: errors.New("not found")},
		statFn: os.Stat,
	}
	err := d.Validate(context.Background(), &backupv1.Target{
		Type:       backupv1.DbType_SQLITE,
		Connection: &backupv1.ConnectionConfig{Database: "/tmp/foo.db"},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "version probe failed")
}

func TestSqlite_Validate_OK(t *testing.T) {
	t.Parallel()
	tmp := writeTempDB(t, []byte("placeholder"))
	d := &sqliteDriver{
		binary: "sqlite3",
		runner: &mockRunner{outputResp: map[string][]byte{"--version": []byte("3.45.1 2024-01-30\n")}},
		statFn: os.Stat,
	}
	err := d.Validate(context.Background(), &backupv1.Target{
		Type:       backupv1.DbType_SQLITE,
		Connection: &backupv1.ConnectionConfig{Database: tmp},
	})
	require.NoError(t, err)
}

func TestSqlite_Dump_WrapsOutputInGzip(t *testing.T) {
	t.Parallel()
	tmp := writeTempDB(t, []byte("placeholder"))
	payload := bytes.Repeat([]byte{0x53, 0x51, 0x4c}, 16) // pretend SQLite header bytes
	mock := &mockRunner{
		outputResp: map[string][]byte{"--version": []byte("3.45.1 2024-01-30\n")},
		streamResp: payload,
	}
	d := &sqliteDriver{binary: "sqlite3", runner: mock, statFn: os.Stat}

	var buf bytes.Buffer
	info, err := d.Dump(context.Background(), &backupv1.Target{
		Type:       backupv1.DbType_SQLITE,
		Connection: &backupv1.ConnectionConfig{Database: tmp},
	}, &buf)
	require.NoError(t, err)
	require.Equal(t, "SQLite 3.45.1", info.EngineVersion)
	require.True(t, IsSqliteGzMagic(buf.Bytes()))

	gz, err := gzip.NewReader(&buf)
	require.NoError(t, err)
	defer gz.Close()
	got, err := io.ReadAll(gz)
	require.NoError(t, err)
	require.Equal(t, payload, got)

	// Confirm `.backup '/dev/stdout'` was invoked with the right path.
	require.NotEmpty(t, mock.calls)
	streamCall := mock.calls[0]
	require.Equal(t, tmp, streamCall.Args[0])
	require.Equal(t, ".backup '/dev/stdout'", streamCall.Args[1])
}

func TestSqlite_Dump_MissingPath(t *testing.T) {
	t.Parallel()
	d := &sqliteDriver{binary: "sqlite3", runner: &mockRunner{}, statFn: os.Stat}
	var buf bytes.Buffer
	_, err := d.Dump(context.Background(), &backupv1.Target{
		Type:       backupv1.DbType_SQLITE,
		Connection: &backupv1.ConnectionConfig{},
	}, &buf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "must be the path")
}

func TestSqlite_Dump_StreamErrorWraps(t *testing.T) {
	t.Parallel()
	tmp := writeTempDB(t, []byte("placeholder"))
	mock := &mockRunner{
		outputResp: map[string][]byte{"--version": []byte("3.45.1\n")},
		streamErr:  errors.New("permission denied"),
	}
	d := &sqliteDriver{binary: "sqlite3", runner: mock, statFn: os.Stat}
	var buf bytes.Buffer
	_, err := d.Dump(context.Background(), &backupv1.Target{
		Type:       backupv1.DbType_SQLITE,
		Connection: &backupv1.ConnectionConfig{Database: tmp},
	}, &buf)
	require.Error(t, err)
	require.Contains(t, err.Error(), ".backup exec")
}

func TestIsSqliteGzMagic(t *testing.T) {
	t.Parallel()
	require.True(t, IsSqliteGzMagic([]byte{0x1f, 0x8b, 0x08}))
	require.False(t, IsSqliteGzMagic([]byte{0x00, 0x00}))
}

func TestSqlite_ProbeMetadata_NoDriver(t *testing.T) {
	t.Parallel()
	// In the MVP build no sqlite database/sql driver is registered.
	// Confirm probeSqliteMetadata returns the expected sentinel error so
	// callers can degrade gracefully.
	_, _, err := probeSqliteMetadata(context.Background(), "/tmp/anything.db")
	require.Error(t, err)
	require.Contains(t, err.Error(), "no sqlite driver registered")
}

// writeTempDB drops a tiny placeholder file into a temp directory and
// returns its absolute path.
func writeTempDB(t *testing.T, contents []byte) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "test.db")
	require.NoError(t, os.WriteFile(p, contents, 0o600))
	return p
}
