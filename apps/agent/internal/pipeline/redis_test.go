package pipeline

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	backupv1 "github.com/backupy/backupy/packages/proto/gen/go/backupv1"
)

// redisMockRunner is a more flexible mock than the shared mockRunner —
// it can return different output for successive Output calls so we can
// simulate the BGSAVE / LASTSAVE / CONFIG GET sequence.
type redisMockRunner struct {
	// outputs is a FIFO queue keyed by the last argument of the call
	// (e.g. "PING", "BGSAVE", "LASTSAVE", "dir", "dbfilename", "server").
	outputs map[string][][]byte
	errs    map[string]error
	calls   [][]string
}

func newRedisMockRunner() *redisMockRunner {
	return &redisMockRunner{outputs: map[string][][]byte{}, errs: map[string]error{}}
}

func (m *redisMockRunner) keyOf(args []string) string {
	for i := len(args) - 1; i >= 0; i-- {
		switch args[i] {
		case "PING", "BGSAVE", "LASTSAVE", "--version":
			return args[i]
		case "dir", "dbfilename":
			return args[i]
		case "server":
			return args[i]
		}
	}
	if len(args) > 0 {
		return args[0]
	}
	return ""
}

func (m *redisMockRunner) Output(_ context.Context, _ string, args []string, _ []string) ([]byte, error) {
	m.calls = append(m.calls, append([]string(nil), args...))
	key := m.keyOf(args)
	if err, ok := m.errs[key]; ok {
		return nil, err
	}
	q, ok := m.outputs[key]
	if !ok || len(q) == 0 {
		return []byte("PONG\n"), nil
	}
	out := q[0]
	if len(q) > 1 {
		m.outputs[key] = q[1:]
	}
	return out, nil
}

func (m *redisMockRunner) RunStream(_ context.Context, _ string, args []string, _ []string, _ io.Writer) error {
	m.calls = append(m.calls, append([]string(nil), args...))
	return nil
}

func newRedisDriverForTest(t *testing.T, runner cmdRunner, rdbContents []byte) *redisDriver {
	t.Helper()
	tmpFile, err := os.CreateTemp(t.TempDir(), "dump-*.rdb")
	require.NoError(t, err)
	_, err = tmpFile.Write(rdbContents)
	require.NoError(t, err)
	require.NoError(t, tmpFile.Close())

	return &redisDriver{
		binary:       "redis-cli",
		runner:       runner,
		fileOpen:     defaultFileOpen,
		now:          time.Now,
		pollInterval: time.Millisecond,
		pollTimeout:  time.Second,
	}
}

func TestRedis_Validate_OK(t *testing.T) {
	t.Parallel()
	m := newRedisMockRunner()
	m.outputs["--version"] = [][]byte{[]byte("redis-cli 7.2.4\n")}
	m.outputs["PING"] = [][]byte{[]byte("PONG\n")}
	d := &redisDriver{binary: "redis-cli", runner: m, fileOpen: defaultFileOpen, now: time.Now, pollInterval: time.Millisecond, pollTimeout: time.Second}
	err := d.Validate(context.Background(), &backupv1.Target{
		Type:       backupv1.DbType_REDIS,
		Connection: &backupv1.ConnectionConfig{Host: "h", Port: 6379, PasswordSecretRef: "pw"},
	})
	require.NoError(t, err)
	// PING call should carry -a and --no-auth-warning when password set.
	var sawAuth bool
	for _, c := range m.calls {
		for _, a := range c {
			if a == "--no-auth-warning" {
				sawAuth = true
			}
		}
	}
	require.True(t, sawAuth)
}

func TestRedis_Validate_PingFails(t *testing.T) {
	t.Parallel()
	m := newRedisMockRunner()
	m.outputs["--version"] = [][]byte{[]byte("redis-cli 7.2.4\n")}
	m.errs["PING"] = errors.New("connection refused")
	d := &redisDriver{binary: "redis-cli", runner: m, fileOpen: defaultFileOpen, now: time.Now, pollInterval: time.Millisecond, pollTimeout: time.Second}
	err := d.Validate(context.Background(), &backupv1.Target{Type: backupv1.DbType_REDIS, Connection: &backupv1.ConnectionConfig{}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "PING")
}

func TestRedis_Validate_BinaryMissing(t *testing.T) {
	t.Parallel()
	d := &redisDriver{binary: "redis-cli", runner: &errOutputRunner{err: errors.New("not found")}, fileOpen: defaultFileOpen, now: time.Now, pollInterval: time.Millisecond, pollTimeout: time.Second}
	err := d.Validate(context.Background(), &backupv1.Target{Type: backupv1.DbType_REDIS, Connection: &backupv1.ConnectionConfig{}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "version probe failed")
}

func TestRedis_Dump_HappyPath(t *testing.T) {
	t.Parallel()
	rdb := []byte("REDIS0011\x00\xff" + strings.Repeat("x", 64))
	tmp, err := os.CreateTemp(t.TempDir(), "dump-*.rdb")
	require.NoError(t, err)
	_, _ = tmp.Write(rdb)
	require.NoError(t, tmp.Close())

	m := newRedisMockRunner()
	m.outputs["LASTSAVE"] = [][]byte{
		[]byte("1000\n"), // initial value
		[]byte("1001\n"), // after BGSAVE — advanced
	}
	m.outputs["BGSAVE"] = [][]byte{[]byte("Background saving started\n")}
	// `CONFIG GET dir` returns two lines: key + value.
	m.outputs["dir"] = [][]byte{[]byte("dir\n" + tmp.Name()[:strings.LastIndex(tmp.Name(), "/")] + "\n")}
	m.outputs["dbfilename"] = [][]byte{[]byte("dbfilename\n" + tmp.Name()[strings.LastIndex(tmp.Name(), "/")+1:] + "\n")}
	m.outputs["server"] = [][]byte{[]byte("# Server\nredis_version:7.2.4\n")}

	d := &redisDriver{
		binary:       "redis-cli",
		runner:       m,
		fileOpen:     defaultFileOpen,
		now:          time.Now,
		pollInterval: time.Millisecond,
		pollTimeout:  time.Second,
	}
	target := &backupv1.Target{Type: backupv1.DbType_REDIS, Connection: &backupv1.ConnectionConfig{Host: "h"}}

	var buf bytes.Buffer
	info, err := d.Dump(context.Background(), target, &buf)
	require.NoError(t, err)
	require.Equal(t, "Redis 7.2.4", info.EngineVersion)
	require.True(t, IsRedisTarGzMagic(buf.Bytes()))

	// Round-trip the tar.gz to confirm the entry name + contents.
	gz, err := gzip.NewReader(&buf)
	require.NoError(t, err)
	tr := tar.NewReader(gz)
	hdr, err := tr.Next()
	require.NoError(t, err)
	require.Equal(t, "dump.rdb", hdr.Name)
	got, err := io.ReadAll(tr)
	require.NoError(t, err)
	require.Equal(t, rdb, got)
}

func TestRedis_Dump_BgsaveTimeout(t *testing.T) {
	t.Parallel()
	m := newRedisMockRunner()
	// LASTSAVE never advances: the same value is returned forever.
	m.outputs["LASTSAVE"] = [][]byte{[]byte("100\n")}
	m.outputs["BGSAVE"] = [][]byte{[]byte("Background saving started\n")}

	d := &redisDriver{
		binary:       "redis-cli",
		runner:       m,
		fileOpen:     defaultFileOpen,
		now:          time.Now,
		pollInterval: time.Millisecond,
		pollTimeout:  10 * time.Millisecond,
	}
	target := &backupv1.Target{Type: backupv1.DbType_REDIS, Connection: &backupv1.ConnectionConfig{Host: "h"}}
	var buf bytes.Buffer
	_, err := d.Dump(context.Background(), target, &buf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "BGSAVE did not complete")
}

func TestRedis_Dump_NoFilesystemAccess(t *testing.T) {
	t.Parallel()
	m := newRedisMockRunner()
	m.outputs["LASTSAVE"] = [][]byte{[]byte("100\n"), []byte("200\n")}
	m.outputs["BGSAVE"] = [][]byte{[]byte("ok\n")}
	// CONFIG GET returns empty path → driver should error clearly.
	m.outputs["dir"] = [][]byte{[]byte("dir\n\n")}
	m.outputs["dbfilename"] = [][]byte{[]byte("dbfilename\ndump.rdb\n")}

	d := &redisDriver{binary: "redis-cli", runner: m, fileOpen: defaultFileOpen, now: time.Now, pollInterval: time.Millisecond, pollTimeout: time.Second}
	target := &backupv1.Target{Type: backupv1.DbType_REDIS, Connection: &backupv1.ConnectionConfig{Host: "h"}}
	var buf bytes.Buffer
	_, err := d.Dump(context.Background(), target, &buf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "same-host filesystem access required")
}

func TestRedis_Dump_OpenFileError(t *testing.T) {
	t.Parallel()
	m := newRedisMockRunner()
	m.outputs["LASTSAVE"] = [][]byte{[]byte("100\n"), []byte("200\n")}
	m.outputs["BGSAVE"] = [][]byte{[]byte("ok\n")}
	m.outputs["dir"] = [][]byte{[]byte("dir\n/nonexistent\n")}
	m.outputs["dbfilename"] = [][]byte{[]byte("dbfilename\ndump.rdb\n")}

	d := &redisDriver{
		binary:   "redis-cli",
		runner:   m,
		fileOpen: func(path string) (io.ReadCloser, os.FileInfo, error) { return nil, nil, errors.New("missing") },
		now:      time.Now, pollInterval: time.Millisecond, pollTimeout: time.Second,
	}
	target := &backupv1.Target{Type: backupv1.DbType_REDIS, Connection: &backupv1.ConnectionConfig{Host: "h"}}
	var buf bytes.Buffer
	_, err := d.Dump(context.Background(), target, &buf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "same-host access required")
}

func TestRedis_Dump_HonoursContextCancel(t *testing.T) {
	t.Parallel()
	m := newRedisMockRunner()
	m.outputs["LASTSAVE"] = [][]byte{[]byte("100\n")} // never advances
	m.outputs["BGSAVE"] = [][]byte{[]byte("ok\n")}
	d := &redisDriver{
		binary: "redis-cli", runner: m, fileOpen: defaultFileOpen,
		now: time.Now, pollInterval: 10 * time.Millisecond, pollTimeout: time.Minute,
	}
	target := &backupv1.Target{Type: backupv1.DbType_REDIS, Connection: &backupv1.ConnectionConfig{Host: "h"}}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	var buf bytes.Buffer
	_, err := d.Dump(ctx, target, &buf)
	require.Error(t, err)
}

func TestRedis_Name(t *testing.T) {
	t.Parallel()
	require.Equal(t, "redis", (&redisDriver{}).Name())
}

func TestIsRedisTarGzMagic(t *testing.T) {
	t.Parallel()
	require.True(t, IsRedisTarGzMagic([]byte{0x1f, 0x8b, 0x08}))
	require.False(t, IsRedisTarGzMagic([]byte{0x00}))
}

func TestRedis_ConnArgs_DBIndex(t *testing.T) {
	t.Parallel()
	d := &redisDriver{}
	args := d.connArgs(&backupv1.Target{Connection: &backupv1.ConnectionConfig{Host: "h", Port: 6379, Database: "3"}})
	require.Contains(t, args, "-n")
	require.Contains(t, args, "3")

	// Non-numeric database is ignored.
	args = d.connArgs(&backupv1.Target{Connection: &backupv1.ConnectionConfig{Host: "h", Database: "notanint"}})
	require.NotContains(t, args, "-n")
}

func TestNewRedisDriverHelperUnused(t *testing.T) {
	t.Parallel()
	// Touch the helper so go vet does not flag it as dead code.
	d := newRedisDriverForTest(t, newRedisMockRunner(), []byte("x"))
	require.NotNil(t, d)
}
