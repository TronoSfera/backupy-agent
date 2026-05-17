package pipeline

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	backupv1 "github.com/backupy/backupy/packages/proto/gen/go/backupv1"
)

// mockRunner is a deterministic cmdRunner for the pg_dump / mysqldump
// drivers. It records the (name, args, env) tuples and replays the
// canned stdout / stderr the test sets up.
type mockRunner struct {
	outputResp map[string][]byte // key = first arg (e.g. "--version")
	streamResp []byte
	streamErr  error
	calls      []mockCall
}

type mockCall struct {
	Args []string
	Env  []string
}

func (m *mockRunner) Output(_ context.Context, _ string, args []string, env []string) ([]byte, error) {
	m.calls = append(m.calls, mockCall{Args: append([]string(nil), args...), Env: append([]string(nil), env...)})
	if len(args) == 0 {
		return nil, nil
	}
	if v, ok := m.outputResp[args[0]]; ok {
		return v, nil
	}
	return nil, nil
}

func (m *mockRunner) RunStream(_ context.Context, _ string, args []string, env []string, out io.Writer) error {
	m.calls = append(m.calls, mockCall{Args: append([]string(nil), args...), Env: append([]string(nil), env...)})
	if m.streamErr != nil {
		return m.streamErr
	}
	if len(m.streamResp) > 0 {
		_, _ = out.Write(m.streamResp)
	}
	return nil
}

func TestParsePgDumpVersion(t *testing.T) {
	got := parsePgDumpVersion("pg_dump (PostgreSQL) 16.2")
	require.Equal(t, "PostgreSQL 16.2", got)

	got = parsePgDumpVersion("pg_dump (PostgreSQL) 16.2 (Debian 16.2-1.pgdg120+1)")
	require.Contains(t, got, "PostgreSQL 16.2")
}

func TestIsPgDumpMagic(t *testing.T) {
	require.True(t, IsPgDumpMagic([]byte("PGDMP\x00")))
	require.False(t, IsPgDumpMagic([]byte("NOTHING")))
	require.False(t, IsPgDumpMagic(nil))
}

func TestPgDump_DumpWritesPayload(t *testing.T) {
	mock := &mockRunner{
		outputResp: map[string][]byte{"--version": []byte("pg_dump (PostgreSQL) 16.2")},
		streamResp: []byte("PGDMP\x01\x02\x03"),
	}
	d := &pgDump{binary: "pg_dump", runner: mock}
	target := &backupv1.Target{
		Type: backupv1.DbType_POSTGRESQL,
		Connection: &backupv1.ConnectionConfig{
			Host: "h", Port: 5432, Database: "db", Username: "u", PasswordSecretRef: "p",
		},
	}
	var buf testWriter
	info, err := d.Dump(context.Background(), target, &buf)
	require.NoError(t, err)
	require.Equal(t, "PostgreSQL 16.2", info.EngineVersion)
	require.Equal(t, []byte("PGDMP\x01\x02\x03"), buf.Bytes())

	// Confirm PGPASSWORD env is propagated.
	require.NotEmpty(t, mock.calls)
	streamCall := mock.calls[0]
	require.Contains(t, streamCall.Env, "PGPASSWORD=p")
}

// testWriter is a tiny bytes.Buffer alternative that exposes Bytes() and
// satisfies io.Writer without pulling bytes into the file's imports.
type testWriter struct{ b []byte }

func (t *testWriter) Write(p []byte) (int, error) {
	t.b = append(t.b, p...)
	return len(p), nil
}
func (t *testWriter) Bytes() []byte { return t.b }
