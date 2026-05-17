package state

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

const testAgentKey = "bkpy_test_abcdefghijklmnopqrstuvwxyz012345"

func newStore(t *testing.T) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "state.db")
	s, err := Open(path, Options{AgentKey: testAgentKey})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s, path
}

func TestOpen_Empty(t *testing.T) {
	s, _ := newStore(t)
	_, _, err := s.LoadConfig()
	require.ErrorIs(t, err, ErrNotFound)

	_, err = s.LoadSession()
	require.ErrorIs(t, err, ErrNotFound)

	n, err := s.QueueDepth()
	require.NoError(t, err)
	require.Equal(t, 0, n)
}

func TestConfig_Roundtrip(t *testing.T) {
	s, _ := newStore(t)
	payload := []byte("hello protobuf bytes")
	require.NoError(t, s.SaveConfig(42, payload))

	v, got, err := s.LoadConfig()
	require.NoError(t, err)
	require.Equal(t, uint64(42), v)
	require.Equal(t, payload, got)
}

func TestQueue_EnqueueDequeueAck(t *testing.T) {
	s, _ := newStore(t)

	require.NoError(t, s.EnqueueJob("run-a", []byte("aaa")))
	require.NoError(t, s.EnqueueJob("run-b", []byte("bbb")))
	require.NoError(t, s.EnqueueJob("run-c", []byte("ccc")))

	n, err := s.QueueDepth()
	require.NoError(t, err)
	require.Equal(t, 3, n)

	jobs, err := s.DequeueJobs(2)
	require.NoError(t, err)
	require.Len(t, jobs, 2)
	require.Equal(t, "run-a", jobs[0].RunID)
	require.Equal(t, []byte("aaa"), jobs[0].Payload)
	require.Equal(t, "run-b", jobs[1].RunID)

	// Ack pops the head; depth drops.
	require.NoError(t, s.AckJob("run-a"))
	n, _ = s.QueueDepth()
	require.Equal(t, 2, n)

	// Idempotent re-enqueue overwrites payload.
	require.NoError(t, s.EnqueueJob("run-b", []byte("BBB")))
	jobs, _ = s.DequeueJobs(10)
	require.Equal(t, []byte("BBB"), jobs[0].Payload)
}

func TestSession(t *testing.T) {
	s, _ := newStore(t)
	require.NoError(t, s.SaveSession("sess-123", 1700000000000))
	got, err := s.LoadSession()
	require.NoError(t, err)
	require.Equal(t, "sess-123", got)
}

func TestHeartbeat(t *testing.T) {
	s, _ := newStore(t)
	ts, err := s.LastHeartbeat()
	require.NoError(t, err)
	require.Equal(t, int64(0), ts)

	require.NoError(t, s.RecordHeartbeat(1700000000000))
	ts, err = s.LastHeartbeat()
	require.NoError(t, err)
	require.Equal(t, int64(1700000000000), ts)
}

func TestLogs_BufferAndDrain(t *testing.T) {
	s, _ := newStore(t)
	require.NoError(t, s.BufferLog(100, []byte("first")))
	require.NoError(t, s.BufferLog(200, []byte("second")))
	require.NoError(t, s.BufferLog(150, []byte("middle")))

	drained, err := s.DrainLogs(10)
	require.NoError(t, err)
	require.Len(t, drained, 3)
	// Chronological order — ts_ms wins, so: 100, 150, 200.
	require.Equal(t, []byte("first"), drained[0])
	require.Equal(t, []byte("middle"), drained[1])
	require.Equal(t, []byte("second"), drained[2])

	// Second drain returns nothing.
	drained, err = s.DrainLogs(10)
	require.NoError(t, err)
	require.Empty(t, drained)
}

// TestEncryption_AtRest ensures values written to disk are NOT plaintext.
// This is the headline guarantee of the state package and the basis for
// task D-17 ("local state encryption").
func TestEncryption_AtRest(t *testing.T) {
	s, path := newStore(t)
	plaintext := []byte("super-secret-config-payload")
	require.NoError(t, s.SaveConfig(1, plaintext))
	require.NoError(t, s.EnqueueJob("run-x", []byte("queued-secret-x")))

	// Close to flush before reading raw.
	require.NoError(t, s.Close())

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.False(t, bytes.Contains(raw, plaintext), "plaintext config leaked to disk")
	require.False(t, bytes.Contains(raw, []byte("queued-secret-x")), "plaintext queue payload leaked to disk")
}

// TestEncryption_WrongKey ensures a different agent key cannot decrypt
// existing state — corruption / key rotation surfaces as a hard error
// rather than silently overwriting good data.
func TestEncryption_WrongKey(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.db")

	s, err := Open(path, Options{AgentKey: testAgentKey})
	require.NoError(t, err)
	require.NoError(t, s.SaveConfig(7, []byte("payload")))
	require.NoError(t, s.Close())

	// Reopen with a different key — must fail to decrypt.
	wrong := "bkpy_test_ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ"
	s2, err := Open(path, Options{AgentKey: wrong})
	require.NoError(t, err)
	defer s2.Close()

	_, _, err = s2.LoadConfig()
	require.Error(t, err)
	require.True(t, errors.Is(err, errCipher) || err.Error() != "", "expected cipher error, got: %v", err)
}

func TestOpen_EmptyKey(t *testing.T) {
	dir := t.TempDir()
	_, err := Open(filepath.Join(dir, "state.db"), Options{AgentKey: ""})
	require.Error(t, err)
}

func TestEnqueue_EmptyRunID(t *testing.T) {
	s, _ := newStore(t)
	require.Error(t, s.EnqueueJob("", []byte("x")))
}
