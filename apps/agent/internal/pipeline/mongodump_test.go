package pipeline

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	backupv1 "github.com/backupy/backupy/packages/proto/gen/go/backupv1"
)

func TestParseMongodumpVersion(t *testing.T) {
	t.Parallel()
	got := parseMongodumpVersion("mongodump version: 100.9.5\ngit version: abc\n")
	require.Equal(t, "MongoDB Tools 100.9.5", got)

	require.Equal(t, "MongoDB", parseMongodumpVersion("totally unexpected"))
}

func TestIsMongodumpArchiveMagic(t *testing.T) {
	t.Parallel()
	require.True(t, IsMongodumpArchiveMagic([]byte{0x1f, 0x8b, 0x08})) // gzip
	require.True(t, IsMongodumpArchiveMagic([]byte{0x6d, 0xe2, 0x99, 0x81}))
	require.False(t, IsMongodumpArchiveMagic([]byte{0x00, 0x00}))
	require.False(t, IsMongodumpArchiveMagic(nil))
}

func TestMongoDump_Validate_MissingTarget(t *testing.T) {
	t.Parallel()
	d := &mongoDump{binary: "mongodump", runner: &mockRunner{}}
	require.Error(t, d.Validate(context.Background(), nil))
	require.Error(t, d.Validate(context.Background(), &backupv1.Target{}))
}

func TestMongoDump_Validate_BinaryMissing(t *testing.T) {
	t.Parallel()
	mock := &mockRunner{outputResp: map[string][]byte{"--version": []byte("")}}
	mock.streamErr = errors.New("exec: \"mongodump\": not found in $PATH")
	d := &mongoDump{binary: "mongodump", runner: &errOutputRunner{err: errors.New("not found")}}
	target := &backupv1.Target{Type: backupv1.DbType_MONGODB, Connection: &backupv1.ConnectionConfig{Host: "h"}}
	err := d.Validate(context.Background(), target)
	require.Error(t, err)
	require.Contains(t, err.Error(), "version probe failed")
}

func TestMongoDump_Validate_OK(t *testing.T) {
	t.Parallel()
	mock := &mockRunner{outputResp: map[string][]byte{"--version": []byte("mongodump version: 100.9.5\n")}}
	d := &mongoDump{binary: "mongodump", runner: mock}
	target := &backupv1.Target{Type: backupv1.DbType_MONGODB, Connection: &backupv1.ConnectionConfig{Host: "h"}}
	require.NoError(t, d.Validate(context.Background(), target))
}

func TestMongoDump_Dump_StreamsBytes(t *testing.T) {
	t.Parallel()
	payload := []byte{0x1f, 0x8b, 0x08, 0x00, 0xde, 0xad, 0xbe, 0xef}
	mock := &mockRunner{
		outputResp: map[string][]byte{"--version": []byte("mongodump version: 100.9.5")},
		streamResp: payload,
	}
	d := &mongoDump{binary: "mongodump", runner: mock}
	target := &backupv1.Target{
		Type: backupv1.DbType_MONGODB,
		Connection: &backupv1.ConnectionConfig{
			Host: "h", Port: 27017, Username: "u", PasswordSecretRef: "p", Database: "mydb",
		},
	}
	var buf testWriter
	info, err := d.Dump(context.Background(), target, &buf)
	require.NoError(t, err)
	require.Equal(t, "MongoDB Tools 100.9.5", info.EngineVersion)
	require.Equal(t, payload, buf.Bytes())

	// Confirm the right CLI flags were passed.
	require.NotEmpty(t, mock.calls)
	streamCall := mock.calls[0]
	require.Contains(t, streamCall.Args, "--archive")
	require.Contains(t, streamCall.Args, "--gzip")
	require.Contains(t, streamCall.Args, "--host")
	require.Contains(t, streamCall.Args, "--port")
	require.Contains(t, streamCall.Args, "--username")
	require.Contains(t, streamCall.Args, "--password")
	require.Contains(t, streamCall.Args, "--db")
	require.Contains(t, streamCall.Args, "mydb")
	require.Contains(t, streamCall.Args, "--authenticationDatabase")
}

func TestMongoDump_Dump_URI(t *testing.T) {
	t.Parallel()
	mock := &mockRunner{
		outputResp: map[string][]byte{"--version": []byte("mongodump version: 100.9.5")},
		streamResp: []byte{0x1f, 0x8b},
	}
	d := &mongoDump{binary: "mongodump", runner: mock}
	target := &backupv1.Target{
		Type: backupv1.DbType_MONGODB,
		Connection: &backupv1.ConnectionConfig{
			Host: "mongodb://user:pw@h:27017/?authSource=admin",
		},
	}
	var buf testWriter
	_, err := d.Dump(context.Background(), target, &buf)
	require.NoError(t, err)
	require.Contains(t, mock.calls[0].Args, "--uri")
	require.Contains(t, mock.calls[0].Args, "mongodb://user:pw@h:27017/?authSource=admin")
	require.NotContains(t, mock.calls[0].Args, "--host")
}

func TestMongoDump_Dump_StreamErrorWraps(t *testing.T) {
	t.Parallel()
	mock := &mockRunner{
		outputResp: map[string][]byte{"--version": []byte("mongodump version: 100.9.5")},
		streamErr:  errors.New("boom (stderr: connection refused)"),
	}
	d := &mongoDump{binary: "mongodump", runner: mock}
	target := &backupv1.Target{Type: backupv1.DbType_MONGODB, Connection: &backupv1.ConnectionConfig{Host: "h"}}
	var buf testWriter
	_, err := d.Dump(context.Background(), target, &buf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "mongodump exec")
}

func TestMongoDump_Name(t *testing.T) {
	t.Parallel()
	require.Equal(t, "mongodump", (&mongoDump{}).Name())
}

func TestRingBuffer_RetainsTail(t *testing.T) {
	t.Parallel()
	r := newRingBuffer(8)
	_, _ = r.Write([]byte("0123456789"))
	require.Equal(t, "23456789", r.Tail())
	_, _ = r.Write([]byte("abc"))
	require.Equal(t, "56789abc", r.Tail())
}

// errOutputRunner is a cmdRunner whose Output always errors. Used to
// simulate "binary not found" without touching the filesystem.
type errOutputRunner struct{ err error }

func (e *errOutputRunner) Output(_ context.Context, _ string, _ []string, _ []string) ([]byte, error) {
	return nil, e.err
}

func (e *errOutputRunner) RunStream(_ context.Context, _ string, _ []string, _ []string, _ io.Writer) error {
	return e.err
}
