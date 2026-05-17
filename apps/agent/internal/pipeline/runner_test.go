package pipeline

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"

	backupv1 "github.com/backupy/backupy/packages/proto/gen/go/backupv1"
)

// hexSha256 returns the lower-case hex SHA-256 of b. Test helper kept
// in this file so the production runner.go has no test-only imports.
func hexSha256(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// fakeDriver emits a fixed plaintext payload prefixed with the configured magic.
type fakeDriver struct {
	name    string
	payload []byte
	version string
	failVal bool
	failDmp bool
}

func (f *fakeDriver) Name() string { return f.name }

func (f *fakeDriver) Validate(_ context.Context, _ *backupv1.Target) error {
	if f.failVal {
		return errors.New("validate boom")
	}
	return nil
}

func (f *fakeDriver) Dump(_ context.Context, _ *backupv1.Target, out io.Writer) (DumpInfo, error) {
	if f.failDmp {
		return DumpInfo{}, errors.New("dump boom")
	}
	if _, err := out.Write(f.payload); err != nil {
		return DumpInfo{}, err
	}
	return DumpInfo{EngineVersion: f.version}, nil
}

// simpleLookups satisfies both TargetLookup and JobLookup with a single
// fixed (job, target) tuple.
type simpleLookups struct {
	job    *backupv1.BackupJobSpec
	target *backupv1.Target
}

func (s *simpleLookups) Job(id string) (*backupv1.BackupJobSpec, bool) {
	if s.job != nil && s.job.Id == id {
		return s.job, true
	}
	return nil, false
}

func (s *simpleLookups) Target(id string) (*backupv1.Target, bool) {
	if s.target != nil && s.target.Id == id {
		return s.target, true
	}
	return nil, false
}

// startFakeS3 spins up an httptest server that accepts a single PUT
// and records the body in `received`.
//
// The handler tolerates abrupt client disconnects — the pipeline may
// cancel the upload mid-stream when an earlier stage (smoke check,
// dump, etc.) fails. In that case `io.Copy` returns "unexpected EOF"
// or "use of closed network connection"; we record whatever bytes
// arrived and respond with 200 so the uploader sees the upload as
// having completed (the run's error still propagates from the failed
// stage upstream).
func startFakeS3(t *testing.T, received *bytes.Buffer) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPut, r.Method)
		_, _ = io.Copy(received, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
}

func TestRunner_HappyPath_PostgreSQL(t *testing.T) {
	// 1 MiB of random bytes prefixed with the pg_dump magic.
	plaintext := append([]byte(PgDumpMagic), make([]byte, 1<<20)...)
	_, err := rand.Read(plaintext[len(PgDumpMagic):])
	require.NoError(t, err)

	driver := &fakeDriver{
		name:    "pg_dump",
		payload: plaintext,
		version: "PostgreSQL 16.2",
	}
	dek := make([]byte, 32)
	_, _ = rand.Read(dek)

	job := &backupv1.BackupJobSpec{Id: "job-1", TargetId: "tgt-1"}
	target := &backupv1.Target{
		Id:   "tgt-1",
		Type: backupv1.DbType_POSTGRESQL,
		Connection: &backupv1.ConnectionConfig{
			Host: "127.0.0.1", Port: 5432, Database: "x", Username: "u",
		},
	}
	lookups := &simpleLookups{job: job, target: target}

	var received bytes.Buffer
	srv := startFakeS3(t, &received)
	defer srv.Close()

	runner := NewRunner(
		map[string]Driver{"postgresql": driver},
		NewUploaderWithClient(srv.Client()),
		WithTargetLookup(lookups),
		WithJobLookup(lookups),
	)

	req := &backupv1.RunBackup{
		JobId:        "job-1",
		RunId:        "run-1",
		EncryptedDek: dek,
		UploadCreds: &backupv1.S3UploadCreds{
			PresignedPutUrl: srv.URL + "/run-1.enc",
			FinalS3Key:      "co_test/agt_test/job_job-1/run_run-1.enc",
		},
	}

	completed, err := runner.Run(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, "job-1", completed.JobId)
	require.Equal(t, "run-1", completed.RunId)
	require.Equal(t, "zstd", completed.Compression)
	require.Equal(t, "PostgreSQL 16.2", completed.DbEngineVersion)
	require.Equal(t, uint64(received.Len()), completed.SizeBytes)
	require.NotEmpty(t, completed.Sha256)
	require.Equal(t, hexSha256(received.Bytes()), completed.Sha256, "sha256 must cover the ciphertext bytes actually uploaded")
	require.Equal(t, dek, completed.EncryptedDek, "encrypted_dek must be passed through unchanged")

	// End-to-end: decrypt + decompress the uploaded blob and verify it
	// equals the original plaintext.
	enc, err := NewEncryptor(dek)
	require.NoError(t, err)
	var compressed bytes.Buffer
	_, err = enc.Decrypt(&received, &compressed)
	require.NoError(t, err)

	zr, err := zstd.NewReader(&compressed)
	require.NoError(t, err)
	defer zr.Close()
	round, err := io.ReadAll(zr)
	require.NoError(t, err)
	require.Equal(t, plaintext, round)
}

func TestRunner_MissingMagic_FailsBeforeUpload(t *testing.T) {
	// Driver claims to be pg_dump but emits the wrong header.
	driver := &fakeDriver{name: "pg_dump", payload: []byte("NOTAPGDUMP"), version: "?"}
	dek := make([]byte, 32)
	_, _ = rand.Read(dek)

	job := &backupv1.BackupJobSpec{Id: "j", TargetId: "t"}
	target := &backupv1.Target{Id: "t", Type: backupv1.DbType_POSTGRESQL, Connection: &backupv1.ConnectionConfig{Host: "x"}}
	lookups := &simpleLookups{job: job, target: target}

	var received bytes.Buffer
	srv := startFakeS3(t, &received)
	defer srv.Close()

	runner := NewRunner(
		map[string]Driver{"postgresql": driver},
		NewUploaderWithClient(srv.Client()),
		WithTargetLookup(lookups),
		WithJobLookup(lookups),
	)
	req := &backupv1.RunBackup{
		JobId: "j", RunId: "r",
		EncryptedDek: dek,
		UploadCreds:  &backupv1.S3UploadCreds{PresignedPutUrl: srv.URL + "/r.enc", FinalS3Key: "k"},
	}

	_, err := runner.Run(context.Background(), req)
	require.Error(t, err)
}

func TestRunner_ValidateFailsFast(t *testing.T) {
	driver := &fakeDriver{name: "pg_dump", payload: []byte(PgDumpMagic), failVal: true}
	dek := make([]byte, 32)
	_, _ = rand.Read(dek)

	job := &backupv1.BackupJobSpec{Id: "j", TargetId: "t"}
	target := &backupv1.Target{Id: "t", Type: backupv1.DbType_POSTGRESQL, Connection: &backupv1.ConnectionConfig{Host: "x"}}
	lookups := &simpleLookups{job: job, target: target}

	runner := NewRunner(
		map[string]Driver{"postgresql": driver},
		NewUploader(),
		WithTargetLookup(lookups),
		WithJobLookup(lookups),
	)
	req := &backupv1.RunBackup{
		JobId: "j", RunId: "r",
		EncryptedDek: dek,
		UploadCreds:  &backupv1.S3UploadCreds{PresignedPutUrl: "http://127.0.0.1:0/never", FinalS3Key: "k"},
	}
	_, err := runner.Run(context.Background(), req)
	require.Error(t, err)
	require.Contains(t, err.Error(), "validate stage")
}

func TestRunner_UnknownDriver(t *testing.T) {
	dek := make([]byte, 32)
	_, _ = rand.Read(dek)
	job := &backupv1.BackupJobSpec{Id: "j", TargetId: "t"}
	target := &backupv1.Target{Id: "t", Type: backupv1.DbType_MONGODB, Connection: &backupv1.ConnectionConfig{Host: "x"}}
	lookups := &simpleLookups{job: job, target: target}

	runner := NewRunner(
		map[string]Driver{"postgresql": &fakeDriver{name: "pg_dump", payload: []byte(PgDumpMagic)}},
		NewUploader(),
		WithTargetLookup(lookups),
		WithJobLookup(lookups),
	)
	req := &backupv1.RunBackup{
		JobId: "j", RunId: "r",
		EncryptedDek: dek,
		UploadCreds:  &backupv1.S3UploadCreds{PresignedPutUrl: "http://127.0.0.1:0/", FinalS3Key: "k"},
	}
	_, err := runner.Run(context.Background(), req)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no driver registered")
}

func TestRunner_DEKWrongLength(t *testing.T) {
	driver := &fakeDriver{name: "pg_dump", payload: []byte(PgDumpMagic)}
	job := &backupv1.BackupJobSpec{Id: "j", TargetId: "t"}
	target := &backupv1.Target{Id: "t", Type: backupv1.DbType_POSTGRESQL, Connection: &backupv1.ConnectionConfig{Host: "x"}}
	lookups := &simpleLookups{job: job, target: target}

	runner := NewRunner(
		map[string]Driver{"postgresql": driver},
		NewUploader(),
		WithTargetLookup(lookups),
		WithJobLookup(lookups),
	)
	req := &backupv1.RunBackup{
		JobId: "j", RunId: "r",
		EncryptedDek: []byte("short"),
		UploadCreds:  &backupv1.S3UploadCreds{PresignedPutUrl: "http://127.0.0.1:0/", FinalS3Key: "k"},
	}
	_, err := runner.Run(context.Background(), req)
	require.Error(t, err)
}
