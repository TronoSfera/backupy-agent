package pipeline

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/backupy/backupy/apps/agent/internal/metrics"
	backupv1 "github.com/backupy/backupy/packages/proto/gen/go/backupv1"
)

// Runner orchestrates one RunBackup end-to-end: validate driver → dump
// → compress (zstd) → encrypt (AES-256-GCM) → upload (presigned PUT) →
// build BackupCompleted.
//
// On any stage failure the returned error wraps a stage-tagged message
// so the caller (WSS client) can forward it verbatim in JobUpdate.
type Runner struct {
	drivers  map[string]Driver
	uploader *Uploader
	logger   *slog.Logger

	// dekResolver decrypts the KMS-wrapped DEK delivered in RunBackup.
	// In MVP-zero the agent is wired to a no-op resolver that treats
	// the bytes as a literal 32-byte DEK (the server has already done
	// the KMS unwrap). Production builds inject a real KMS client.
	dekResolver DEKResolver

	// targetLookup answers "given target_id, return the Target spec".
	// Plumbed by the caller (typically the WSS client which holds the
	// AgentConfig snapshot). For tests, an in-memory map suffices.
	targetLookup TargetLookup

	// jobLookup answers "given job_id, return the BackupJobSpec".
	jobLookup JobLookup
}

// DEKResolver decrypts the KMS-wrapped DEK from RunBackup. Returns the
// 32-byte raw DEK ready to feed into NewEncryptor.
type DEKResolver interface {
	Unwrap(ctx context.Context, encryptedDEK []byte) ([]byte, error)
}

// TargetLookup resolves a target_id to a Target spec (carries
// connection details).
type TargetLookup interface {
	Target(id string) (*backupv1.Target, bool)
}

// JobLookup resolves a job_id to a BackupJobSpec (carries target_id
// and operational knobs like timeout_sec).
type JobLookup interface {
	Job(id string) (*backupv1.BackupJobSpec, bool)
}

// RunnerOption configures a Runner.
type RunnerOption func(*Runner)

// WithLogger overrides the default slog.Default() logger.
func WithLogger(l *slog.Logger) RunnerOption {
	return func(r *Runner) { r.logger = l }
}

// WithDEKResolver injects a custom DEK resolver. Defaults to a
// passthrough that uses the encrypted_dek bytes as-is.
func WithDEKResolver(d DEKResolver) RunnerOption {
	return func(r *Runner) { r.dekResolver = d }
}

// WithTargetLookup injects the AgentConfig snapshot accessor.
func WithTargetLookup(t TargetLookup) RunnerOption {
	return func(r *Runner) { r.targetLookup = t }
}

// WithJobLookup injects the AgentConfig snapshot accessor.
func WithJobLookup(j JobLookup) RunnerOption {
	return func(r *Runner) { r.jobLookup = j }
}

// NewRunner constructs a Runner. drivers maps DbType-string ("postgresql"
// | "mysql" | "mariadb") to a Driver. uploader is required.
func NewRunner(drivers map[string]Driver, uploader *Uploader, opts ...RunnerOption) *Runner {
	r := &Runner{
		drivers:     drivers,
		uploader:    uploader,
		logger:      slog.Default(),
		dekResolver: passthroughDEK{},
	}
	for _, o := range opts {
		o(r)
	}
	return r
}

// Run executes one backup. On success returns a populated BackupCompleted.
// On failure returns a wrapped error.
func (r *Runner) Run(ctx context.Context, req *backupv1.RunBackup) (completed *backupv1.BackupCompleted, retErr error) {
	if req == nil {
		return nil, errors.New("pipeline: nil RunBackup")
	}
	if req.UploadCreds == nil || req.UploadCreds.PresignedPutUrl == "" {
		return nil, errors.New("pipeline: RunBackup missing upload credentials")
	}
	if r.uploader == nil {
		return nil, errors.New("pipeline: runner has no uploader")
	}

	start := time.Now()
	// --- D-19 BEGIN: record run outcome + duration regardless of exit path.
	defer func() {
		status := "success"
		if retErr != nil {
			status = "failure"
		}
		metrics.RunsTotal.WithLabelValues(req.JobId, status).Inc()
		metrics.RunDuration.WithLabelValues(req.JobId).Observe(time.Since(start).Seconds())
		if completed != nil {
			metrics.RunSizeBytes.WithLabelValues(req.JobId).Observe(float64(completed.SizeBytes))
		}
	}()
	// --- D-19 END

	// Resolve job → target → driver.
	job, target, err := r.resolve(req)
	if err != nil {
		return nil, err
	}
	driverKey := dbTypeKey(target.Type)
	driver, ok := r.drivers[driverKey]
	if !ok {
		return nil, fmt.Errorf("pipeline: no driver registered for db_type=%s", driverKey)
	}

	// Unwrap the DEK once. The plaintext DEK never leaves this function.
	dek, err := r.dekResolver.Unwrap(ctx, req.EncryptedDek)
	if err != nil {
		return nil, fmt.Errorf("pipeline: unwrap DEK: %w", err)
	}
	defer wipe(dek)

	encryptor, err := NewEncryptor(dek)
	if err != nil {
		return nil, fmt.Errorf("pipeline: build encryptor: %w", err)
	}

	// Smoke-validate the driver before we burn upload time on a dead db.
	if err := driver.Validate(ctx, target); err != nil {
		return nil, fmt.Errorf("pipeline: validate stage: %w", err)
	}

	// Apply per-job timeout if configured.
	if job != nil && job.TimeoutSec > 0 {
		c, cancel := context.WithTimeout(ctx, time.Duration(job.TimeoutSec)*time.Second)
		defer cancel()
		ctx = c
	}

	// --- B19 BEGIN: D-16 pre/post hooks.
	//
	// Pre-hooks run before the dump. A non-zero pre-hook FAILS the run
	// (the database is not touched). Post-hooks run after the upload
	// stage regardless of pipeline outcome; their failures are logged
	// but do not change the run's terminal status.
	//
	// Both sets share a single HookSet so their combined runtime is
	// capped by HooksTotalBudget. We defer the post-hook block below
	// inside a wrapper so it executes whether the pipeline succeeds or
	// fails.
	hookSet := NewHookSet()
	var preHooks, postHooks []string
	if job != nil {
		preHooks = job.PreHooks
		postHooks = job.PostHooks
	}
	for i, cmd := range preHooks {
		if i >= HooksMaxCount {
			r.logger.Warn("pre-hook skipped: HooksMaxCount exceeded",
				slog.String("job_id", req.JobId),
				slog.Int("hook_index", i))
			break
		}
		res, hookErr := hookSet.Run(ctx, cmd, nil, 0)
		if hookErr != nil {
			r.logger.Error("pre-hook failed; aborting run before dump",
				slog.String("job_id", req.JobId),
				slog.String("run_id", req.RunId),
				slog.Int("hook_index", i),
				slog.Int("exit_code", res.ExitCode),
				slog.String("stderr", res.Stderr),
				slog.Any("err", hookErr))
			return nil, fmt.Errorf("pipeline: pre_hook[%d] failed: %w", i, hookErr)
		}
		r.logger.Info("pre-hook ok",
			slog.String("job_id", req.JobId),
			slog.Int("hook_index", i),
			slog.Duration("duration", res.Duration))
	}
	// post-hooks fire on every exit path (success or failure).
	defer func() {
		for i, cmd := range postHooks {
			if i >= HooksMaxCount {
				r.logger.Warn("post-hook skipped: HooksMaxCount exceeded",
					slog.String("job_id", req.JobId),
					slog.Int("hook_index", i))
				break
			}
			// Use a fresh background context so a cancelled run still
			// gets its post-hooks (e.g. "release lock" must run).
			res, hookErr := hookSet.Run(context.Background(), cmd, nil, 0)
			if hookErr != nil {
				r.logger.Error("post-hook failed (non-fatal)",
					slog.String("job_id", req.JobId),
					slog.String("run_id", req.RunId),
					slog.Int("hook_index", i),
					slog.Int("exit_code", res.ExitCode),
					slog.String("stderr", res.Stderr),
					slog.Any("err", hookErr))
				continue
			}
			r.logger.Info("post-hook ok",
				slog.String("job_id", req.JobId),
				slog.Int("hook_index", i),
				slog.Duration("duration", res.Duration))
		}
	}()
	// --- B19 END

	// Wire the pipe chain:
	//   driver.Dump -> dumpPW  (PipeWriter)
	//                  dumpPR  (PipeReader)
	//       zstd       -> compressedPW
	//                     compressedPR
	//       encrypt    -> encryptedPW
	//                     encryptedPR
	//       uploader   -> presigned PUT, sha256 over ciphertext
	//
	// We use io.Pipe to backpressure each stage onto the next without
	// buffering the full backup in memory.

	dumpPR, dumpPW := io.Pipe()
	compressedPR, compressedPW := io.Pipe()
	encryptedPR, encryptedPW := io.Pipe()

	dumpInfoCh := make(chan DumpInfo, 1)
	// stageErr collects the first error from any stage so the caller
	// gets a meaningful message regardless of which stage failed first.
	errs := make(chan error, 4)

	// Stage 1 — dump.
	go func() {
		defer dumpPW.Close()
		info, err := driver.Dump(ctx, target, dumpPW)
		if err != nil {
			_ = dumpPW.CloseWithError(err)
			errs <- fmt.Errorf("dump: %w", err)
			dumpInfoCh <- DumpInfo{}
			return
		}
		dumpInfoCh <- info
		errs <- nil
	}()

	// Stage 2 — zstd compress, gated on a magic-byte smoke check.
	// The peek is performed inside the goroutine so the main goroutine
	// is not blocked waiting for the first bytes of the dump.
	go func() {
		defer compressedPW.Close()
		validated, smokeErr := smokeValidatedReader(dumpPR, driver.Name())
		if smokeErr != nil {
			_ = compressedPW.CloseWithError(smokeErr)
			// Tear down the dump pipe so the dump goroutine unblocks
			// from its Write loop and exits promptly.
			_ = dumpPR.CloseWithError(smokeErr)
			errs <- fmt.Errorf("smoke: %w", smokeErr)
			return
		}
		_, _, err := CompressZstd(validated, compressedPW)
		if err != nil {
			_ = compressedPW.CloseWithError(err)
			_ = dumpPR.CloseWithError(err)
			errs <- fmt.Errorf("compress: %w", err)
			return
		}
		errs <- nil
	}()

	// Stage 3 — encrypt.
	go func() {
		defer encryptedPW.Close()
		if _, err := encryptor.Stream(compressedPR, encryptedPW); err != nil {
			_ = encryptedPW.CloseWithError(err)
			_ = compressedPR.CloseWithError(err)
			errs <- fmt.Errorf("encrypt: %w", err)
			return
		}
		errs <- nil
	}()

	// Stage 4 — upload (blocking call on the calling goroutine). On
	// failure we still need to wait for the three upstream goroutines
	// to unwind so the function does not leak them; closing the pipe
	// readers below makes their pending Write calls return promptly.
	sha256hex, uploaded, uploadErr := r.uploader.Put(ctx, req.UploadCreds.PresignedPutUrl, encryptedPR, -1)
	if uploadErr != nil {
		// Closing the readers signals every upstream Write to fail
		// with io.ErrClosedPipe so the producer goroutines exit.
		_ = encryptedPR.CloseWithError(uploadErr)
		_ = compressedPR.CloseWithError(uploadErr)
		_ = dumpPR.CloseWithError(uploadErr)
		errs <- fmt.Errorf("upload: %w", uploadErr)
	} else {
		errs <- nil
	}

	// Wait for all four stage results (upload + three producers).
	var firstErr error
	for i := 0; i < 4; i++ {
		if e := <-errs; e != nil && firstErr == nil {
			firstErr = e
		}
	}
	if firstErr != nil {
		return nil, firstErr
	}

	info := <-dumpInfoCh

	s3key := req.UploadCreds.FinalS3Key
	completed = &backupv1.BackupCompleted{
		JobId:           req.JobId,
		RunId:           req.RunId,
		S3Key:           s3key,
		SizeBytes:       uint64(uploaded),
		Sha256:          sha256hex,
		DurationMs:      uint64(time.Since(start).Milliseconds()),
		EncryptedDek:    req.EncryptedDek, // passed through unchanged
		Compression:     "zstd",
		DbEngineVersion: info.EngineVersion,
	}

	r.logger.Info("backup completed",
		slog.String("job_id", req.JobId),
		slog.String("run_id", req.RunId),
		slog.String("s3_key", s3key),
		slog.Int64("size_bytes", uploaded),
		slog.String("sha256", sha256hex),
		slog.Duration("elapsed", time.Since(start)),
	)
	return completed, nil
}

// resolve looks up the BackupJobSpec and Target for a RunBackup, using
// the optional JobLookup/TargetLookup hooks. If either lookup is nil,
// we still try to drive the pipeline with a synthetic Target derived
// from RunBackup — useful in tests that don't bother to set up lookups.
func (r *Runner) resolve(req *backupv1.RunBackup) (*backupv1.BackupJobSpec, *backupv1.Target, error) {
	var (
		job    *backupv1.BackupJobSpec
		target *backupv1.Target
	)
	if r.jobLookup != nil {
		var ok bool
		job, ok = r.jobLookup.Job(req.JobId)
		if !ok {
			return nil, nil, fmt.Errorf("pipeline: unknown job_id %q", req.JobId)
		}
	}
	if r.targetLookup != nil {
		var ok bool
		if job != nil {
			target, ok = r.targetLookup.Target(job.TargetId)
		}
		if !ok || target == nil {
			return nil, nil, fmt.Errorf("pipeline: unknown target for job %q", req.JobId)
		}
	}
	if target == nil {
		return nil, nil, fmt.Errorf("pipeline: cannot resolve target for job %q (no lookups configured)", req.JobId)
	}
	if target.Connection == nil {
		return nil, nil, errors.New("pipeline: target has no connection config")
	}
	return job, target, nil
}

// dbTypeKey converts the DbType enum to the string key used in the
// Runner's drivers map.
func dbTypeKey(t backupv1.DbType) string {
	switch t {
	case backupv1.DbType_POSTGRESQL:
		return "postgresql"
	case backupv1.DbType_MYSQL:
		return "mysql"
	case backupv1.DbType_MARIADB:
		return "mariadb"
	case backupv1.DbType_MONGODB:
		return "mongodb"
	case backupv1.DbType_REDIS:
		return "redis"
	case backupv1.DbType_SQLITE:
		return "sqlite"
	default:
		return strings.ToLower(t.String())
	}
}

// smokeValidatedReader peeks the first bytes of the dump and validates
// them against the known magic for `driverName`. A validation failure
// is returned immediately; callers should propagate it without reading
// further from the reader. On success the returned io.Reader replays
// the peeked bytes followed by the rest of the underlying stream.
func smokeValidatedReader(r io.Reader, driverName string) (io.Reader, error) {
	br := bufio.NewReaderSize(r, 64)
	switch driverName {
	case "pg_dump":
		head, err := br.Peek(len(PgDumpMagic))
		if err != nil && err != io.EOF {
			return nil, err
		}
		if !IsPgDumpMagic(head) {
			return nil, fmt.Errorf("pg_dump output missing PGDMP magic (got %q)", trimForLog(head))
		}
	case "mysqldump":
		head, err := br.Peek(32)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF && err != bufio.ErrBufferFull {
			return nil, err
		}
		if !IsMysqldumpHeader(head) {
			return nil, fmt.Errorf("mysqldump output missing banner (got %q)", trimForLog(head))
		}
	}
	return br, nil
}

// trimForLog truncates a header for inclusion in error messages.
func trimForLog(b []byte) []byte {
	if len(b) > 32 {
		b = b[:32]
	}
	// Replace control characters so the message is grep-friendly.
	out := make([]byte, len(b))
	for i, c := range b {
		if c < 0x20 || c >= 0x7f {
			out[i] = '.'
		} else {
			out[i] = c
		}
	}
	return bytes.TrimSpace(out)
}

// passthroughDEK is the default DEKResolver — assumes the bytes
// arriving in encrypted_dek are already the 32-byte raw DEK. The
// production wiring will replace this with a KMS-backed resolver.
type passthroughDEK struct{}

func (passthroughDEK) Unwrap(_ context.Context, in []byte) ([]byte, error) {
	if len(in) != dekSize {
		return nil, fmt.Errorf("pipeline: expected %d-byte DEK, got %d", dekSize, len(in))
	}
	out := make([]byte, dekSize)
	copy(out, in)
	return out, nil
}

// wipe zeroes a byte slice. Best-effort — the Go runtime makes no
// guarantee that the underlying memory pages aren't already swapped
// out, but this still raises the bar for casual memory inspection.
func wipe(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
