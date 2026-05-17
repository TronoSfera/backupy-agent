// B14: Redis driver.
//
// MVP strategy — same-host snapshot:
//
//  1. Run `BGSAVE` via `redis-cli`.
//  2. Poll `LASTSAVE` until the timestamp advances, confirming a fresh
//     snapshot was written.
//  3. Locate the on-disk RDB file using `CONFIG GET dir` +
//     `CONFIG GET dbfilename`, open it and stream its bytes to the
//     pipeline writer wrapped in a tar+gzip envelope (so restore can
//     un-tar one logical "dump.rdb" entry regardless of the on-disk
//     filename).
//
// Limitation: this driver REQUIRES the agent to share the host's
// filesystem with the Redis server (or have the dump dir bind-mounted
// in). For network-only Redis we surface a clear error pointing operators
// at the on-host agent pattern. A future iteration may add `--rdb` over
// `redis-cli --rdb -` which streams over the wire — but that path is
// known to lock the master and was deferred.
package pipeline

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	backupv1 "github.com/backupy/backupy/packages/proto/gen/go/backupv1"
)

// redisDriver implements Driver against the redis-cli binary plus
// filesystem access to the running Redis dump directory.
type redisDriver struct {
	binary string
	runner cmdRunner
	// fileOpen is overridable for tests so we can inject a fake dump.rdb.
	fileOpen func(path string) (io.ReadCloser, os.FileInfo, error)
	// now is overridable so tests can fast-forward poll iterations.
	now func() time.Time
	// pollInterval defaults to 250 ms; tests set it to 1 ms.
	pollInterval time.Duration
	// pollTimeout caps the BGSAVE wait. Defaults to 5 minutes.
	pollTimeout time.Duration
}

// NewRedisDriver constructs the default driver wired to the bundled
// redis-cli binary on $PATH.
func NewRedisDriver() Driver {
	return &redisDriver{
		binary:       "redis-cli",
		runner:       realRunner{},
		fileOpen:     defaultFileOpen,
		now:          time.Now,
		pollInterval: 250 * time.Millisecond,
		pollTimeout:  5 * time.Minute,
	}
}

func defaultFileOpen(path string) (io.ReadCloser, os.FileInfo, error) {
	f, err := os.Open(path) // #nosec G304 -- path comes from CONFIG GET of trusted local Redis.
	if err != nil {
		return nil, nil, err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	return f, st, nil
}

// Name implements Driver.Name.
func (r *redisDriver) Name() string { return "redis" }

// Validate runs `redis-cli PING` against the configured target and
// verifies the binary is installed. Returns a wrapped error otherwise.
func (r *redisDriver) Validate(ctx context.Context, target *backupv1.Target) error {
	if target == nil || target.Connection == nil {
		return errors.New("pipeline: redis: nil target/connection")
	}
	versionOut, err := r.runner.Output(ctx, r.binary, []string{"--version"}, nil)
	if err != nil {
		return fmt.Errorf("pipeline: redis-cli version probe failed (is redis-cli installed?): %w", err)
	}
	if !strings.Contains(strings.ToLower(string(versionOut)), "redis-cli") {
		return fmt.Errorf("pipeline: unexpected redis-cli --version output: %q", string(versionOut))
	}
	pingArgs := append(r.connArgs(target), "PING")
	out, err := r.runner.Output(ctx, r.binary, pingArgs, nil)
	if err != nil {
		return fmt.Errorf("pipeline: redis PING failed: %w", err)
	}
	if !strings.Contains(strings.ToUpper(string(out)), "PONG") {
		return fmt.Errorf("pipeline: redis PING returned unexpected response: %q", string(out))
	}
	return nil
}

// Dump produces a tar+gzip stream containing a single entry named
// "dump.rdb" sourced from the running Redis instance's snapshot.
//
// Sequence:
//
//	BGSAVE                        // request async snapshot
//	old := LASTSAVE
//	loop until LASTSAVE > old     // wait for it to land
//	dir := CONFIG GET dir
//	file := CONFIG GET dbfilename
//	open dir/file, tar+gzip into out
func (r *redisDriver) Dump(ctx context.Context, target *backupv1.Target, out io.Writer) (DumpInfo, error) {
	if target == nil || target.Connection == nil {
		return DumpInfo{}, errors.New("pipeline: redis: nil target/connection")
	}
	base := r.connArgs(target)

	prevSave, err := r.lastSave(ctx, base)
	if err != nil {
		return DumpInfo{}, err
	}

	if _, err := r.runner.Output(ctx, r.binary, append(base, "BGSAVE"), nil); err != nil {
		return DumpInfo{}, fmt.Errorf("pipeline: redis BGSAVE failed: %w", err)
	}

	if err := r.waitForSave(ctx, base, prevSave); err != nil {
		return DumpInfo{}, err
	}

	dir, err := r.config(ctx, base, "dir")
	if err != nil {
		return DumpInfo{}, fmt.Errorf("pipeline: redis CONFIG GET dir: %w", err)
	}
	name, err := r.config(ctx, base, "dbfilename")
	if err != nil {
		return DumpInfo{}, fmt.Errorf("pipeline: redis CONFIG GET dbfilename: %w", err)
	}
	if dir == "" || name == "" {
		return DumpInfo{}, errors.New("pipeline: redis returned empty dump path; same-host filesystem access required")
	}
	rdbPath := filepath.Join(dir, name)

	src, st, err := r.fileOpen(rdbPath)
	if err != nil {
		return DumpInfo{}, fmt.Errorf("pipeline: redis: cannot read %s (same-host access required): %w", rdbPath, err)
	}
	defer src.Close()

	if err := writeTarGz(out, "dump.rdb", st.Size(), st.ModTime(), src); err != nil {
		return DumpInfo{}, fmt.Errorf("pipeline: redis: write archive: %w", err)
	}

	info := DumpInfo{EngineVersion: r.serverVersion(ctx, base)}
	return info, nil
}

// connArgs assembles the host/port/password/db tuple for redis-cli.
func (r *redisDriver) connArgs(t *backupv1.Target) []string {
	c := t.Connection
	args := []string{}
	if c.Host != "" {
		args = append(args, "-h", c.Host)
	}
	if c.Port != 0 {
		args = append(args, "-p", strconv.FormatUint(uint64(c.Port), 10))
	}
	if c.PasswordSecretRef != "" {
		args = append(args, "-a", c.PasswordSecretRef, "--no-auth-warning")
	}
	if c.Username != "" {
		args = append(args, "--user", c.Username)
	}
	// Database is a numeric index; we set it only if it parses as int.
	if c.Database != "" {
		if _, err := strconv.Atoi(c.Database); err == nil {
			args = append(args, "-n", c.Database)
		}
	}
	return args
}

// lastSave parses the integer Unix timestamp returned by LASTSAVE.
func (r *redisDriver) lastSave(ctx context.Context, base []string) (int64, error) {
	out, err := r.runner.Output(ctx, r.binary, append(base, "LASTSAVE"), nil)
	if err != nil {
		return 0, fmt.Errorf("pipeline: redis LASTSAVE failed: %w", err)
	}
	s := strings.TrimSpace(string(out))
	ts, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("pipeline: redis LASTSAVE returned non-integer %q: %w", s, err)
	}
	return ts, nil
}

// waitForSave polls LASTSAVE until it advances past prev or the timeout
// is hit. Context cancellation is honoured.
func (r *redisDriver) waitForSave(ctx context.Context, base []string, prev int64) error {
	deadline := r.now().Add(r.pollTimeout)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		cur, err := r.lastSave(ctx, base)
		if err != nil {
			return err
		}
		if cur > prev {
			return nil
		}
		if r.now().After(deadline) {
			return fmt.Errorf("pipeline: redis BGSAVE did not complete within %s", r.pollTimeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(r.pollInterval):
		}
	}
}

// config returns the value side of a `CONFIG GET <key>` reply. redis-cli
// prints two lines: the key and the value. We pick the last non-empty
// line for robustness.
func (r *redisDriver) config(ctx context.Context, base []string, key string) (string, error) {
	out, err := r.runner.Output(ctx, r.binary, append(base, "CONFIG", "GET", key), nil)
	if err != nil {
		return "", err
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		v := strings.TrimSpace(lines[i])
		if v != "" && !strings.EqualFold(v, key) {
			return v, nil
		}
	}
	return "", nil
}

// serverVersion best-effort extracts the redis_version field from
// `INFO server`. Empty string on failure so callers can fall back to a
// generic "Redis" label.
func (r *redisDriver) serverVersion(ctx context.Context, base []string) string {
	out, err := r.runner.Output(ctx, r.binary, append(base, "INFO", "server"), nil)
	if err != nil {
		return "Redis"
	}
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "redis_version:") {
			return "Redis " + strings.TrimPrefix(line, "redis_version:")
		}
	}
	return "Redis"
}

// writeTarGz packs `src` of length size into a tar+gzip stream written
// to out, under a single entry named `name`.
func writeTarGz(out io.Writer, name string, size int64, modTime time.Time, src io.Reader) error {
	gz := gzip.NewWriter(out)
	defer gz.Close()
	tw := tar.NewWriter(gz)
	defer tw.Close()

	hdr := &tar.Header{
		Name:    name,
		Mode:    0o600,
		Size:    size,
		ModTime: modTime,
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return fmt.Errorf("tar header: %w", err)
	}
	if _, err := io.Copy(tw, src); err != nil {
		return fmt.Errorf("tar body: %w", err)
	}
	if err := tw.Close(); err != nil {
		return fmt.Errorf("tar close: %w", err)
	}
	if err := gz.Close(); err != nil {
		return fmt.Errorf("gzip close: %w", err)
	}
	return nil
}

// IsRedisTarGzMagic reports whether head looks like the gzip header that
// every redisDriver.Dump stream begins with.
func IsRedisTarGzMagic(head []byte) bool {
	return len(head) >= 2 && head[0] == 0x1f && head[1] == 0x8b
}
