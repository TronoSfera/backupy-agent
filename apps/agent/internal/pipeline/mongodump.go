// B14: MongoDB driver.
//
// Spawns `mongodump --archive --gzip` and streams the resulting archive
// to the pipeline writer. The archive is a single binary blob containing
// every database (or one when target.Connection.Database is set), so
// downstream stages (zstd compress, AES-GCM encrypt, upload) can treat
// it the same way they treat pg_dump's custom-format archive.
//
// We deliberately shell out instead of importing the official Mongo Go
// driver: the binary handles oplog tailing, BSON encoding and resume
// semantics already, and keeping the agent's go.mod minimal matters.
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"

	backupv1 "github.com/backupy/backupy/packages/proto/gen/go/backupv1"
)

// mongoDump is the MongoDB driver implementation.
type mongoDump struct {
	binary string
	runner cmdRunner
}

// NewMongoDump constructs the default driver wired to the bundled
// mongodump binary on $PATH.
func NewMongoDump() Driver {
	return &mongoDump{binary: "mongodump", runner: realRunner{}}
}

// Name implements Driver.Name.
func (m *mongoDump) Name() string { return "mongodump" }

// Validate verifies the mongodump binary is installed and that the
// configured connection answers a quick ping. We use mongodump's own
// `--version` + a no-op archive write to /dev/null with `--dbpath` skipped
// — instead the cheapest reachability test is just `--version`. A full
// connection probe is left to the Dump call to avoid double round-trips
// on small databases.
func (m *mongoDump) Validate(ctx context.Context, target *backupv1.Target) error {
	if target == nil || target.Connection == nil {
		return errors.New("pipeline: mongodump: nil target/connection")
	}
	out, err := m.runner.Output(ctx, m.binary, []string{"--version"}, nil)
	if err != nil {
		return fmt.Errorf("pipeline: mongodump version probe failed (is mongodump installed?): %w", err)
	}
	if !strings.Contains(strings.ToLower(string(out)), "mongodump") {
		return fmt.Errorf("pipeline: unexpected mongodump --version output: %q", string(out))
	}
	return nil
}

// Dump streams a `mongodump --archive --gzip` binary archive to out.
// We do NOT layer mongodump's own gzip on top of zstd — passing --gzip
// here keeps the archive self-describing and lets restore reach for the
// canonical `mongorestore --gzip --archive=…` workflow. The outer zstd
// stage will still compress the gzip stream (gain is marginal but the
// uniform pipeline shape outweighs the cost).
func (m *mongoDump) Dump(ctx context.Context, target *backupv1.Target, out io.Writer) (DumpInfo, error) {
	if target == nil || target.Connection == nil {
		return DumpInfo{}, errors.New("pipeline: mongodump: nil target/connection")
	}
	args := append(m.connArgs(target), "--archive", "--gzip")
	if err := m.runner.RunStream(ctx, m.binary, args, nil, out); err != nil {
		return DumpInfo{}, fmt.Errorf("pipeline: mongodump exec: %w", err)
	}
	versionOut, vErr := m.runner.Output(ctx, m.binary, []string{"--version"}, nil)
	engineVersion := "MongoDB"
	if vErr == nil {
		engineVersion = parseMongodumpVersion(string(versionOut))
	}
	return DumpInfo{EngineVersion: engineVersion}, nil
}

// connArgs builds the host/port/user/uri flag tuple. If a URI is
// embedded in the host field (mongodb://…) we pass it via --uri,
// otherwise we use --host/--port/--username and rely on
// $MONGODB_PASSWORD via env (set by the runner via password_secret_ref).
func (m *mongoDump) connArgs(t *backupv1.Target) []string {
	c := t.Connection
	args := []string{}

	if strings.HasPrefix(c.Host, "mongodb://") || strings.HasPrefix(c.Host, "mongodb+srv://") {
		args = append(args, "--uri", c.Host)
	} else {
		if c.Host != "" {
			args = append(args, "--host", c.Host)
		}
		if c.Port != 0 {
			args = append(args, "--port", strconv.FormatUint(uint64(c.Port), 10))
		}
		if c.Username != "" {
			args = append(args, "--username", c.Username)
		}
		if c.PasswordSecretRef != "" {
			// mongodump accepts --password inline. We pass it as an
			// argument because mongodump does not honour environment
			// variables for the password.
			args = append(args, "--password", c.PasswordSecretRef)
			args = append(args, "--authenticationDatabase", "admin")
		}
	}

	if c.Database != "" {
		args = append(args, "--db", c.Database)
	}
	return args
}

// parseMongodumpVersion extracts the version line from mongodump
// --version output:
//
//	"mongodump version: 100.9.5"
//	-> "MongoDB Tools 100.9.5"
func parseMongodumpVersion(s string) string {
	s = strings.TrimSpace(s)
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(strings.ToLower(line), "mongodump version:") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 {
				return "MongoDB Tools " + strings.TrimSpace(parts[1])
			}
		}
	}
	return "MongoDB"
}

// IsMongodumpArchiveMagic reports whether head starts with mongodump's
// archive magic number. The mongo archive format begins with the
// little-endian uint32 0x8199e26d. With --gzip the outer stream is gzip
// (magic 0x1f 0x8b) so we accept either.
func IsMongodumpArchiveMagic(head []byte) bool {
	if len(head) >= 2 && head[0] == 0x1f && head[1] == 0x8b {
		return true // gzip
	}
	if len(head) >= 4 && head[0] == 0x6d && head[1] == 0xe2 && head[2] == 0x99 && head[3] == 0x81 {
		return true // raw mongo archive (little-endian 0x8199e26d)
	}
	return false
}

// -----------------------------------------------------------------------------
// streaming runner with stderr ring buffer + ctx-cancel friendliness.
// Used by the mongo, redis and sqlite drivers — kept here so we do not
// disturb the existing realRunner that pg_dump / mysqldump rely on.
// -----------------------------------------------------------------------------

// ringBuffer is a fixed-capacity tail buffer for stderr. It always
// retains the LAST `cap` bytes written to it, dropping earlier writes.
// Concurrent Write calls are guarded by mu.
type ringBuffer struct {
	mu  sync.Mutex
	buf []byte
	cap int
}

func newRingBuffer(cap int) *ringBuffer { return &ringBuffer{cap: cap} }

func (r *ringBuffer) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.buf = append(r.buf, p...)
	if len(r.buf) > r.cap {
		r.buf = r.buf[len(r.buf)-r.cap:]
	}
	return len(p), nil
}

func (r *ringBuffer) Tail() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return strings.TrimSpace(string(r.buf))
}

// streamWithCancel runs a child process, copying its stdout to out and
// capturing the last 4 KB of stderr. On ctx.Done() it sends SIGINT to
// the child (rather than the default SIGKILL) so mongodump / redis-cli
// / sqlite3 get a chance to flush partial output and exit cleanly.
func streamWithCancel(ctx context.Context, name string, args []string, env []string, out io.Writer) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = mergeEnv(env)
	cmd.Stdout = out
	stderrRing := newRingBuffer(4 * 1024)
	cmd.Stderr = stderrRing
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGINT) }

	if err := cmd.Run(); err != nil {
		var ee *exec.ExitError
		tail := stderrRing.Tail()
		if errors.As(err, &ee) {
			return fmt.Errorf("%s exited %d: %w (stderr: %s)", name, ee.ExitCode(), err, tail)
		}
		return fmt.Errorf("%s: %w (stderr: %s)", name, err, tail)
	}
	return nil
}

// streamingRunner is a cmdRunner that uses streamWithCancel for
// RunStream. Its Output method behaves like realRunner.Output.
type streamingRunner struct{}

func (streamingRunner) Output(ctx context.Context, name string, args []string, env []string) ([]byte, error) {
	return realRunner{}.Output(ctx, name, args, env)
}

func (streamingRunner) RunStream(ctx context.Context, name string, args []string, env []string, out io.Writer) error {
	return streamWithCancel(ctx, name, args, env, out)
}
