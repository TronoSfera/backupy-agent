package pipeline

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"

	backupv1 "github.com/backupy/backupy/packages/proto/gen/go/backupv1"
)

// PgDumpMagic is the magic header pg_dump custom-format archives start
// with — "PGDMP". The smoke validation step (D-08) verifies the first
// five bytes of the dump stream before any uploading.
const PgDumpMagic = "PGDMP"

// pgDump is the PostgreSQL driver implementation.
type pgDump struct {
	// binary is the on-disk pg_dump executable. Tests inject a stub.
	binary string
	// runner abstracts os/exec so the unit tests can mock it.
	runner cmdRunner
}

// NewPgDump constructs the default driver wired to the bundled pg_dump
// binary on $PATH.
func NewPgDump() Driver {
	return &pgDump{binary: "pg_dump", runner: realRunner{}}
}

// Name implements Driver.Name.
func (p *pgDump) Name() string { return "pg_dump" }

// Validate runs `pg_dump --version` and a trivial `psql`-style probe to
// confirm we can reach the target. We deliberately only invoke the
// pg_dump binary itself so the agent does not need to bundle the full
// psql client.
func (p *pgDump) Validate(ctx context.Context, target *backupv1.Target) error {
	if target == nil || target.Connection == nil {
		return errors.New("pipeline: pg_dump: nil target/connection")
	}
	// `pg_dump --version` returns immediately and exits 0 if the binary
	// is present. We use it as a cheap "binary installed" check.
	versionOut, err := p.runner.Output(ctx, p.binary, []string{"--version"}, nil)
	if err != nil {
		return fmt.Errorf("pipeline: pg_dump version probe failed: %w", err)
	}
	if !strings.Contains(strings.ToLower(string(versionOut)), "pg_dump") {
		return fmt.Errorf("pipeline: unexpected pg_dump --version output: %q", string(versionOut))
	}
	// A schema-only dump piped to /dev/null is a reasonable smoke test:
	// it actually opens a connection but transfers almost no data.
	args := append(p.connArgs(target), "--schema-only", "--no-acl", "--no-owner")
	if _, err := p.runner.Output(ctx, p.binary, args, p.env(target)); err != nil {
		return fmt.Errorf("pipeline: pg_dump smoke probe failed: %w", err)
	}
	return nil
}

// Dump streams a custom-format pg_dump archive to `out`.
func (p *pgDump) Dump(ctx context.Context, target *backupv1.Target, out io.Writer) (DumpInfo, error) {
	if target == nil || target.Connection == nil {
		return DumpInfo{}, errors.New("pipeline: pg_dump: nil target/connection")
	}
	args := append(p.connArgs(target),
		"--format=custom",
		"--no-owner",
		"--no-acl",
		"--serializable-deferrable",
		"--no-comments",
	)
	if err := p.runner.RunStream(ctx, p.binary, args, p.env(target), out); err != nil {
		return DumpInfo{}, fmt.Errorf("pipeline: pg_dump exec: %w", err)
	}
	versionOut, vErr := p.runner.Output(ctx, p.binary, []string{"--version"}, nil)
	engineVersion := "PostgreSQL"
	if vErr == nil {
		engineVersion = parsePgDumpVersion(string(versionOut))
	}
	return DumpInfo{EngineVersion: engineVersion}, nil
}

// connArgs builds the host/port/user/db flag tuple shared by Validate and Dump.
func (p *pgDump) connArgs(t *backupv1.Target) []string {
	c := t.Connection
	args := []string{}
	if c.Host != "" {
		args = append(args, "-h", c.Host)
	}
	if c.Port != 0 {
		args = append(args, "-p", strconv.FormatUint(uint64(c.Port), 10))
	}
	if c.Username != "" {
		args = append(args, "-U", c.Username)
	}
	if c.Database != "" {
		args = append(args, "-d", c.Database)
	}
	return args
}

// env returns the environment for the child process — specifically
// PGPASSWORD so the password is never visible on the command line.
//
// password_secret_ref is the server-side reference; by the time we get
// here the WSS layer has already resolved it to the actual secret. To
// keep this package agnostic we read it back from a connection field
// named password_secret_ref interpreted literally as the password value
// (the agent stores resolved secrets there for the duration of one run).
func (p *pgDump) env(t *backupv1.Target) []string {
	if t.Connection == nil || t.Connection.PasswordSecretRef == "" {
		return nil
	}
	return []string{"PGPASSWORD=" + t.Connection.PasswordSecretRef}
}

// parsePgDumpVersion converts "pg_dump (PostgreSQL) 16.2" to
// "PostgreSQL 16.2", the canonical engine_version string.
func parsePgDumpVersion(s string) string {
	s = strings.TrimSpace(s)
	// e.g. "pg_dump (PostgreSQL) 16.2"
	if i := strings.Index(s, "(PostgreSQL)"); i >= 0 {
		rest := strings.TrimSpace(s[i+len("(PostgreSQL)"):])
		return "PostgreSQL " + rest
	}
	return s
}

// IsPgDumpMagic returns true if `head` starts with the pg_dump custom
// archive magic. Used by smoke-validation.
func IsPgDumpMagic(head []byte) bool {
	return bytes.HasPrefix(head, []byte(PgDumpMagic))
}

// -----------------------------------------------------------------------------
// cmd runner abstraction (shared with mysqldump.go)
// -----------------------------------------------------------------------------

// cmdRunner allows tests to swap out os/exec with deterministic stubs.
type cmdRunner interface {
	// Output runs cmd+args with env, returns combined stdout, or an
	// error. Used for short-lived commands like --version probes.
	Output(ctx context.Context, name string, args []string, env []string) ([]byte, error)
	// RunStream runs cmd+args with env and pipes stdout to `out`.
	// stderr is captured into the returned error on non-zero exit.
	RunStream(ctx context.Context, name string, args []string, env []string, out io.Writer) error
}

// realRunner is the production cmdRunner backed by os/exec.
type realRunner struct{}

func (realRunner) Output(ctx context.Context, name string, args []string, env []string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = mergeEnv(env)
	out, err := cmd.Output()
	if err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			return out, fmt.Errorf("%s exited %d: %s", name, ee.ExitCode(), bytes.TrimSpace(ee.Stderr))
		}
		return out, err
	}
	return out, nil
}

func (realRunner) RunStream(ctx context.Context, name string, args []string, env []string, out io.Writer) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = mergeEnv(env)
	cmd.Stdout = out
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s: %w (stderr=%s)", name, err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

// mergeEnv inherits the parent process environment and overlays the
// supplied entries on top. Returning nil keeps Go's default (inherit
// everything) when callers pass no overrides.
func mergeEnv(extra []string) []string {
	if len(extra) == 0 {
		return nil
	}
	base := os.Environ()
	out := make([]string, 0, len(base)+len(extra))
	out = append(out, base...)
	out = append(out, extra...)
	return out
}
