package pipeline

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	backupv1 "github.com/backupy/backupy/packages/proto/gen/go/backupv1"
)

// mysqldump implements Driver against the bundled mysqldump binary for
// both MySQL and MariaDB targets.
type mysqldump struct {
	binary string
	runner cmdRunner
}

// NewMysqldump constructs the default driver wired to the bundled
// mysqldump binary on $PATH.
func NewMysqldump() Driver {
	return &mysqldump{binary: "mysqldump", runner: realRunner{}}
}

// Name implements Driver.Name.
func (m *mysqldump) Name() string { return "mysqldump" }

// Validate runs --version and a no-data smoke dump (`--no-data --no-create-info`)
// which contacts the server but emits almost nothing.
func (m *mysqldump) Validate(ctx context.Context, target *backupv1.Target) error {
	if target == nil || target.Connection == nil {
		return errors.New("pipeline: mysqldump: nil target/connection")
	}
	versionOut, err := m.runner.Output(ctx, m.binary, []string{"--version"}, nil)
	if err != nil {
		return fmt.Errorf("pipeline: mysqldump version probe failed: %w", err)
	}
	if !strings.Contains(strings.ToLower(string(versionOut)), "mysqldump") {
		return fmt.Errorf("pipeline: unexpected mysqldump --version output: %q", string(versionOut))
	}
	args := append(m.connArgs(target), "--no-data", "--no-create-info", "--skip-triggers", "--skip-comments")
	if _, err := m.runner.Output(ctx, m.binary, args, nil); err != nil {
		return fmt.Errorf("pipeline: mysqldump smoke probe failed: %w", err)
	}
	return nil
}

// Dump streams a logical mysqldump SQL stream to `out`.
func (m *mysqldump) Dump(ctx context.Context, target *backupv1.Target, out io.Writer) (DumpInfo, error) {
	if target == nil || target.Connection == nil {
		return DumpInfo{}, errors.New("pipeline: mysqldump: nil target/connection")
	}
	args := append(m.connArgs(target),
		"--single-transaction",
		"--routines",
		"--triggers",
		"--events",
		"--quick",
		"--hex-blob",
		"--skip-extended-insert",
	)
	if err := m.runner.RunStream(ctx, m.binary, args, nil, out); err != nil {
		return DumpInfo{}, fmt.Errorf("pipeline: mysqldump exec: %w", err)
	}
	versionOut, vErr := m.runner.Output(ctx, m.binary, []string{"--version"}, nil)
	engineVersion := "MySQL"
	if vErr == nil {
		engineVersion = parseMysqldumpVersion(string(versionOut))
	}
	return DumpInfo{EngineVersion: engineVersion}, nil
}

// connArgs assembles host/port/user/password/db flags. We pass the
// password inline (`--password=…`) because mysqldump does not accept
// it via env; the password never appears in process listings as long
// as the host is configured with `hidepid=2` or similar — but to be
// safe, callers should treat exec.Cmd argv as sensitive.
func (m *mysqldump) connArgs(t *backupv1.Target) []string {
	c := t.Connection
	args := []string{}
	if c.Host != "" {
		args = append(args, "-h", c.Host)
	}
	if c.Port != 0 {
		args = append(args, "-P", strconv.FormatUint(uint64(c.Port), 10))
	}
	if c.Username != "" {
		args = append(args, "-u", c.Username)
	}
	if c.PasswordSecretRef != "" {
		args = append(args, "--password="+c.PasswordSecretRef)
	}
	if c.Database != "" {
		args = append(args, c.Database)
	}
	return args
}

// parseMysqldumpVersion converts the human-readable --version banner
// into a canonical "MySQL 8.0.36" / "MariaDB 11.2.3" string.
//
// Examples:
//
//	"mysqldump  Ver 8.0.36 for Linux on x86_64 (MySQL Community Server - GPL)"
//	-> "MySQL 8.0.36"
//
//	"mysqldump from 11.2.3-MariaDB, client 10.19 …"
//	-> "MariaDB 11.2.3"
func parseMysqldumpVersion(s string) string {
	s = strings.TrimSpace(s)
	low := strings.ToLower(s)
	if i := strings.Index(low, "ver "); i >= 0 {
		rest := s[i+len("Ver "):]
		v := strings.Fields(rest)
		if len(v) > 0 {
			return "MySQL " + v[0]
		}
	}
	if i := strings.Index(low, "mariadb"); i >= 0 {
		// look back for a version-like token
		fields := strings.FieldsFunc(s, func(r rune) bool {
			return r == ' ' || r == ',' || r == '\t'
		})
		for _, f := range fields {
			if strings.Contains(strings.ToLower(f), "mariadb") {
				v := strings.Split(f, "-")
				if len(v) >= 1 {
					return "MariaDB " + v[0]
				}
			}
		}
		_ = i
	}
	return s
}

// IsMysqldumpHeader returns true if `head` looks like the start of a
// mysqldump SQL stream. mysqldump traditionally starts with a banner
// comment like "-- MySQL dump" or "-- MariaDB dump".
func IsMysqldumpHeader(head []byte) bool {
	if len(head) == 0 {
		return false
	}
	low := bytes.ToLower(head)
	return bytes.HasPrefix(low, []byte("-- mysql dump")) ||
		bytes.HasPrefix(low, []byte("-- mariadb dump")) ||
		bytes.HasPrefix(low, []byte("-- mysqldump")) ||
		bytes.HasPrefix(low, []byte("-- mariadb-dump"))
}
