// Package config loads agent configuration from environment variables.
//
// The bootstrap surface is intentionally tiny — per docs/03-agent-spec.md
// the only required inputs are the server URL and the agent key. Everything
// else (targets, schedules, S3 creds, etc.) arrives from the server via
// ConfigUpdate after the WSS handshake.
//
// Secrets are tagged `json:"-"` so they never leak through structured
// logging or `agent dump-state` output.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"

	"github.com/caarlos0/env/v11"
)

// agentKeyPattern enforces the documented BACKUPY_AGENT_KEY format
// `bkpy_(live|test)_<32 base62 chars>`. The server issues keys in
// this exact shape — see docs/03-agent-spec.md and server task A-09.
var agentKeyPattern = regexp.MustCompile(`^(?:bkpy_(?:live|test)_[A-Za-z0-9]{32}|[a-f0-9]{64})$`)

// Config holds all agent bootstrap configuration.
type Config struct {
	ServerURL    string `env:"BACKUPY_SERVER_URL,required" envDefault:"https://api.backupy.ru"`
	AgentKey     string `env:"BACKUPY_AGENT_KEY,required"  json:"-"`
	StateDir     string `env:"BACKUPY_STATE_DIR"           envDefault:"/var/lib/backup-agent"`
	LogLevel     string `env:"BACKUPY_LOG_LEVEL"           envDefault:"info"`
	DockerSocket string `env:"BACKUPY_DOCKER_SOCKET"       envDefault:"/var/run/docker.sock"`

	// DevAllowInsecure relaxes the https:// requirement on ServerURL.
	// Intended for local development against a plaintext server only.
	DevAllowInsecure bool `env:"BACKUPY_DEV_ALLOW_INSECURE" envDefault:"false"`

	// MetricsListenAddr is the bind address for the Prometheus
	// `/metrics` endpoint (D-19). Default is loopback only —
	// 127.0.0.1:9090. Set to empty to disable the metrics server.
	// SECURITY: never bind to 0.0.0.0 in production; the endpoint
	// reveals job IDs and run cadence usable for host fingerprinting.
	MetricsListenAddr string `env:"BACKUPY_METRICS_LISTEN_ADDR" envDefault:"127.0.0.1:9090"`
}

// Load parses environment variables into a Config and validates them.
// Missing required vars or malformed values cause Load to return an error
// describing the problem; nothing about secret values is included.
func Load() (*Config, error) {
	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		return nil, fmt.Errorf("config: parse env: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// Validate enforces the documented constraints on each field.
//
//   - ServerURL must parse as an https:// URL (http:// only with
//     BACKUPY_DEV_ALLOW_INSECURE=true).
//   - AgentKey must match the canonical `bkpy_(live|test)_…` pattern.
//   - StateDir must be writable; we test by creating and removing a temp
//     file so a misconfigured volume mount fails fast at startup.
func (c *Config) Validate() error {
	if err := validateServerURL(c.ServerURL, c.DevAllowInsecure); err != nil {
		return err
	}
	if !agentKeyPattern.MatchString(c.AgentKey) {
		return errors.New("config: BACKUPY_AGENT_KEY has invalid format; expected 64 hex chars (or legacy bkpy_(live|test)_<32 alnum>)")
	}
	if err := validateStateDirWritable(c.StateDir); err != nil {
		return err
	}
	return nil
}

func validateServerURL(raw string, allowInsecure bool) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("config: BACKUPY_SERVER_URL is not a valid URL: %w", err)
	}
	if u.Host == "" {
		return errors.New("config: BACKUPY_SERVER_URL is missing host")
	}
	switch u.Scheme {
	case "https":
		return nil
	case "http":
		if !allowInsecure {
			return errors.New("config: BACKUPY_SERVER_URL must use https:// (set BACKUPY_DEV_ALLOW_INSECURE=true for local dev)")
		}
		return nil
	default:
		return fmt.Errorf("config: BACKUPY_SERVER_URL has unsupported scheme %q (expected https)", u.Scheme)
	}
}

func validateStateDirWritable(dir string) error {
	if dir == "" {
		return errors.New("config: BACKUPY_STATE_DIR must not be empty")
	}
	// Ensure the directory exists; create it (and parents) if missing.
	// 0o700 — only the agent UID should ever touch state.
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("config: cannot create state dir %q: %w", dir, err)
	}
	probe, err := os.CreateTemp(dir, ".writable-probe-*")
	if err != nil {
		return fmt.Errorf("config: state dir %q is not writable: %w", dir, err)
	}
	name := probe.Name()
	_ = probe.Close()
	_ = os.Remove(name)
	return nil
}

// StateDBPath returns the absolute path to the BoltDB state file.
func (c *Config) StateDBPath() string {
	return filepath.Join(c.StateDir, "state.db")
}
