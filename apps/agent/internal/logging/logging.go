// Package logging configures the agent's structured logger built on
// the standard library's log/slog package.
//
// Output is always JSON on stdout (per docs/03-agent-spec.md — agent logs
// are eventually streamed to the server, so structured form is mandatory).
// The dev profile lowers verbosity by disabling source positions.
//
// BACKUPY_AGENT_KEY is never logged — see config.Config which tags it
// `json:"-"` and the redactKey helper here for defence-in-depth.
package logging

import (
	"io"
	"log/slog"
	"os"
	"strings"
)

// New returns a configured *slog.Logger writing JSON to stdout.
func New(level string) *slog.Logger {
	return NewWithWriter(os.Stdout, level)
}

// NewWithWriter is the same as New but writes to a caller-supplied io.Writer.
// Useful for tests.
func NewWithWriter(w io.Writer, level string) *slog.Logger {
	opts := &slog.HandlerOptions{
		Level:       parseLevel(level),
		ReplaceAttr: redactSecrets,
	}
	return slog.New(slog.NewJSONHandler(w, opts))
}

// parseLevel converts a textual log level to slog.Level. Unknown levels
// default to info so a typo never silently silences the logger.
func parseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "trace", "debug":
		return slog.LevelDebug
	case "info", "":
		return slog.LevelInfo
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// redactSecrets is a slog.HandlerOptions.ReplaceAttr hook that masks any
// attribute named like a secret. Defence-in-depth — keys should never be
// passed into logs in the first place, but if a caller slips up the value
// is replaced before serialisation.
func redactSecrets(_ []string, a slog.Attr) slog.Attr {
	key := strings.ToLower(a.Key)
	switch key {
	case "agent_key", "backup_agent_key", "password", "secret", "token", "authorization":
		return slog.String(a.Key, "***")
	}
	return a
}
