package logging

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseLevel(t *testing.T) {
	tests := []struct {
		in   string
		want slog.Level
	}{
		{"debug", slog.LevelDebug},
		{"trace", slog.LevelDebug},
		{"info", slog.LevelInfo},
		{"INFO", slog.LevelInfo},
		{"warn", slog.LevelWarn},
		{"warning", slog.LevelWarn},
		{"error", slog.LevelError},
		{"", slog.LevelInfo},
		{"bogus", slog.LevelInfo},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			require.Equal(t, tt.want, parseLevel(tt.in))
		})
	}
}

func TestRedactSecrets(t *testing.T) {
	var buf bytes.Buffer
	log := NewWithWriter(&buf, "info")
	log.Info("test", slog.String("agent_key", "bkpy_live_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
		slog.String("password", "hunter2"),
		slog.String("user", "alice"))

	var got map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &got))
	require.Equal(t, "***", got["agent_key"])
	require.Equal(t, "***", got["password"])
	require.Equal(t, "alice", got["user"])
}

func TestNew_EmitsJSON(t *testing.T) {
	var buf bytes.Buffer
	log := NewWithWriter(&buf, "info")
	log.Info("hello", slog.String("k", "v"))
	require.True(t, strings.HasPrefix(strings.TrimSpace(buf.String()), "{"), "expected JSON output")
}
