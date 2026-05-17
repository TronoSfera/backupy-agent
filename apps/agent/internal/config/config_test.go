package config

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const validKey = "bkpy_live_abcdefghijklmnopqrstuvwxyz012345"

func TestValidate_Happy(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{
		ServerURL: "https://api.backupy.ru",
		AgentKey:  validKey,
		StateDir:  dir,
		LogLevel:  "info",
	}
	require.NoError(t, cfg.Validate())
	require.Equal(t, filepath.Join(dir, "state.db"), cfg.StateDBPath())
}

func TestValidate_ServerURL(t *testing.T) {
	cases := []struct {
		name   string
		url    string
		insec  bool
		errSub string
	}{
		{"https ok", "https://api.backupy.ru", false, ""},
		{"http rejected by default", "http://localhost:8080", false, "must use https"},
		{"http allowed with dev flag", "http://localhost:8080", true, ""},
		{"ftp rejected", "ftp://example.com", false, "unsupported scheme"},
		{"missing host", "https://", false, "missing host"},
		{"unparsable", "://broken", false, "valid URL"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateServerURL(tc.url, tc.insec)
			if tc.errSub == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errSub)
		})
	}
}

func TestValidate_AgentKey(t *testing.T) {
	dir := t.TempDir()
	cases := []struct {
		name string
		key  string
		ok   bool
	}{
		{"live key", "bkpy_live_abcdefghijklmnopqrstuvwxyz012345", true},
		{"test key", "bkpy_test_abcdefghijklmnopqrstuvwxyz012345", true},
		{"wrong prefix", "bkpy_dev_abcdefghijklmnopqrstuvwxyz012345", false},
		{"too short", "bkpy_live_abc", false},
		{"too long", "bkpy_live_abcdefghijklmnopqrstuvwxyz0123456", false},
		{"invalid char", "bkpy_live_abcdefghijklmnopqrstuvwxyz01234!", false},
		{"empty", "", false},
		{"missing underscore", "bkpylive_abcdefghijklmnopqrstuvwxyz012345", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &Config{
				ServerURL: "https://api.backupy.ru",
				AgentKey:  tc.key,
				StateDir:  dir,
			}
			err := cfg.Validate()
			if tc.ok {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), "BACKUP_AGENT_KEY")
			}
		})
	}
}

func TestValidate_StateDir(t *testing.T) {
	t.Run("empty rejected", func(t *testing.T) {
		err := validateStateDirWritable("")
		require.Error(t, err)
	})
	t.Run("creates missing dir", func(t *testing.T) {
		root := t.TempDir()
		nested := filepath.Join(root, "nested", "state")
		require.NoError(t, validateStateDirWritable(nested))
	})
	t.Run("redaction in errors", func(t *testing.T) {
		// Sanity check: error text never mentions the agent key.
		cfg := &Config{
			ServerURL: "https://api.backupy.ru",
			AgentKey:  validKey,
			StateDir:  "", // invalid
		}
		err := cfg.Validate()
		require.Error(t, err)
		require.False(t, strings.Contains(err.Error(), validKey))
	})
}
