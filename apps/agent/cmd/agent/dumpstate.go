package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/backupy/backupy/apps/agent/internal/config"
	"github.com/backupy/backupy/apps/agent/internal/state"
)

// stateDump is the pretty-printable shape of the agent's state file.
// Binary blobs are hex-encoded so the JSON stays UTF-8 clean.
type stateDump struct {
	Path          string         `json:"path"`
	SessionID     string         `json:"session_id,omitempty"`
	ConfigVersion uint64         `json:"config_version"`
	ConfigBytes   string         `json:"config_bytes_hex,omitempty"`
	QueueDepth    int            `json:"queue_depth"`
	Queue         []dumpedJob    `json:"queue,omitempty"`
	LastHeartbeat int64          `json:"last_heartbeat_ms"`
	Note          string         `json:"note,omitempty"`
	Meta          map[string]any `json:"meta,omitempty"`
}

type dumpedJob struct {
	RunID      string `json:"run_id"`
	PayloadHex string `json:"payload_hex"`
}

func newDumpStateCmd() *cobra.Command {
	var allowSecrets bool
	cmd := &cobra.Command{
		Use:   "dump-state",
		Short: "Debug: print state.db contents as JSON",
		Long: `Dump the contents of /var/lib/backup-agent/state.db as JSON.

Refuses to print decrypted config or job payloads unless --allow-secrets
is passed, since those may include S3 presigned URLs or other sensitive
material received from the server.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.Load()
			if err != nil {
				return err
			}
			s, err := state.Open(cfg.StateDBPath(), state.Options{AgentKey: cfg.AgentKey})
			if err != nil {
				return err
			}
			defer s.Close()

			dump := stateDump{Path: cfg.StateDBPath()}

			if v, raw, err := s.LoadConfig(); err == nil {
				dump.ConfigVersion = v
				if allowSecrets {
					dump.ConfigBytes = hex.EncodeToString(raw)
				} else {
					dump.Note = "config bytes omitted; re-run with --allow-secrets to include"
				}
			}

			if sid, err := s.LoadSession(); err == nil {
				dump.SessionID = sid
			}
			if hb, err := s.LastHeartbeat(); err == nil {
				dump.LastHeartbeat = hb
			}
			if d, err := s.QueueDepth(); err == nil {
				dump.QueueDepth = d
			}

			if allowSecrets {
				if jobs, err := s.DequeueJobs(100); err == nil {
					for _, j := range jobs {
						dump.Queue = append(dump.Queue, dumpedJob{
							RunID:      j.RunID,
							PayloadHex: hex.EncodeToString(j.Payload),
						})
					}
				}
			}

			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			if err := enc.Encode(dump); err != nil {
				return fmt.Errorf("encode: %w", err)
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&allowSecrets, "allow-secrets", false,
		"include decrypted config and job payloads (may leak secrets)")
	return cmd
}
