package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/backupy/backupy/apps/agent/internal/config"
	"github.com/backupy/backupy/apps/agent/internal/state"
)

// newHealthCheckCmd implements the binary used by Docker's HEALTHCHECK
// instruction. We deliberately keep this command's surface tiny so the
// distroless image (no shell) can run it directly.
//
// Health criteria:
//
//  1. Required env vars are set (BACKUPY_AGENT_KEY / BACKUPY_SERVER_URL).
//  2. The state.db file can be opened (validates encryption key + on-disk
//     integrity).
//
// In a future iteration we may extend this to query an internal UNIX
// socket served by `agent run` so connection liveness is also reflected.
func newHealthCheckCmd() *cobra.Command {
	return &cobra.Command{
		Use:           "health-check",
		Short:         "Probe used by Docker HEALTHCHECK; exits 0 when healthy",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.Load()
			if err != nil {
				fmt.Fprintln(os.Stderr, "health-check: config:", err)
				os.Exit(1)
			}
			// Try to open the state DB — read+write lock validates that
			// the file isn't corrupt and the key derivation still works.
			s, err := state.Open(cfg.StateDBPath(), state.Options{AgentKey: cfg.AgentKey})
			if err != nil {
				fmt.Fprintln(os.Stderr, "health-check: state:", err)
				os.Exit(1)
			}
			_ = s.Close()
			fmt.Println("ok")
			return nil
		},
	}
}
