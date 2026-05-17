// Command agent is the Backupy agent — connects to the control plane,
// runs backups, and reports status. See docs/03-agent-spec.md.
//
// Subcommands (per spec §3.7):
//
//	agent run            — start the service loop (default)
//	agent version        — print build metadata
//	agent health-check   — used as Docker HEALTHCHECK; exits 0 when healthy
//	agent dump-state     — debug-only state dump as JSON
//
// `agent self-update` is documented in the spec but landing in task D-15.
package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/spf13/cobra"

	"github.com/backupy/backupy/apps/agent/internal/version"
)

func main() {
	root := newRootCmd()
	if err := root.Execute(); err != nil {
		// Cobra has already printed the error; this final line guarantees
		// a non-zero exit code for environments that ignore Execute's err.
		fmt.Fprintln(os.Stderr, "agent: fatal:", err)
		os.Exit(1)
	}
}

// newRootCmd wires up all subcommands. Exported as a factory so tests
// (and future `agent_test.go`) can construct an isolated root command.
func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:           "agent",
		Short:         "Backupy agent — connects to the platform and runs backups",
		Long:          "Backupy agent service. See docs/03-agent-spec.md for the full spec.",
		Version:       version.Full(),
		SilenceUsage:  true,
		SilenceErrors: false,
	}

	// When invoked with no subcommand, default to `run`. This makes the
	// Docker ENTRYPOINT clean: ["/usr/local/bin/agent"] just works.
	root.AddCommand(newRunCmd())
	root.AddCommand(newVersionCmd())
	root.AddCommand(newHealthCheckCmd())
	root.AddCommand(newDumpStateCmd())

	// Default to `run` when no args given (Docker CMD convention).
	root.RunE = func(cmd *cobra.Command, args []string) error {
		if len(args) > 0 {
			return fmt.Errorf("unknown command %q", args[0])
		}
		return newRunCmd().RunE(cmd, args)
	}

	return root
}

// fatal logs at error level and exits non-zero. Used by subcommands that
// want a uniform exit path after a structured log line.
func fatal(logger *slog.Logger, msg string, err error) {
	if logger == nil {
		logger = slog.Default()
	}
	logger.Error(msg, slog.Any("err", err))
	os.Exit(1)
}
