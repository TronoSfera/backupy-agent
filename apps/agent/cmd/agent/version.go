package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/backupy/backupy/apps/agent/internal/version"
)

func newVersionCmd() *cobra.Command {
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Print version, commit and build date",
		RunE: func(cmd *cobra.Command, args []string) error {
			info := version.Current()
			if asJSON {
				enc := json.NewEncoder(os.Stdout)
				enc.SetIndent("", "  ")
				return enc.Encode(info)
			}
			fmt.Printf("backupy-agent %s\n", version.Full())
			return nil
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "emit version info as JSON")
	return cmd
}
