// Package version exposes build-time information injected via -ldflags.
//
// Example go build:
//
//	go build -ldflags "
//	  -X github.com/backupy/backupy/apps/agent/internal/version.Version=$(VERSION)
//	  -X github.com/backupy/backupy/apps/agent/internal/version.Commit=$(COMMIT)
//	  -X github.com/backupy/backupy/apps/agent/internal/version.BuildDate=$(DATE)
//	" ./cmd/agent
package version

import "fmt"

// These variables are overwritten by the linker at build time. Defaults
// describe an unstamped local build so `agent version` is always answerable.
var (
	Version   = "dev"
	Commit    = "none"
	BuildDate = "unknown"
)

// Info is an immutable snapshot of build metadata.
type Info struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildDate string `json:"build_date"`
}

// Current returns the current build info.
func Current() Info {
	return Info{
		Version:   Version,
		Commit:    Commit,
		BuildDate: BuildDate,
	}
}

// Full returns a human-readable single-line version string for CLI output.
func Full() string {
	return fmt.Sprintf("%s (commit %s, built %s)", Version, Commit, BuildDate)
}
