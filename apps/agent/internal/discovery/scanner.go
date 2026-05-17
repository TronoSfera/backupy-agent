package discovery

import (
	"context"
	"log/slog"

	agentproto "github.com/backupy/backupy/apps/agent/internal/proto"
	backupv1 "github.com/backupy/backupy/packages/proto/gen/go/backupv1"
)

// DiscoveredContainer is the in-process representation of one discovered
// database candidate. Field names track the proto DiscoveredContainer
// message (see packages/proto/v1/agent_to_server.proto).
//
// Container is kept as an alias for backwards compatibility with the
// pre-D-05 skeleton — callers that already imported discovery.Container
// keep compiling.
type DiscoveredContainer struct {
	ContainerID    string
	Name           string
	Image          string
	DetectedDBType string // "postgresql" | "mysql" | "mariadb" | "mongodb" | "redis"
	Networks       []string
	EnvHints       map[string]string // env-var KEYS only — values stay on host
	Ports          []PortBinding
}

// Container is an alias retained for callers from the pre-D-05 skeleton.
// New code should use DiscoveredContainer.
type Container = DiscoveredContainer

// PortBinding describes a single container port (optionally) published
// to the host. ContainerPort is always populated; HostPort is 0 when the
// port is exposed but not published.
type PortBinding struct {
	ContainerPort uint32
	HostPort      uint32
	Protocol      string
}

// Scanner runs container discovery against the Docker daemon.
type Scanner interface {
	// Scan returns all currently-detected database containers. The
	// returned slice is empty (not nil) when no containers match.
	Scan(ctx context.Context) ([]DiscoveredContainer, error)
}

// Config configures a Scanner constructed via NewScanner. Kept for
// backwards compatibility — new callers use NewDockerScanner directly.
type Config struct {
	DockerSocket string
	Logger       *slog.Logger
}

// NewScanner returns the default Docker-socket-backed Scanner using the
// given configuration. The logger is optional; if nil, slog.Default() is
// used.
func NewScanner(cfg Config) Scanner {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	socket := cfg.DockerSocket
	if socket == "" {
		socket = "/var/run/docker.sock"
	}
	return newDockerScanner(socket, logger)
}

// NewDockerScanner returns a Scanner that talks to the Docker daemon at
// the unix socket path provided. Useful for unit tests that swap in a
// test HTTP server (see docker_test.go).
func NewDockerScanner(socketPath string) Scanner {
	return newDockerScanner(socketPath, slog.Default())
}

// BuildReport projects a slice of discovered containers onto the proto
// DiscoveryReport message so the WSS client can send it without knowing
// about the Container struct.
func BuildReport(containers []DiscoveredContainer) *agentproto.DiscoveryReport {
	report := &agentproto.DiscoveryReport{
		Containers: make([]*backupv1.DiscoveredContainer, 0, len(containers)),
	}
	for _, c := range containers {
		ports := make([]*backupv1.PortBinding, 0, len(c.Ports))
		for _, p := range c.Ports {
			ports = append(ports, &backupv1.PortBinding{
				ContainerPort: p.ContainerPort,
				HostPort:      p.HostPort,
				Protocol:      p.Protocol,
			})
		}
		// Defensive copy of env hints so subsequent mutation of the
		// source map cannot leak into the serialised message.
		hints := make(map[string]string, len(c.EnvHints))
		for k, v := range c.EnvHints {
			hints[k] = v
		}
		report.Containers = append(report.Containers, &backupv1.DiscoveredContainer{
			ContainerId:    c.ContainerID,
			Name:           c.Name,
			Image:          c.Image,
			DetectedDbType: c.DetectedDBType,
			Networks:       append([]string(nil), c.Networks...),
			EnvHints:       hints,
			Ports:          ports,
		})
	}
	return report
}
