// Package discovery scans the local Docker socket and reports discovered
// database containers (postgres, mysql, mariadb, mongo, redis).
//
// Implementation notes (D-05):
//
//   - We talk to /var/run/docker.sock directly over HTTP via a custom
//     net.Dialer wired into http.Transport. NO Docker SDK dependency.
//   - Container detection is purely image-name heuristic plus a small set
//     of env-var name hints. We do NOT exfiltrate env values, only the
//     keys — the spec is explicit that plaintext secrets must stay on the
//     host. See docs/03-agent-spec.md → "Auto-discovery".
//   - The returned Container struct mirrors the proto DiscoveredContainer
//     message so callers can map 1:1 without an extra translation layer.
package discovery

// File split:
//   - scanner.go : the Scanner interface, Container/PortBinding types,
//                  NewDockerScanner constructor, BuildReport helper.
//   - docker.go  : the unix-socket HTTP client + Docker API parsing logic.
//
// This file exists so go-doc on the package shows the high-level overview.
