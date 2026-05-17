package discovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// dockerAPIVersion is the minimum Docker Engine API version the agent
// negotiates with. /v1.41/ corresponds to Engine 20.10 (released 2020-12)
// — every supported host distribution ships at least this version.
const dockerAPIVersion = "v1.41"

// dockerHTTPTimeout caps a single Docker API call. The full Scan calls
// (1 list + N inspects), so we keep each individual request snappy.
const dockerHTTPTimeout = 5 * time.Second

// dbTypeByImagePrefix maps a normalised image basename prefix to the
// DetectedDBType string the rest of the agent uses. Order matters only
// for documentation; lookup is exact-prefix.
var dbTypeByImagePrefix = []struct {
	prefix string
	dbType string
}{
	// Postgres official + common forks.
	{"postgres", "postgresql"},
	{"postgis/postgis", "postgresql"},
	{"timescale/timescaledb", "postgresql"},
	{"bitnami/postgresql", "postgresql"},
	// MySQL (server, percona). Order: mariadb BEFORE mysql so that the
	// `mariadb` image is not swallowed by a `mysql` substring rule.
	{"mariadb", "mariadb"},
	{"bitnami/mariadb", "mariadb"},
	{"mysql", "mysql"},
	{"percona", "mysql"},
	{"bitnami/mysql", "mysql"},
	// MongoDB.
	{"mongo", "mongodb"},
	{"bitnami/mongodb", "mongodb"},
	// Redis.
	{"redis", "redis"},
	{"bitnami/redis", "redis"},
}

// envHintKeysByDBType lists which env-var KEYS are exposed in DiscoveryReport.
// Values stay on host. The intent is to populate the connection form in the
// UI: "this container has POSTGRES_USER set — do you want to pre-fill?".
var envHintKeysByDBType = map[string][]string{
	"postgresql": {"POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB", "POSTGRESQL_USER", "POSTGRESQL_PASSWORD", "POSTGRESQL_DATABASE", "PGUSER", "PGPASSWORD", "PGDATABASE"},
	"mysql":      {"MYSQL_USER", "MYSQL_PASSWORD", "MYSQL_ROOT_PASSWORD", "MYSQL_DATABASE", "MYSQL_ALLOW_EMPTY_PASSWORD"},
	"mariadb":    {"MYSQL_USER", "MYSQL_PASSWORD", "MYSQL_ROOT_PASSWORD", "MYSQL_DATABASE", "MARIADB_USER", "MARIADB_PASSWORD", "MARIADB_ROOT_PASSWORD", "MARIADB_DATABASE"},
	"mongodb":    {"MONGO_INITDB_ROOT_USERNAME", "MONGO_INITDB_ROOT_PASSWORD", "MONGO_INITDB_DATABASE"},
	"redis":      {"REDIS_PASSWORD", "REDIS_USERNAME"},
}

// envHintAllowSet collapses envHintKeysByDBType into a single membership
// set used by filterEnv — every key in this set is allowed across all
// containers, but the value is replaced with "set" sentinel.
var envHintAllowSet = func() map[string]struct{} {
	out := make(map[string]struct{})
	for _, ks := range envHintKeysByDBType {
		for _, k := range ks {
			out[k] = struct{}{}
		}
	}
	return out
}()

// dockerScanner is the production Scanner implementation.
type dockerScanner struct {
	httpClient *http.Client
	baseURL    string // e.g. "http://docker/v1.41"
	logger     *slog.Logger
}

func newDockerScanner(socketPath string, logger *slog.Logger) *dockerScanner {
	if logger == nil {
		logger = slog.Default()
	}
	if !strings.HasPrefix(socketPath, "http://") && !strings.HasPrefix(socketPath, "https://") {
		// Treat the path as a unix socket. The HTTP client below dials
		// the file regardless of the host portion of the URL.
		return &dockerScanner{
			httpClient: newUnixHTTPClient(socketPath),
			baseURL:    "http://docker/" + dockerAPIVersion,
			logger:     logger.With(slog.String("component", "discovery")),
		}
	}
	// Test path: socketPath is an http(s):// base URL pointing at a fake
	// daemon. We append the API version segment so production and tests
	// hit identical relative paths.
	base := strings.TrimRight(socketPath, "/")
	return &dockerScanner{
		httpClient: &http.Client{Timeout: dockerHTTPTimeout},
		baseURL:    base + "/" + dockerAPIVersion,
		logger:     logger.With(slog.String("component", "discovery")),
	}
}

// newUnixHTTPClient builds an *http.Client whose transport dials the
// given unix socket on every request. The URL host segment is ignored.
func newUnixHTTPClient(socket string) *http.Client {
	dialer := &net.Dialer{Timeout: 2 * time.Second}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, "unix", socket)
		},
		// Keep the connection pool tiny — discovery runs at most every
		// hour and we don't want lingering connections to a host file.
		MaxIdleConns:        2,
		IdleConnTimeout:     30 * time.Second,
		DisableCompression:  true,
		TLSHandshakeTimeout: time.Second,
	}
	return &http.Client{Transport: transport, Timeout: dockerHTTPTimeout}
}

// dockerListEntry is a partial mirror of the /containers/json response.
// Only the fields we actually need are decoded — the Docker API ships
// dozens of fields we don't care about.
type dockerListEntry struct {
	ID      string `json:"Id"`
	Names   []string
	Image   string
	State   string
	Ports   []struct {
		PrivatePort uint32 `json:"PrivatePort"`
		PublicPort  uint32 `json:"PublicPort"`
		Type        string `json:"Type"`
	}
	NetworkSettings struct {
		Networks map[string]json.RawMessage `json:"Networks"`
	} `json:"NetworkSettings"`
}

// dockerInspectResponse mirrors the small subset of /containers/{id}/json
// we need: container config (env, image) and network settings.
type dockerInspectResponse struct {
	ID     string `json:"Id"`
	Name   string `json:"Name"`
	State  struct {
		Running bool `json:"Running"`
	} `json:"State"`
	Config struct {
		Image string   `json:"Image"`
		Env   []string `json:"Env"`
	} `json:"Config"`
	HostConfig struct {
		NetworkMode string `json:"NetworkMode"`
	} `json:"HostConfig"`
	NetworkSettings struct {
		Networks map[string]json.RawMessage `json:"Networks"`
		Ports    map[string][]struct {
			HostIP   string `json:"HostIp"`
			HostPort string `json:"HostPort"`
		} `json:"Ports"`
	} `json:"NetworkSettings"`
}

// Scan implements Scanner.Scan.
func (s *dockerScanner) Scan(ctx context.Context) ([]DiscoveredContainer, error) {
	list, err := s.listContainers(ctx)
	if err != nil {
		return nil, fmt.Errorf("discovery: list containers: %w", err)
	}
	out := make([]DiscoveredContainer, 0, len(list))
	for _, c := range list {
		dbType := detectDBType(c.Image)
		if dbType == "" {
			continue
		}
		details, err := s.inspectContainer(ctx, c.ID)
		if err != nil {
			s.logger.Warn("discovery: inspect failed", slog.String("container_id", c.ID), slog.Any("err", err))
			continue
		}
		// Only running containers are useful for live discovery.
		if !details.State.Running {
			continue
		}
		out = append(out, buildContainer(c, details, dbType))
	}
	return out, nil
}

// listContainers issues GET /containers/json (running containers only).
func (s *dockerScanner) listContainers(ctx context.Context) ([]dockerListEntry, error) {
	endpoint := s.baseURL + "/containers/json"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build list request: %w", err)
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("docker GET %s: %w", endpoint, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("docker list containers: HTTP %d", resp.StatusCode)
	}
	var entries []dockerListEntry
	if err := json.NewDecoder(resp.Body).Decode(&entries); err != nil {
		return nil, fmt.Errorf("decode list response: %w", err)
	}
	return entries, nil
}

// inspectContainer issues GET /containers/{id}/json.
func (s *dockerScanner) inspectContainer(ctx context.Context, id string) (*dockerInspectResponse, error) {
	endpoint := s.baseURL + "/containers/" + url.PathEscape(id) + "/json"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build inspect request: %w", err)
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("docker GET %s: %w", endpoint, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, errors.New("container not found")
	}
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("docker inspect: HTTP %d", resp.StatusCode)
	}
	var details dockerInspectResponse
	if err := json.NewDecoder(resp.Body).Decode(&details); err != nil {
		return nil, fmt.Errorf("decode inspect response: %w", err)
	}
	return &details, nil
}

// detectDBType applies the dbTypeByImagePrefix table. The image string
// may include a registry, tag, or digest — we strip those before
// matching so "ghcr.io/postgres:16" still resolves to "postgresql".
func detectDBType(image string) string {
	norm := normaliseImage(image)
	for _, rule := range dbTypeByImagePrefix {
		if norm == rule.prefix || strings.HasPrefix(norm, rule.prefix+":") || strings.HasPrefix(norm, rule.prefix+"/") {
			return rule.dbType
		}
		// also match "<prefix><anything>" when prefix is a bare word
		// like "postgres" — e.g. "postgresql" image alias.
		if !strings.Contains(rule.prefix, "/") && strings.HasPrefix(norm, rule.prefix) {
			rest := norm[len(rule.prefix):]
			if rest == "" || rest[0] == ':' || rest[0] == '/' || isAlnumExtension(rule.prefix, rest) {
				return rule.dbType
			}
		}
	}
	return ""
}

// isAlnumExtension allows a small whitelist of suffixes after a bare
// prefix — e.g. "postgresql" after "postgres", or "mysql8" tag-free
// build images. Conservative to avoid false positives like "mysqld-exporter".
func isAlnumExtension(prefix, rest string) bool {
	switch prefix {
	case "postgres":
		return rest == "ql"
	case "mysql":
		// digits only, e.g. "mysql8".
		for _, r := range rest {
			if r < '0' || r > '9' {
				return false
			}
		}
		return rest != ""
	}
	return false
}

// normaliseImage strips registry host, digest, and lowercases the result.
// It keeps the namespace ("bitnami/postgresql") because some rules match
// the namespaced form.
func normaliseImage(image string) string {
	s := strings.ToLower(strings.TrimSpace(image))
	if at := strings.Index(s, "@"); at >= 0 {
		s = s[:at] // drop digest
	}
	// Strip registry host iff the first segment contains a "." or ":"
	// (port). Docker official images on Docker Hub have no host segment.
	if slash := strings.Index(s, "/"); slash > 0 {
		first := s[:slash]
		if strings.ContainsAny(first, ".:") {
			s = s[slash+1:]
		}
	}
	return s
}

// buildContainer projects raw Docker API structs into a DiscoveredContainer.
func buildContainer(list dockerListEntry, det *dockerInspectResponse, dbType string) DiscoveredContainer {
	name := strings.TrimPrefix(det.Name, "/")
	if name == "" && len(list.Names) > 0 {
		name = strings.TrimPrefix(list.Names[0], "/")
	}

	// Networks: prefer inspect response; fall back to list entry.
	networks := make([]string, 0, len(det.NetworkSettings.Networks))
	for n := range det.NetworkSettings.Networks {
		networks = append(networks, n)
	}
	if len(networks) == 0 {
		for n := range list.NetworkSettings.Networks {
			networks = append(networks, n)
		}
	}

	// Ports: build from the inspect response's NetworkSettings.Ports map
	// which carries both exposed and published ports. The list entry's
	// Ports field is also accepted as a fallback.
	ports := portsFromInspect(det.NetworkSettings.Ports)
	if len(ports) == 0 {
		for _, p := range list.Ports {
			ports = append(ports, PortBinding{
				ContainerPort: p.PrivatePort,
				HostPort:      p.PublicPort,
				Protocol:      defaultProto(p.Type),
			})
		}
	}

	return DiscoveredContainer{
		ContainerID:    det.ID,
		Name:           name,
		Image:          det.Config.Image,
		DetectedDBType: dbType,
		Networks:       networks,
		EnvHints:       filterEnv(det.Config.Env),
		Ports:          ports,
	}
}

// portsFromInspect parses the "Ports" map from /containers/{id}/json.
// The keys look like "5432/tcp"; the values are arrays of host bindings
// (one per host interface). A nil/empty bindings array means "exposed
// but not published" — HostPort stays 0.
func portsFromInspect(in map[string][]struct {
	HostIP   string `json:"HostIp"`
	HostPort string `json:"HostPort"`
}) []PortBinding {
	if len(in) == 0 {
		return nil
	}
	out := make([]PortBinding, 0, len(in))
	for key, bindings := range in {
		port, proto := parsePortKey(key)
		if port == 0 {
			continue
		}
		if len(bindings) == 0 {
			out = append(out, PortBinding{ContainerPort: port, Protocol: proto})
			continue
		}
		for _, b := range bindings {
			out = append(out, PortBinding{
				ContainerPort: port,
				HostPort:      parsePort(b.HostPort),
				Protocol:      proto,
			})
		}
	}
	return out
}

// parsePortKey splits "5432/tcp" → (5432, "tcp"). Defaults protocol to "tcp".
func parsePortKey(key string) (uint32, string) {
	slash := strings.Index(key, "/")
	if slash < 0 {
		return parsePort(key), "tcp"
	}
	return parsePort(key[:slash]), defaultProto(key[slash+1:])
}

func parsePort(s string) uint32 {
	if s == "" {
		return 0
	}
	var v uint32
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0
		}
		v = v*10 + uint32(r-'0')
		if v > 65535 {
			return 0
		}
	}
	return v
}

func defaultProto(p string) string {
	p = strings.ToLower(strings.TrimSpace(p))
	if p == "" {
		return "tcp"
	}
	return p
}

// filterEnv reads `KEY=VALUE` entries from the container env. For any
// key in the allow-set we emit the key with a sentinel value ("set"), so
// downstream code sees structural presence but never the secret.
//
// Returns an empty map (not nil) when no hints are found, so the proto
// `map<string,string>` always serialises an empty map rather than nil.
func filterEnv(env []string) map[string]string {
	out := make(map[string]string)
	for _, e := range env {
		eq := strings.IndexByte(e, '=')
		if eq <= 0 {
			continue
		}
		key := e[:eq]
		if _, ok := envHintAllowSet[key]; !ok {
			continue
		}
		out[key] = "set"
	}
	return out
}
