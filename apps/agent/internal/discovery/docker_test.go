package discovery

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// fakeDockerServer wires a single httptest.Server that pretends to be the
// Docker Engine HTTP API on the configured API-version prefix.
func fakeDockerServer(t *testing.T, list []map[string]any, inspect map[string]map[string]any) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	listPath := "/" + dockerAPIVersion + "/containers/json"
	mux.HandleFunc(listPath, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(list)
	})
	mux.HandleFunc("/"+dockerAPIVersion+"/containers/", func(w http.ResponseWriter, r *http.Request) {
		// expect path: /vX/containers/{id}/json
		trim := strings.TrimPrefix(r.URL.Path, "/"+dockerAPIVersion+"/containers/")
		id := strings.TrimSuffix(trim, "/json")
		if id == "" {
			http.NotFound(w, r)
			return
		}
		body, ok := inspect[id]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(body)
	})
	return httptest.NewServer(mux)
}

func TestScan_DetectsPostgres(t *testing.T) {
	srv := fakeDockerServer(t,
		[]map[string]any{{
			"Id":    "abc",
			"Names": []string{"/db"},
			"Image": "postgres:16",
			"State": "running",
		}},
		map[string]map[string]any{
			"abc": {
				"Id":    "abc",
				"Name":  "/db",
				"State": map[string]any{"Running": true},
				"Config": map[string]any{
					"Image": "postgres:16",
					"Env":   []string{"POSTGRES_USER=app", "POSTGRES_PASSWORD=hunter2", "POSTGRES_DB=app", "PATH=/usr/bin"},
				},
				"NetworkSettings": map[string]any{
					"Networks": map[string]any{"bridge": map[string]any{}},
					"Ports": map[string]any{
						"5432/tcp": []map[string]any{{"HostIp": "0.0.0.0", "HostPort": "5432"}},
					},
				},
			},
		},
	)
	defer srv.Close()

	s := newDockerScanner(srv.URL, nil)
	got, err := s.Scan(context.Background())
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "postgresql", got[0].DetectedDBType)
	require.Equal(t, "db", got[0].Name)
	require.Contains(t, got[0].EnvHints, "POSTGRES_PASSWORD")
	require.NotContains(t, got[0].EnvHints, "PATH", "PATH should be filtered out")
	// Critically: values must not leak — we replace with "set".
	for _, v := range got[0].EnvHints {
		require.Equal(t, "set", v, "env values must never appear in EnvHints")
	}
	require.Len(t, got[0].Ports, 1)
	require.Equal(t, uint32(5432), got[0].Ports[0].ContainerPort)
	require.Equal(t, uint32(5432), got[0].Ports[0].HostPort)
	require.Equal(t, "tcp", got[0].Ports[0].Protocol)
}

func TestScan_DetectsAllSupportedTypes(t *testing.T) {
	cases := []struct {
		image  string
		dbType string
	}{
		{"postgres:16", "postgresql"},
		{"postgres", "postgresql"},
		{"timescale/timescaledb:latest-pg16", "postgresql"},
		{"mysql:8.0", "mysql"},
		{"percona:8", "mysql"},
		{"mariadb:11", "mariadb"},
		{"mongo:7", "mongodb"},
		{"redis:7-alpine", "redis"},
	}
	list := make([]map[string]any, 0, len(cases))
	inspect := make(map[string]map[string]any, len(cases))
	for i, c := range cases {
		id := "c" + string(rune('a'+i))
		list = append(list, map[string]any{
			"Id": id, "Names": []string{"/" + id}, "Image": c.image, "State": "running",
		})
		inspect[id] = map[string]any{
			"Id":    id,
			"Name":  "/" + id,
			"State": map[string]any{"Running": true},
			"Config": map[string]any{
				"Image": c.image,
				"Env":   []string{},
			},
		}
	}
	srv := fakeDockerServer(t, list, inspect)
	defer srv.Close()

	s := newDockerScanner(srv.URL, nil)
	got, err := s.Scan(context.Background())
	require.NoError(t, err)
	require.Len(t, got, len(cases))

	gotByImage := make(map[string]string)
	for _, c := range got {
		gotByImage[c.Image] = c.DetectedDBType
	}
	for _, c := range cases {
		require.Equal(t, c.dbType, gotByImage[c.image], "image %q", c.image)
	}
}

func TestScan_SkipsNonDBImages(t *testing.T) {
	srv := fakeDockerServer(t,
		[]map[string]any{
			{"Id": "x", "Names": []string{"/web"}, "Image": "nginx:1.27", "State": "running"},
			{"Id": "y", "Names": []string{"/app"}, "Image": "myorg/api:1.2", "State": "running"},
			// mysqld-exporter must NOT be classified as mysql.
			{"Id": "z", "Names": []string{"/exp"}, "Image": "prom/mysqld-exporter", "State": "running"},
		},
		map[string]map[string]any{},
	)
	defer srv.Close()

	s := newDockerScanner(srv.URL, nil)
	got, err := s.Scan(context.Background())
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestScan_SkipsStoppedContainers(t *testing.T) {
	srv := fakeDockerServer(t,
		[]map[string]any{{"Id": "abc", "Names": []string{"/db"}, "Image": "postgres:16", "State": "exited"}},
		map[string]map[string]any{
			"abc": {
				"Id":    "abc",
				"Name":  "/db",
				"State": map[string]any{"Running": false},
				"Config": map[string]any{"Image": "postgres:16", "Env": []string{}},
			},
		},
	)
	defer srv.Close()

	s := newDockerScanner(srv.URL, nil)
	got, err := s.Scan(context.Background())
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestScan_ContainerWithNoPublishedPorts(t *testing.T) {
	srv := fakeDockerServer(t,
		[]map[string]any{{"Id": "abc", "Names": []string{"/db"}, "Image": "postgres:16", "State": "running"}},
		map[string]map[string]any{
			"abc": {
				"Id":    "abc",
				"Name":  "/db",
				"State": map[string]any{"Running": true},
				"Config": map[string]any{
					"Image": "postgres:16",
					"Env":   []string{"POSTGRES_USER=app"},
				},
				"NetworkSettings": map[string]any{
					"Ports": map[string]any{
						// EXPOSE 5432 with no host binding.
						"5432/tcp": nil,
					},
				},
			},
		},
	)
	defer srv.Close()

	s := newDockerScanner(srv.URL, nil)
	got, err := s.Scan(context.Background())
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Len(t, got[0].Ports, 1)
	require.Equal(t, uint32(5432), got[0].Ports[0].ContainerPort)
	require.Equal(t, uint32(0), got[0].Ports[0].HostPort, "exposed-but-not-published HostPort must be 0")
}

func TestNormaliseImage(t *testing.T) {
	cases := map[string]string{
		"postgres:16":                    "postgres:16",
		"ghcr.io/example/postgres:16":    "example/postgres:16",
		"localhost:5000/mariadb:latest":  "mariadb:latest",
		"postgres@sha256:deadbeef":       "postgres",
		"BITNAMI/Postgresql:14":          "bitnami/postgresql:14",
	}
	for in, want := range cases {
		require.Equal(t, want, normaliseImage(in), "input %q", in)
	}
}

func TestBuildReport_NoValueLeakage(t *testing.T) {
	report := BuildReport([]DiscoveredContainer{{
		ContainerID:    "abc",
		Name:           "db",
		Image:          "postgres:16",
		DetectedDBType: "postgresql",
		EnvHints:       map[string]string{"POSTGRES_USER": "set", "POSTGRES_PASSWORD": "set"},
	}})
	require.Len(t, report.Containers, 1)
	for _, v := range report.Containers[0].EnvHints {
		require.Equal(t, "set", v)
	}
}
