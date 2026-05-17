package metrics

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/require"
)

func TestHandlerExposesBuildInfo(t *testing.T) {
	Reset()
	SetBuildInfo("v1.2.3", "abcdef")

	srv := httptest.NewServer(promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{}))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	out := string(body)

	require.Contains(t, out, "backupy_agent_build_info")
	require.Contains(t, out, `version="v1.2.3"`)
	require.Contains(t, out, `commit="abcdef"`)
}

func TestSetWSSStateOneHot(t *testing.T) {
	Reset()
	SetWSSState("connected")

	mfs, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	var connected, reconnecting, disconnected float64
	for _, mf := range mfs {
		if mf.GetName() != "backupy_agent_wss_connection_state" {
			continue
		}
		for _, m := range mf.GetMetric() {
			var label string
			for _, l := range m.GetLabel() {
				if l.GetName() == "state" {
					label = l.GetValue()
				}
			}
			switch label {
			case "connected":
				connected = m.GetGauge().GetValue()
			case "reconnecting":
				reconnecting = m.GetGauge().GetValue()
			case "disconnected":
				disconnected = m.GetGauge().GetValue()
			}
		}
	}
	require.Equal(t, 1.0, connected)
	require.Equal(t, 0.0, reconnecting)
	require.Equal(t, 0.0, disconnected)
}

func TestListenAndServeContextCancel(t *testing.T) {
	// Ask the OS for a free port to avoid collisions with a real
	// running agent on 9090.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ListenAndServe(ctx, addr) }()

	// Wait briefly for the listener to come up.
	var resp *http.Response
	for i := 0; i < 50; i++ {
		resp, err = http.Get("http://" + addr + "/metrics")
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.True(t, strings.Contains(string(body), "backupy_agent_"), "expected backupy_agent_* metrics in body")

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("metrics server did not shut down within 5s of ctx cancel")
	}
}
