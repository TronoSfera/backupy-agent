// Package metrics owns the Prometheus instrumentation surface for the
// agent.
//
// Security: the HTTP endpoint is intended to be bound to LOOPBACK only
// (default 127.0.0.1:9090). Operators who want external scraping
// should configure their Prometheus to scrape via an SSH tunnel or
// expose via a reverse proxy with authentication. NEVER bind to
// 0.0.0.0 in production — the endpoint reveals job IDs, run cadence,
// and other metadata an attacker can use to fingerprint the host.
//
// Metric names follow Prometheus best practice: `backupy_agent_*`
// prefix, base unit (seconds, bytes), and `_total` suffix for
// monotonic counters.
//
// Counters/gauges/histograms are package-level singletons registered
// in init() so call sites can increment them inline without nil
// checks or dependency injection.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// RunsTotal counts backup runs by job_id and terminal status
	// ("success" or "failure"). Cardinality scales with the number of
	// configured jobs — bounded by the user's plan, so safe.
	RunsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "backupy_agent_runs_total",
		Help: "Total number of backup runs the agent has executed, partitioned by job_id and terminal status.",
	}, []string{"job_id", "status"})

	// RunDuration observes the wall-clock duration of a backup run in
	// seconds, partitioned by job_id. Buckets target the documented
	// run size envelope (1s small dump → 1h large).
	RunDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "backupy_agent_run_duration_seconds",
		Help:    "Wall-clock duration of backup runs in seconds.",
		Buckets: []float64{1, 5, 15, 60, 300, 900, 3600},
	}, []string{"job_id"})

	// RunSizeBytes observes the size of the uploaded ciphertext in
	// bytes, partitioned by job_id. Buckets span 1 MiB → 10 GiB.
	RunSizeBytes = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "backupy_agent_run_size_bytes",
		Help:    "Size of the uploaded ciphertext in bytes.",
		Buckets: []float64{1 << 20, 10 << 20, 100 << 20, 1 << 30, 10 << 30},
	}, []string{"job_id"})

	// WSSState is 1 for the currently-active state and 0 otherwise.
	// Allowed states: connected, reconnecting, disconnected.
	WSSState = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "backupy_agent_wss_connection_state",
		Help: "WSS connection state — 1 for the current state, 0 for the others. Labels: state=connected|reconnecting|disconnected.",
	}, []string{"state"})

	// WSSReconnects counts every reconnect attempt since the agent
	// started.
	WSSReconnects = promauto.NewCounter(prometheus.CounterOpts{
		Name: "backupy_agent_wss_reconnects_total",
		Help: "Total number of WSS reconnect attempts since process start.",
	})

	// DispatchPending tracks the on-disk persistent queue depth.
	DispatchPending = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "backupy_agent_dispatch_pending",
		Help: "Current depth of the persistent dispatch queue.",
	})

	// BuildInfo is a 1-valued gauge labelled with version + commit so
	// dashboards can group by build.
	BuildInfo = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "backupy_agent_build_info",
		Help: "Always 1; labels expose agent build version and commit.",
	}, []string{"version", "commit"})
)

// Reset clears every metric value. Intended for tests only — the
// production singletons accumulate across the process lifetime.
func Reset() {
	RunsTotal.Reset()
	RunDuration.Reset()
	RunSizeBytes.Reset()
	WSSState.Reset()
	BuildInfo.Reset()
	DispatchPending.Set(0)
}

// SetWSSState marks `state` as the active connection state by writing
// 1 to its gauge and zeroing the other two. The caller passes one of
// "connected", "reconnecting", "disconnected" — unknown values are
// recorded as-is so a misuse is visible in /metrics rather than
// silently swallowed.
func SetWSSState(state string) {
	for _, s := range []string{"connected", "reconnecting", "disconnected"} {
		v := 0.0
		if s == state {
			v = 1.0
		}
		WSSState.WithLabelValues(s).Set(v)
	}
	if state != "connected" && state != "reconnecting" && state != "disconnected" {
		WSSState.WithLabelValues(state).Set(1)
	}
}

// SetBuildInfo registers the build version + commit as labels on the
// build_info gauge. Safe to call multiple times — the gauge value is
// always 1.
func SetBuildInfo(version, commit string) {
	BuildInfo.WithLabelValues(version, commit).Set(1)
}
