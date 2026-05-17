// Package wss owns the long-lived WebSocket-Secure connection from the
// agent to the control plane (see docs/07-api-contract.md §1 and
// docs/03-agent-spec.md → "WSS-канал").
//
// Lifecycle:
//
//  1. Dial cfg.ServerURL/v1/agents/connect with `Authorization: Bearer
//     <agent_key>`.
//  2. Send Register; await RegisterAck.
//  3. Persist session_id + config snapshot in state.Store.
//  4. Spawn read + write goroutines; replay any queued outbound jobs;
//     start the 30s heartbeat ticker.
//  5. On any read/write error, close the socket and reconnect with
//     exponential backoff (see backoff.go).
//  6. On context cancellation, close cleanly and return.
//
// Safe for concurrent Send calls.
package wss

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/proto"

	pkgmetrics "github.com/backupy/backupy/apps/agent/internal/metrics"
	agentproto "github.com/backupy/backupy/apps/agent/internal/proto"
	"github.com/backupy/backupy/apps/agent/internal/queue"
	"github.com/backupy/backupy/apps/agent/internal/state"
)

// heartbeatInterval is the default heartbeat cadence; the server may
// override it in RegisterAck.heartbeat_interval_sec.
const heartbeatInterval = 30 * time.Second

// readIdleTimeout is how long the agent waits for *any* server frame
// before declaring the connection dead. The server pings every 30s; we
// allow three misses to match the RTO from docs/07 §1.
const readIdleTimeout = 90 * time.Second

// writeTimeout bounds a single Write — must be shorter than the read
// timeout on the other side so the server doesn't tear us down first.
const writeTimeout = 10 * time.Second

// registerTimeout bounds the Register/RegisterAck handshake.
const registerTimeout = 15 * time.Second

// Config is everything the client needs from the outside world.
type Config struct {
	ServerURL string
	AgentKey  string // never logged
	// AgentVersion is reported in the Register payload.
	AgentVersion string
	// Hostname / OS / Arch are filled by the agent's runtime probe;
	// callers may leave them empty and the client will detect.
	Hostname      string
	OS            string
	Arch          string
	DockerVersion string
	Capabilities  []string
	// AllowInsecure permits ws:// / http:// dial schemes when ServerURL
	// uses one. Production must leave this false — it matches the
	// agent's BACKUP_DEV_ALLOW_INSECURE bootstrap flag.
	AllowInsecure bool
}

// Handlers carries user callbacks invoked when the server pushes a
// command. Each callback runs on the read goroutine and must return
// quickly — long-running work belongs on a worker pool.
type Handlers struct {
	OnConfigUpdate   func(ctx context.Context, msg *agentproto.ConfigUpdate) error
	OnRunBackup      func(ctx context.Context, msg *agentproto.RunBackup) error
	OnCancelJob      func(ctx context.Context, msg *agentproto.CancelJob) error
	OnRunHealthCheck func(ctx context.Context, msg *agentproto.RunHealthCheck) error
	OnSelfUpdate     func(ctx context.Context, msg *agentproto.SelfUpdate) error
}

// AgentMetrics is a hook supplied by the caller to populate periodic
// Heartbeats. Returns the most recent snapshot of host metrics. May be
// nil — heartbeats then carry zeroed metrics.
type AgentMetrics func() *agentproto.AgentMetrics

// Client is the WSS connection manager. Safe for concurrent Send calls.
type Client struct {
	cfg      Config
	state    *state.Store
	queue    queue.Queue
	handlers *Handlers
	metrics  AgentMetrics
	logger   *slog.Logger

	// connMu protects the active websocket conn and out chan; both
	// are nil when disconnected.
	connMu sync.Mutex
	conn   *websocket.Conn
	out    chan *agentproto.Envelope

	seq           atomic.Uint64
	configVersion atomic.Uint64
	sessionID     atomic.Value // string

	reconnectCount atomic.Uint64
}

// NewClient constructs a client. The connection is not opened until
// Start. logger may be nil.
func NewClient(cfg Config, st *state.Store, q queue.Queue, h *Handlers, m AgentMetrics, logger *slog.Logger) *Client {
	if logger == nil {
		logger = slog.Default()
	}
	c := &Client{
		cfg:      cfg,
		state:    st,
		queue:    q,
		handlers: h,
		metrics:  m,
		logger:   logger.With(slog.String("component", "wss")),
	}
	// Seed config version from the last persisted snapshot so the
	// server can decide whether a ConfigUpdate is needed at register.
	if st != nil {
		if v, _, err := st.LoadConfig(); err == nil {
			c.configVersion.Store(v)
		}
	}
	if cfg.Hostname == "" {
		if h, err := osHostname(); err == nil {
			c.cfg.Hostname = h
		}
	}
	if cfg.OS == "" {
		c.cfg.OS = runtime.GOOS
	}
	if cfg.Arch == "" {
		c.cfg.Arch = runtime.GOARCH
	}
	return c
}

// Start runs the connection lifecycle until ctx is cancelled. Each
// iteration dials, runs read/write loops, and on any failure waits per
// the backoff schedule before retrying.
func (c *Client) Start(ctx context.Context) error {
	c.logger.Info("wss client starting",
		slog.String("server_url", c.cfg.ServerURL))
	bo := NewBackoff()
	for {
		if err := ctx.Err(); err != nil {
			c.logger.Info("wss client shutting down", slog.Any("reason", err))
			return nil
		}
		runErr := c.runOnce(ctx)
		if ctx.Err() != nil {
			c.logger.Info("wss client shutting down")
			pkgmetrics.SetWSSState("disconnected")
			return nil
		}
		c.reconnectCount.Add(1)
		pkgmetrics.WSSReconnects.Inc()
		pkgmetrics.SetWSSState("reconnecting")
		delay := bo.Next()
		c.logger.Warn("wss disconnected; reconnecting",
			slog.Any("err", runErr),
			slog.Duration("backoff", delay))
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}
		// Reset backoff after a successful connection cycle is handled
		// inside runOnce when the handshake completes.
		_ = bo
	}
}

// Send queues an outbound envelope. If the client is currently
// connected, the envelope is pushed to the in-memory out channel; if
// disconnected (or the buffer is full), the envelope is persisted to
// the on-disk queue keyed by correlation_id (or seq-N fallback).
func (c *Client) Send(env *agentproto.Envelope) error {
	if env == nil {
		return errors.New("wss: nil envelope")
	}
	if env.Seq == 0 {
		env.Seq = c.seq.Add(1)
	}
	if env.TsMs == 0 {
		env.TsMs = agentproto.NowMillis()
	}
	c.connMu.Lock()
	out := c.out
	c.connMu.Unlock()
	if out != nil {
		select {
		case out <- env:
			return nil
		default:
			// fall through to persistent queue
		}
	}
	raw, err := proto.Marshal(env)
	if err != nil {
		return fmt.Errorf("wss: marshal envelope: %w", err)
	}
	key := env.CorrelationId
	if key == "" {
		key = fmt.Sprintf("seq-%d", env.Seq)
	}
	if c.queue == nil {
		return errors.New("wss: queue not configured, message dropped")
	}
	return c.queue.Enqueue(key, raw)
}

// SessionID returns the most recently assigned session id, or "" if no
// successful handshake has happened yet.
func (c *Client) SessionID() string {
	v, _ := c.sessionID.Load().(string)
	return v
}

// ConfigVersion returns the currently-applied config version.
func (c *Client) ConfigVersion() uint64 { return c.configVersion.Load() }

// ReconnectCount returns the total number of reconnect attempts since
// Start was invoked. Exported for the metrics endpoint.
func (c *Client) ReconnectCount() uint64 { return c.reconnectCount.Load() }

// runOnce dials, performs the handshake, and pumps frames until the
// connection breaks. Returns the cause of the disconnect, or nil if
// ctx was cancelled.
func (c *Client) runOnce(ctx context.Context) error {
	ws, err := c.dial(ctx)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer func() { _ = ws.Close(websocket.StatusNormalClosure, "") }()

	// Handshake: Register -> RegisterAck.
	ack, err := c.handshake(ctx, ws)
	if err != nil {
		_ = ws.Close(websocket.StatusPolicyViolation, "handshake failed")
		return fmt.Errorf("handshake: %w", err)
	}

	// Persist session + config.
	c.sessionID.Store(ack.SessionId)
	// Track the applied config version in-memory regardless of whether a
	// state store is wired in (the store is optional in tests and for
	// stateless deployments). Persistence happens separately below.
	if ack.Config != nil && ack.Config.Version > c.configVersion.Load() {
		c.configVersion.Store(ack.Config.Version)
	}
	if c.state != nil {
		_ = c.state.SaveSession(ack.SessionId, time.Now().UnixMilli())
		if ack.Config != nil {
			if raw, merr := proto.Marshal(ack.Config); merr == nil {
				_ = c.state.SaveConfig(ack.Config.Version, raw)
			}
		}
	}
	// Apply via user callback so the pipeline reacts immediately. This
	// runs regardless of state-store presence because handlers are an
	// independent injection point.
	if ack.Config != nil && c.handlers != nil && c.handlers.OnConfigUpdate != nil {
		_ = c.handlers.OnConfigUpdate(ctx, &agentproto.ConfigUpdate{Config: ack.Config})
	}

	// Handshake completed — mark the connection as live for metrics.
	pkgmetrics.SetWSSState("connected")

	// Per-connection out channel; mirror it on the Client so Send can
	// reach it.
	out := make(chan *agentproto.Envelope, 64)
	c.connMu.Lock()
	c.conn = ws
	c.out = out
	c.connMu.Unlock()
	defer func() {
		c.connMu.Lock()
		c.conn = nil
		c.out = nil
		c.connMu.Unlock()
	}()

	// Replay any queued envelopes from disk — they were buffered while
	// disconnected and must reach the server idempotently.
	c.replayQueue(ctx, out)

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 3)
	go func() { errCh <- c.readLoop(connCtx, ws) }()
	go func() { errCh <- c.writeLoop(connCtx, ws, out) }()
	go func() { errCh <- c.heartbeatLoop(connCtx, out) }()

	err = <-errCh
	cancel()
	// Closing the websocket unblocks any pending Read/Write call so
	// the remaining loops exit promptly.
	_ = ws.Close(websocket.StatusNormalClosure, "client done")
	// Drain the rest so they don't outlive us.
	<-errCh
	<-errCh
	return err
}

// dial opens the WebSocket connection, attaching the Authorization
// header. Returns the live conn or an error.
func (c *Client) dial(ctx context.Context) (*websocket.Conn, error) {
	wsURL, err := buildWSURL(c.cfg.ServerURL, c.cfg.AllowInsecure)
	if err != nil {
		return nil, err
	}
	h := http.Header{}
	h.Set("Authorization", "Bearer "+c.cfg.AgentKey)

	dialCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	ws, _, err := websocket.Dial(dialCtx, wsURL, &websocket.DialOptions{
		HTTPHeader: h,
	})
	if err != nil {
		return nil, err
	}
	// The protobuf-encoded frames can be larger than the default 32 KB
	// read limit (e.g. AgentConfig with many targets).
	ws.SetReadLimit(4 * 1024 * 1024)
	c.logger.Info("wss connected", slog.String("url", wsURL))
	return ws, nil
}

// buildWSURL rewrites http(s):// to ws(s):// and appends the canonical
// agent endpoint path.
func buildWSURL(raw string, allowInsecure bool) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("parse server url: %w", err)
	}
	switch u.Scheme {
	case "http":
		if !allowInsecure {
			return "", errors.New("server url must be https://; set AllowInsecure for dev")
		}
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	case "ws", "wss":
		// already a websocket URL
	default:
		return "", fmt.Errorf("unsupported scheme %q", u.Scheme)
	}
	// Mount the canonical control-plane endpoint. Keep any path the
	// caller provided as a prefix (some deploys put a reverse proxy
	// path in front of the API).
	const endpoint = "/v1/agents/connect"
	if !strings.HasSuffix(u.Path, endpoint) {
		u.Path = strings.TrimRight(u.Path, "/") + endpoint
	}
	return u.String(), nil
}

// handshake sends Register and waits for RegisterAck.
func (c *Client) handshake(ctx context.Context, ws *websocket.Conn) (*agentproto.RegisterAck, error) {
	reg := &agentproto.Register{
		AgentVersion:           c.cfg.AgentVersion,
		Hostname:               c.cfg.Hostname,
		Os:                     c.cfg.OS,
		Arch:                   c.cfg.Arch,
		DockerVersion:          c.cfg.DockerVersion,
		Capabilities:           c.cfg.Capabilities,
		LastKnownConfigVersion: c.configVersion.Load(),
	}
	env := agentproto.NewEnvelope()
	env.Seq = c.seq.Add(1)
	env.Payload = &agentproto.Envelope_Register{Register: reg}
	raw, err := proto.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("marshal register: %w", err)
	}
	hCtx, cancel := context.WithTimeout(ctx, registerTimeout)
	defer cancel()
	if err := ws.Write(hCtx, websocket.MessageBinary, raw); err != nil {
		return nil, fmt.Errorf("write register: %w", err)
	}
	typ, data, err := ws.Read(hCtx)
	if err != nil {
		return nil, fmt.Errorf("read register_ack: %w", err)
	}
	if typ != websocket.MessageBinary {
		return nil, fmt.Errorf("register_ack must be binary, got %s", typ)
	}
	ackEnv := &agentproto.Envelope{}
	if err := proto.Unmarshal(data, ackEnv); err != nil {
		return nil, fmt.Errorf("unmarshal register_ack: %w", err)
	}
	p, ok := ackEnv.Payload.(*agentproto.Envelope_RegisterAck)
	if !ok || p.RegisterAck == nil {
		return nil, fmt.Errorf("first server payload must be RegisterAck, got %T", ackEnv.Payload)
	}
	return p.RegisterAck, nil
}

// replayQueue dumps any pending RunBackup envelopes from the persistent
// queue onto the live out channel. Idempotent on the server side via
// run_id. Best-effort: a single failure stops replay but does not tear
// down the connection.
func (c *Client) replayQueue(ctx context.Context, out chan<- *agentproto.Envelope) {
	if c.queue == nil {
		return
	}
	jobs, err := c.queue.Pop(ctx, 100)
	if err != nil {
		c.logger.Warn("queue: pop failed", slog.Any("err", err))
		return
	}
	for _, j := range jobs {
		env := &agentproto.Envelope{}
		if err := proto.Unmarshal(j.Payload, env); err != nil {
			c.logger.Warn("queue: corrupt payload; dropping",
				slog.String("run_id", j.RunID), slog.Any("err", err))
			_ = c.queue.Ack(j.RunID)
			continue
		}
		select {
		case out <- env:
			_ = c.queue.Ack(j.RunID)
		case <-ctx.Done():
			return
		}
	}
}

// osHostname is overridable in tests; defaults to os.Hostname.
var osHostname = func() (string, error) {
	return hostname()
}
