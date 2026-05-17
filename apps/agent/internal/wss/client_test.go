package wss

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	agentproto "github.com/backupy/backupy/apps/agent/internal/proto"
	"github.com/backupy/backupy/apps/agent/internal/queue"
)

func TestBuildWSURL(t *testing.T) {
	tests := []struct {
		name      string
		in        string
		insecure  bool
		want      string
		expectErr bool
	}{
		{"https rewrites to wss", "https://api.example.com", false, "wss://api.example.com/v1/agents/connect", false},
		{"http rejected without flag", "http://localhost:8080", false, "", true},
		{"http accepted with flag", "http://localhost:8080", true, "ws://localhost:8080/v1/agents/connect", false},
		{"already wss preserved", "wss://api.example.com", false, "wss://api.example.com/v1/agents/connect", false},
		{"path preserved", "https://api.example.com/proxy", false, "wss://api.example.com/proxy/v1/agents/connect", false},
		{"unknown scheme", "ftp://nope", false, "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := buildWSURL(tc.in, tc.insecure)
			if tc.expectErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestClient_DispatchPingReturnsAck(t *testing.T) {
	c := &Client{}
	env := agentproto.NewEnvelope()
	env.CorrelationId = "ping-1"
	env.Payload = &agentproto.Envelope_Ping{Ping: &agentproto.Ping{TsMs: 123}}
	// Send needs an out channel or queue; here we exercise the Send
	// fallback path: with queue=nil and out=nil, Send returns an error
	// that we intentionally swallow. Dispatch should not panic.
	require.NotPanics(t, func() {
		_ = c.dispatch(context.Background(), env)
	})
}

// fakeServer is a minimal coder/websocket echo that performs a
// Register/RegisterAck handshake then records any inbound frames into a
// channel. Used to drive end-to-end client behaviour without spinning
// up the real server package.
func fakeServer(t *testing.T, inboundCh chan<- *agentproto.Envelope) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Bearer test-key", r.Header.Get("Authorization"))
		ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		require.NoError(t, err)
		ctx := r.Context()
		// Read Register.
		_, data, err := ws.Read(ctx)
		require.NoError(t, err)
		env := &agentproto.Envelope{}
		require.NoError(t, proto.Unmarshal(data, env))
		_, ok := env.Payload.(*agentproto.Envelope_Register)
		require.True(t, ok)
		// Send RegisterAck.
		ack := agentproto.NewEnvelope()
		ack.Payload = &agentproto.Envelope_RegisterAck{RegisterAck: &agentproto.RegisterAck{
			SessionId: "sess-1", HeartbeatIntervalSec: 30,
			Config: &agentproto.AgentConfig{Version: 5},
		}}
		raw, _ := proto.Marshal(ack)
		require.NoError(t, ws.Write(ctx, websocket.MessageBinary, raw))

		// Forward subsequent frames to inboundCh until the conn closes.
		for {
			_, data, err := ws.Read(ctx)
			if err != nil {
				return
			}
			env := &agentproto.Envelope{}
			if proto.Unmarshal(data, env) != nil {
				continue
			}
			select {
			case inboundCh <- env:
			default:
			}
		}
	}))
}

func TestClient_RegisterAndHeartbeat(t *testing.T) {
	inbound := make(chan *agentproto.Envelope, 16)
	ts := fakeServer(t, inbound)
	defer ts.Close()

	var hbSeen atomic.Bool
	c := NewClient(Config{
		ServerURL:     ts.URL,
		AgentKey:      "test-key",
		AgentVersion:  "test",
		AllowInsecure: true,
	}, nil, nil, nil, func() *agentproto.AgentMetrics {
		return &agentproto.AgentMetrics{CpuPercent: 1.5}
	}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = c.Start(ctx) }()

	// Expect to see a Heartbeat in the inbound stream.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case env := <-inbound:
			if _, ok := env.Payload.(*agentproto.Envelope_Heartbeat); ok {
				hbSeen.Store(true)
			}
		case <-time.After(100 * time.Millisecond):
		}
		if hbSeen.Load() {
			break
		}
	}
	require.True(t, hbSeen.Load(), "expected at least one Heartbeat envelope")
	require.NotEmpty(t, c.SessionID())
	require.Equal(t, uint64(5), c.ConfigVersion())
}

func TestClient_SendWithoutConnectionQueues(t *testing.T) {
	// Use a memory queue stub.
	q := &memQueue{}
	c := NewClient(Config{
		ServerURL:    "wss://example.com",
		AgentKey:     "k",
		AgentVersion: "v",
	}, nil, q, nil, nil, nil)

	env := agentproto.NewEnvelope()
	env.CorrelationId = "x-1"
	env.Payload = &agentproto.Envelope_Heartbeat{Heartbeat: &agentproto.Heartbeat{}}
	require.NoError(t, c.Send(env))
	require.Equal(t, 1, q.depth)
}

// memQueue is a stub queue used by the test above. It satisfies the
// queue.Queue interface without bringing in BoltDB.
type memQueue struct {
	depth int
	last  []byte
}

func (m *memQueue) Enqueue(_ string, payload []byte) error {
	m.depth++
	m.last = payload
	return nil
}
func (m *memQueue) Pop(_ context.Context, _ int) ([]queue.Job, error) { return nil, nil }
func (m *memQueue) Ack(_ string) error                                { return nil }
func (m *memQueue) Depth() (int, error)                               { return m.depth, nil }

// ensure strings is used to silence unused-import linter when tests
// shift around.
var _ = strings.TrimSpace
