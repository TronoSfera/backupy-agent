package wss

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/proto"

	agentproto "github.com/backupy/backupy/apps/agent/internal/proto"
)

// readLoop pumps inbound frames from the server into the dispatch
// switch until the connection breaks or ctx is cancelled.
func (c *Client) readLoop(ctx context.Context, ws *websocket.Conn) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		readCtx, cancel := context.WithTimeout(ctx, readIdleTimeout)
		typ, data, err := ws.Read(readCtx)
		cancel()
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return fmt.Errorf("read timeout after %s", readIdleTimeout)
			}
			return fmt.Errorf("read: %w", err)
		}
		if typ != websocket.MessageBinary {
			c.logger.Warn("wss: ignoring non-binary frame", slog.String("type", typ.String()))
			continue
		}
		env := &agentproto.Envelope{}
		if err := proto.Unmarshal(data, env); err != nil {
			c.logger.Warn("wss: unmarshal failed; closing", slog.Any("err", err))
			return fmt.Errorf("unmarshal: %w", err)
		}
		if err := c.dispatch(ctx, env); err != nil {
			c.logger.Warn("wss: dispatch error",
				slog.String("payload_type", payloadName(env)),
				slog.Any("err", err))
		}
	}
}

// dispatch routes a single inbound envelope to a user callback.
func (c *Client) dispatch(ctx context.Context, env *agentproto.Envelope) error {
	if env == nil || env.Payload == nil {
		return errors.New("empty envelope")
	}
	switch p := env.Payload.(type) {
	case *agentproto.Envelope_ConfigUpdate:
		cfg := p.ConfigUpdate.GetConfig()
		if cfg != nil {
			if raw, err := proto.Marshal(cfg); err == nil && c.state != nil {
				_ = c.state.SaveConfig(cfg.Version, raw)
			}
			c.configVersion.Store(cfg.Version)
		}
		if c.handlers != nil && c.handlers.OnConfigUpdate != nil {
			return c.handlers.OnConfigUpdate(ctx, p.ConfigUpdate)
		}
	case *agentproto.Envelope_RunBackup:
		if c.handlers != nil && c.handlers.OnRunBackup != nil {
			return c.handlers.OnRunBackup(ctx, p.RunBackup)
		}
	case *agentproto.Envelope_CancelJob:
		if c.handlers != nil && c.handlers.OnCancelJob != nil {
			return c.handlers.OnCancelJob(ctx, p.CancelJob)
		}
	case *agentproto.Envelope_RunHealthCheck:
		if c.handlers != nil && c.handlers.OnRunHealthCheck != nil {
			return c.handlers.OnRunHealthCheck(ctx, p.RunHealthCheck)
		}
	case *agentproto.Envelope_SelfUpdate:
		if c.handlers != nil && c.handlers.OnSelfUpdate != nil {
			return c.handlers.OnSelfUpdate(ctx, p.SelfUpdate)
		}
	case *agentproto.Envelope_Ping:
		// Reply with an Ack so the server has a round-trip metric.
		ack := agentproto.NewEnvelope()
		ack.CorrelationId = env.CorrelationId
		ack.Payload = &agentproto.Envelope_Ack{Ack: &agentproto.Ack{
			CorrelationId: env.CorrelationId, Accepted: true,
		}}
		return c.Send(ack)
	case *agentproto.Envelope_RegisterAck:
		// Should only arrive during handshake; ignore here.
		return nil
	default:
		return fmt.Errorf("unsupported server->agent payload %T", p)
	}
	return nil
}

// writeLoop drains the per-connection out channel into the wire.
func (c *Client) writeLoop(ctx context.Context, ws *websocket.Conn, out <-chan *agentproto.Envelope) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case env := <-out:
			if env == nil {
				continue
			}
			if env.Seq == 0 {
				env.Seq = c.seq.Add(1)
			}
			if env.TsMs == 0 {
				env.TsMs = agentproto.NowMillis()
			}
			raw, err := proto.Marshal(env)
			if err != nil {
				c.logger.Warn("wss: marshal failed; dropping",
					slog.String("payload_type", payloadName(env)),
					slog.Any("err", err))
				continue
			}
			wCtx, cancel := context.WithTimeout(ctx, writeTimeout)
			err = ws.Write(wCtx, websocket.MessageBinary, raw)
			cancel()
			if err != nil {
				return fmt.Errorf("write: %w", err)
			}
		}
	}
}

// heartbeatLoop sends a Heartbeat envelope every heartbeatInterval.
func (c *Client) heartbeatLoop(ctx context.Context, out chan<- *agentproto.Envelope) error {
	t := time.NewTicker(heartbeatInterval)
	defer t.Stop()
	// Send an initial heartbeat right after the handshake so the
	// server's last_seen_at populates without waiting 30s.
	if err := c.emitHeartbeat(ctx, out); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if err := c.emitHeartbeat(ctx, out); err != nil {
				return err
			}
		}
	}
}

func (c *Client) emitHeartbeat(ctx context.Context, out chan<- *agentproto.Envelope) error {
	hb := &agentproto.Heartbeat{ConfigVersion: c.configVersion.Load()}
	if c.metrics != nil {
		hb.Metrics = c.metrics()
	}
	env := agentproto.NewEnvelope()
	env.Payload = &agentproto.Envelope_Heartbeat{Heartbeat: hb}
	select {
	case out <- env:
		if c.state != nil {
			_ = c.state.RecordHeartbeat(time.Now().UnixMilli())
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// payloadName returns a short identifier for log lines.
func payloadName(env *agentproto.Envelope) string {
	if env == nil || env.Payload == nil {
		return "empty"
	}
	return fmt.Sprintf("%T", env.Payload)
}

// hostname is a thin wrapper around os.Hostname so tests can stub it.
func hostname() (string, error) {
	return os.Hostname()
}
