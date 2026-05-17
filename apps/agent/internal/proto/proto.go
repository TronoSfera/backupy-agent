// Package proto re-exports the generated protobuf types so the rest of the
// agent imports a single canonical name (`proto.Envelope`) rather than the
// long `backupv1.Envelope`. It also centralises a few convenience helpers
// (NewEnvelope) so call sites stay concise.
//
// Prerequisite: `make proto` must have been run from the repo root to
// generate packages/proto/gen/go/v1/*.pb.go. The agent's go.mod uses a
// `replace` directive pointing at that local path — see go.mod.
package proto

import (
	"time"

	backupv1 "github.com/backupy/backupy/packages/proto/gen/go/backupv1"
)

// Type aliases keep the agent codebase decoupled from the long generated
// package path. If we ever migrate to backup.v2 we change the alias here
// and the rest of the agent compiles unchanged.
type (
	Envelope          = backupv1.Envelope
	Register          = backupv1.Register
	Heartbeat         = backupv1.Heartbeat
	AgentMetrics      = backupv1.AgentMetrics
	DiscoveryReport   = backupv1.DiscoveryReport
	JobUpdate         = backupv1.JobUpdate
	BackupCompleted   = backupv1.BackupCompleted
	HealthCheckResult = backupv1.HealthCheckResult
	LogEvent          = backupv1.LogEvent
	Ack               = backupv1.Ack

	RegisterAck    = backupv1.RegisterAck
	ConfigUpdate   = backupv1.ConfigUpdate
	RunBackup      = backupv1.RunBackup
	CancelJob      = backupv1.CancelJob
	RunHealthCheck = backupv1.RunHealthCheck
	SelfUpdate     = backupv1.SelfUpdate
	Ping           = backupv1.Ping

	AgentConfig = backupv1.AgentConfig

	// Envelope payload oneof wrappers — full set so client/loops construct
	// outgoing envelopes and pattern-match incoming via `agentproto.Envelope_*`
	// without importing the generated package.
	Envelope_Register        = backupv1.Envelope_Register
	Envelope_Heartbeat       = backupv1.Envelope_Heartbeat
	Envelope_Discovery       = backupv1.Envelope_Discovery
	Envelope_JobUpdate       = backupv1.Envelope_JobUpdate
	Envelope_BackupCompleted = backupv1.Envelope_BackupCompleted
	Envelope_HealthResult    = backupv1.Envelope_HealthResult
	Envelope_Log             = backupv1.Envelope_Log
	Envelope_RestoreUpdate   = backupv1.Envelope_RestoreUpdate
	Envelope_Ack             = backupv1.Envelope_Ack
	Envelope_RegisterAck     = backupv1.Envelope_RegisterAck
	Envelope_ConfigUpdate    = backupv1.Envelope_ConfigUpdate
	Envelope_RunBackup       = backupv1.Envelope_RunBackup
	Envelope_CancelJob       = backupv1.Envelope_CancelJob
	Envelope_RunHealthCheck  = backupv1.Envelope_RunHealthCheck
	Envelope_SelfUpdate      = backupv1.Envelope_SelfUpdate
	Envelope_Ping            = backupv1.Envelope_Ping
)

// NowMillis returns the current wall-clock time in unix milliseconds —
// the canonical ts_ms field used in every Envelope.
func NowMillis() uint64 {
	return uint64(time.Now().UnixMilli())
}

// NewEnvelope is a convenience constructor that stamps ts_ms with the
// current time. Callers set seq + correlation_id + payload separately.
func NewEnvelope() *Envelope {
	return &Envelope{TsMs: NowMillis()}
}
