// Package queue is a thin in-process abstraction over state.Store's queue
// bucket. The state package owns persistence; this package owns ordering
// semantics and (in D-02) the channel-based delivery to job workers.
//
// Today this file declares the small interface and a state-backed
// implementation that satisfies it — enough for the WSS client to push
// inbound RunBackup envelopes onto disk during the skeleton phase.
package queue

import (
	"context"

	"github.com/backupy/backupy/apps/agent/internal/state"
)

// Queue is the agent's persistent job queue.
type Queue interface {
	// Enqueue persists the encoded payload keyed by run_id. Idempotent.
	Enqueue(runID string, payload []byte) error
	// Pop returns up to n jobs without removing them from durable storage.
	Pop(ctx context.Context, n int) ([]Job, error)
	// Ack removes a job from durable storage after successful handling.
	Ack(runID string) error
	// Depth returns the current pending job count.
	Depth() (int, error)
}

// Job is one queued item.
type Job struct {
	RunID   string
	Payload []byte
}

// NewBolt returns a Queue backed by the BoltDB state store.
func NewBolt(s *state.Store) Queue {
	return &boltQueue{s: s}
}

type boltQueue struct {
	s *state.Store
}

func (q *boltQueue) Enqueue(runID string, payload []byte) error {
	return q.s.EnqueueJob(runID, payload)
}

func (q *boltQueue) Pop(_ context.Context, n int) ([]Job, error) {
	raw, err := q.s.DequeueJobs(n)
	if err != nil {
		return nil, err
	}
	out := make([]Job, len(raw))
	for i, j := range raw {
		out[i] = Job{RunID: j.RunID, Payload: j.Payload}
	}
	return out, nil
}

func (q *boltQueue) Ack(runID string) error {
	return q.s.AckJob(runID)
}

func (q *boltQueue) Depth() (int, error) {
	return q.s.QueueDepth()
}
