// Package state owns the agent's persistent on-disk state — a BoltDB file
// at $BACKUP_STATE_DIR/state.db.
//
// Buckets:
//
//	"config"      — last-known AgentConfig (key: "current") and version.
//	"queue"       — pending RunBackup jobs (key: run_id, value: encoded envelope).
//	"registry"    — session metadata: last session_id, server_time, heartbeat.
//	"logs_buffer" — rate-limited LogEvent buffer when server is unreachable.
//
// All bucket values are encrypted with AES-256-GCM keyed by HKDF(BACKUP_AGENT_KEY).
// See crypto.go for the wire format.
//
// Concurrency: bbolt serialises write transactions itself, so the Store is
// safe for concurrent use without an internal mutex.
package state

import (
	"crypto/cipher"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
)

// Bucket names — exported only as constants here; callers go through
// Store methods, not raw bbolt buckets.
var (
	bktConfig   = []byte("config")
	bktQueue    = []byte("queue")
	bktRegistry = []byte("registry")
	bktLogs     = []byte("logs_buffer")

	keyConfigCurrent = []byte("current")
	keyConfigVersion = []byte("version")
	keySessionID     = []byte("session_id")
	keyServerTime    = []byte("server_time_ms")
	keyHeartbeat     = []byte("last_heartbeat_ms")
)

// ErrNotFound is returned when a key is absent. Distinguishing missing data
// from a cipher error is important — a wrong key must never be silently
// treated as "no config yet".
var ErrNotFound = errors.New("state: not found")

// Store is the public handle for the agent's BoltDB-backed state.
type Store struct {
	db   *bolt.DB
	aead cipher.AEAD
}

// QueuedJob is a single pending job pulled from the queue bucket.
type QueuedJob struct {
	RunID   string
	Payload []byte // decrypted, opaque to this package
}

// Options controls Store construction. All fields are optional except
// AgentKey which is required to derive the AES key.
type Options struct {
	AgentKey string
	// Timeout controls how long Open waits for an exclusive file lock.
	// Zero defaults to 5 seconds — enough for an old process to die,
	// short enough to fail fast in CI.
	Timeout time.Duration
}

// Open creates or opens the BoltDB file at path, initialises the four core
// buckets, and prepares the AES cipher used for value encryption.
func Open(path string, opts Options) (*Store, error) {
	if path == "" {
		return nil, errors.New("state: empty path")
	}
	if filepath.Ext(path) == "" {
		// Be forgiving: callers pass us a directory by mistake more often
		// than they pass a file with no extension. Suffix .db here so the
		// resulting error message is obvious.
		path += ".db"
	}
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	key, err := deriveStateKey(opts.AgentKey)
	if err != nil {
		return nil, err
	}
	aead, err := newGCM(key)
	if err != nil {
		return nil, err
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: timeout})
	if err != nil {
		return nil, fmt.Errorf("state: open bbolt %q: %w", path, err)
	}
	s := &Store{db: db, aead: aead}
	if err := s.ensureBuckets(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) ensureBuckets() error {
	return s.db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bktConfig, bktQueue, bktRegistry, bktLogs} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return fmt.Errorf("state: create bucket %s: %w", b, err)
			}
		}
		return nil
	})
}

// Close releases the BoltDB file handle.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// Path returns the file path of the underlying BoltDB.
func (s *Store) Path() string {
	return s.db.Path()
}

// --- config bucket --------------------------------------------------------

// SaveConfig stores the encoded AgentConfig snapshot together with its
// monotonically increasing version. Callers serialise the protobuf
// themselves so this package stays oblivious to message shapes.
func (s *Store) SaveConfig(version uint64, raw []byte) error {
	enc, err := seal(s.aead, raw)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bktConfig)
		if err := b.Put(keyConfigCurrent, enc); err != nil {
			return err
		}
		return b.Put(keyConfigVersion, u64Bytes(version))
	})
}

// LoadConfig returns the last saved config plus its version. Returns
// ErrNotFound when no config has ever been saved.
func (s *Store) LoadConfig() (uint64, []byte, error) {
	var version uint64
	var raw []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bktConfig)
		v := b.Get(keyConfigCurrent)
		if v == nil {
			return ErrNotFound
		}
		pt, err := open(s.aead, v)
		if err != nil {
			return err
		}
		raw = pt
		if vb := b.Get(keyConfigVersion); vb != nil {
			version = bytesToU64(vb)
		}
		return nil
	})
	if err != nil {
		return 0, nil, err
	}
	return version, raw, nil
}

// --- queue bucket --------------------------------------------------------

// EnqueueJob persists a pending job keyed by run_id. Idempotent: re-enqueuing
// the same run_id overwrites the previous payload (matches the spec — jobs
// dedupe by run_id).
func (s *Store) EnqueueJob(runID string, payload []byte) error {
	if runID == "" {
		return errors.New("state: empty run id")
	}
	enc, err := seal(s.aead, payload)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bktQueue).Put([]byte(runID), enc)
	})
}

// DequeueJobs returns up to n jobs in key order without removing them.
// Use AckJob to drop a job once its delivery is confirmed.
func (s *Store) DequeueJobs(n int) ([]QueuedJob, error) {
	if n <= 0 {
		return nil, nil
	}
	out := make([]QueuedJob, 0, n)
	err := s.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bktQueue).Cursor()
		for k, v := c.First(); k != nil && len(out) < n; k, v = c.Next() {
			pt, err := open(s.aead, v)
			if err != nil {
				return err
			}
			// Copy because bbolt slices are only valid for the txn lifetime.
			kc := make([]byte, len(k))
			copy(kc, k)
			out = append(out, QueuedJob{RunID: string(kc), Payload: pt})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// AckJob removes a job from the queue. Safe to call on an unknown run_id.
func (s *Store) AckJob(runID string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bktQueue).Delete([]byte(runID))
	})
}

// QueueDepth returns the current pending job count. O(buckets) — cheap.
func (s *Store) QueueDepth() (int, error) {
	var n int
	err := s.db.View(func(tx *bolt.Tx) error {
		n = tx.Bucket(bktQueue).Stats().KeyN
		return nil
	})
	return n, err
}

// --- registry bucket -----------------------------------------------------

// SaveSession persists the session_id assigned by the server in RegisterAck.
func (s *Store) SaveSession(sessionID string, serverTimeMs int64) error {
	enc, err := seal(s.aead, []byte(sessionID))
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bktRegistry)
		if err := b.Put(keySessionID, enc); err != nil {
			return err
		}
		return b.Put(keyServerTime, u64Bytes(uint64(serverTimeMs)))
	})
}

// LoadSession returns the last known session_id, or ErrNotFound.
func (s *Store) LoadSession() (string, error) {
	var sid string
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bktRegistry).Get(keySessionID)
		if v == nil {
			return ErrNotFound
		}
		pt, err := open(s.aead, v)
		if err != nil {
			return err
		}
		sid = string(pt)
		return nil
	})
	if err != nil {
		return "", err
	}
	return sid, nil
}

// RecordHeartbeat writes the wall-clock time of the last successful heartbeat.
func (s *Store) RecordHeartbeat(tsMs int64) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bktRegistry).Put(keyHeartbeat, u64Bytes(uint64(tsMs)))
	})
}

// LastHeartbeat returns the timestamp written by RecordHeartbeat, or 0.
func (s *Store) LastHeartbeat() (int64, error) {
	var ts int64
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bktRegistry).Get(keyHeartbeat)
		if v != nil {
			ts = int64(bytesToU64(v))
		}
		return nil
	})
	return ts, err
}

// --- logs buffer ---------------------------------------------------------

// BufferLog appends a log payload keyed by timestamp+ordinal so iteration
// returns chronological order. The key encodes ts_ms (big-endian) so bbolt
// sorts naturally.
func (s *Store) BufferLog(tsMs int64, payload []byte) error {
	enc, err := seal(s.aead, payload)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bktLogs)
		// Sequence number ensures uniqueness when ts collides.
		seq, _ := b.NextSequence()
		key := make([]byte, 8+8)
		binary.BigEndian.PutUint64(key[:8], uint64(tsMs))
		binary.BigEndian.PutUint64(key[8:], seq)
		return b.Put(key, enc)
	})
}

// DrainLogs returns up to n buffered log payloads in chronological order
// and removes them from the buffer in the same transaction.
func (s *Store) DrainLogs(n int) ([][]byte, error) {
	if n <= 0 {
		return nil, nil
	}
	out := make([][]byte, 0, n)
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bktLogs)
		c := b.Cursor()
		var keys [][]byte
		for k, v := c.First(); k != nil && len(out) < n; k, v = c.Next() {
			pt, err := open(s.aead, v)
			if err != nil {
				return err
			}
			out = append(out, pt)
			kc := make([]byte, len(k))
			copy(kc, k)
			keys = append(keys, kc)
		}
		for _, k := range keys {
			if err := b.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// --- helpers --------------------------------------------------------------

func u64Bytes(n uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, n)
	return b
}

func bytesToU64(b []byte) uint64 {
	if len(b) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}
