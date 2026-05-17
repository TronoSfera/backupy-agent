// State-at-rest encryption helpers.
//
// All bucket *values* are wrapped with AES-256-GCM using a key derived from
// BACKUP_AGENT_KEY via HKDF-SHA256 (per docs/03-agent-spec.md →
// "Шифрование state опционально (key derived из BACKUP_AGENT_KEY)").
//
// Wire format on disk:
//
//	[1 byte version=0x01] [12 bytes nonce] [ciphertext] [16 bytes GCM tag]
//
// Bucket *keys* (e.g. job ids) are stored in clear because BoltDB relies on
// the byte-ordering of keys for iteration and a deterministic encryption of
// keys would still leak ordering information without buying much.
package state

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"

	"golang.org/x/crypto/hkdf"
)

const (
	stateKeyLen     = 32           // AES-256
	stateNonceLen   = 12           // GCM standard
	stateEnvVersion = byte(0x01)   // bump on format change
	hkdfInfo        = "backupy-agent/state-v1"
	hkdfSalt        = "backupy-agent-state-salt"
)

// errCipher indicates corrupt or wrong-key state. Surfaced as-is so the
// agent can refuse to start instead of silently overwriting good data.
var errCipher = errors.New("state: cipher open failed (corrupt or wrong key)")

// deriveStateKey returns a 32-byte AES-256 key derived from the agent key.
// HKDF is overkill for a single derivation step, but it costs nothing and
// keeps us compliant with NIST SP 800-108 should we ever need to rotate.
func deriveStateKey(agentKey string) ([]byte, error) {
	if agentKey == "" {
		return nil, errors.New("state: empty agent key — cannot derive encryption key")
	}
	r := hkdf.New(sha256New, []byte(agentKey), []byte(hkdfSalt), []byte(hkdfInfo))
	key := make([]byte, stateKeyLen)
	if _, err := io.ReadFull(r, key); err != nil {
		return nil, fmt.Errorf("state: hkdf read: %w", err)
	}
	return key, nil
}

// newGCM constructs a fresh AEAD around a 32-byte AES key.
func newGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("state: aes init: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("state: gcm init: %w", err)
	}
	return aead, nil
}

// seal returns version || nonce || ciphertext+tag.
func seal(aead cipher.AEAD, plaintext []byte) ([]byte, error) {
	nonce := make([]byte, stateNonceLen)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("state: rand: %w", err)
	}
	out := make([]byte, 0, 1+stateNonceLen+len(plaintext)+aead.Overhead())
	out = append(out, stateEnvVersion)
	out = append(out, nonce...)
	out = aead.Seal(out, nonce, plaintext, nil)
	return out, nil
}

// open reverses seal. Returns errCipher on any decryption failure so callers
// can distinguish "no such record" (bbolt nil) from "tampered/wrong key".
func open(aead cipher.AEAD, sealed []byte) ([]byte, error) {
	if len(sealed) < 1+stateNonceLen+aead.Overhead() {
		return nil, errCipher
	}
	if sealed[0] != stateEnvVersion {
		return nil, fmt.Errorf("state: unsupported envelope version 0x%02x", sealed[0])
	}
	nonce := sealed[1 : 1+stateNonceLen]
	ct := sealed[1+stateNonceLen:]
	pt, err := aead.Open(nil, nonce, ct, nil)
	if err != nil {
		return nil, errCipher
	}
	return pt, nil
}
