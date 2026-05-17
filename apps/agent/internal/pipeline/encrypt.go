// Backupy chunk-stream encryption format (v1).
//
// The agent encrypts the (already-compressed) backup as a sequence of
// independent AES-256-GCM frames. The output is appended to the upload
// stream byte-by-byte — no length prefix on the whole blob, no header.
//
// Wire format (all integers big-endian):
//
//   chunk := uint32 ciphertext_len      // bytes that follow, EXCLUDING this u32
//            12-byte random nonce       // unique per chunk
//            ciphertext (≤ chunkPlainSize + 16-byte GCM tag)
//
// EOF marker: a single chunk with ciphertext_len == 0. The decryptor
// treats this as "stream finished cleanly" — without it, a truncated
// upload would be indistinguishable from a clean end.
//
// Chunk plaintext size is CHUNK_PLAIN_SIZE = 1 MiB. Larger frames waste
// memory on the decryptor; smaller frames pay too much per-chunk overhead.
//
// The DEK is 32 bytes (AES-256). It is supplied by the server in
// RunBackup.encrypted_dek, decrypted by the agent runtime (KMS path —
// not covered here), and discarded once the upload completes. The
// envelope ciphertext (KMS-wrapped DEK) is passed THROUGH the agent
// unchanged in BackupCompleted.encrypted_dek so the server can persist
// it alongside the backup row.
//
// The backupy-decrypt CLI inverts this format. Keep the constants below
// in sync with apps/backupy-decrypt — they are the single source of truth.

package pipeline

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	// ChunkPlainSize is the maximum plaintext bytes per AES-GCM frame.
	// 1 MiB keeps decryption memory bounded while keeping per-chunk
	// overhead (16 byte tag + 12 byte nonce + 4 byte header) negligible.
	ChunkPlainSize = 1 << 20

	// dekSize is the expected DEK length (AES-256).
	dekSize = 32

	// nonceSize matches AES-GCM's standard 96-bit nonce. The stdlib
	// rejects any other length when GCM.NonceSize() is honoured.
	nonceSize = 12

	// gcmTagSize is the GCM authentication tag length (16 bytes).
	gcmTagSize = 16

	// chunkHeaderSize is the 4-byte big-endian length prefix per chunk.
	chunkHeaderSize = 4
)

// Encryptor encrypts arbitrarily large streams using AES-256-GCM with a
// per-chunk random nonce. Construct one per backup run — reuse across
// runs is allowed but cheap to avoid.
type Encryptor struct {
	dek []byte
	aead cipher.AEAD
}

// NewEncryptor builds an Encryptor from a 32-byte DEK.
func NewEncryptor(dek []byte) (*Encryptor, error) {
	if len(dek) != dekSize {
		return nil, fmt.Errorf("pipeline: DEK must be %d bytes, got %d", dekSize, len(dek))
	}
	block, err := aes.NewCipher(dek)
	if err != nil {
		return nil, fmt.Errorf("pipeline: aes new cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("pipeline: gcm new: %w", err)
	}
	// Defensive: confirm the stdlib's nonce expectation matches our constant.
	if aead.NonceSize() != nonceSize {
		return nil, fmt.Errorf("pipeline: gcm nonce size mismatch: %d", aead.NonceSize())
	}
	// Keep a copy so the caller may overwrite the slice afterwards.
	keyCopy := make([]byte, len(dek))
	copy(keyCopy, dek)
	return &Encryptor{dek: keyCopy, aead: aead}, nil
}

// Stream reads plaintext from `in` in ChunkPlainSize chunks, encrypts
// each one with a fresh random nonce, and writes framed ciphertext to
// `out`. After EOF on `in` it writes the zero-length terminator chunk.
//
// Returns the total number of PLAINTEXT bytes consumed from `in`.
func (e *Encryptor) Stream(in io.Reader, out io.Writer) (int64, error) {
	if e == nil || e.aead == nil {
		return 0, errors.New("pipeline: nil Encryptor")
	}
	buf := make([]byte, ChunkPlainSize)
	header := make([]byte, chunkHeaderSize)
	nonce := make([]byte, nonceSize)
	var total int64

	for {
		n, readErr := io.ReadFull(in, buf)
		if n > 0 {
			if _, err := rand.Read(nonce); err != nil {
				return total, fmt.Errorf("pipeline: read nonce: %w", err)
			}
			ct := e.aead.Seal(nil, nonce, buf[:n], nil)
			// Frame: u32(len) || nonce || ciphertext+tag
			binary.BigEndian.PutUint32(header, uint32(len(nonce)+len(ct)))
			if _, err := out.Write(header); err != nil {
				return total, fmt.Errorf("pipeline: write chunk header: %w", err)
			}
			if _, err := out.Write(nonce); err != nil {
				return total, fmt.Errorf("pipeline: write chunk nonce: %w", err)
			}
			if _, err := out.Write(ct); err != nil {
				return total, fmt.Errorf("pipeline: write chunk ciphertext: %w", err)
			}
			total += int64(n)
		}
		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
			break
		}
		if readErr != nil {
			return total, fmt.Errorf("pipeline: read plaintext: %w", readErr)
		}
	}

	// EOF marker: zero-length chunk.
	binary.BigEndian.PutUint32(header, 0)
	if _, err := out.Write(header); err != nil {
		return total, fmt.Errorf("pipeline: write eof marker: %w", err)
	}
	return total, nil
}

// Decrypt is the inverse of Stream — used by tests and the
// backupy-decrypt CLI. Validates GCM tags and the EOF marker.
func (e *Encryptor) Decrypt(in io.Reader, out io.Writer) (int64, error) {
	if e == nil || e.aead == nil {
		return 0, errors.New("pipeline: nil Encryptor")
	}
	header := make([]byte, chunkHeaderSize)
	var total int64
	for {
		if _, err := io.ReadFull(in, header); err != nil {
			if errors.Is(err, io.EOF) {
				// Stream ended without an explicit terminator — refuse.
				return total, errors.New("pipeline: encrypted stream truncated (no EOF marker)")
			}
			return total, fmt.Errorf("pipeline: read chunk header: %w", err)
		}
		size := binary.BigEndian.Uint32(header)
		if size == 0 {
			return total, nil // clean EOF
		}
		if size < uint32(nonceSize+gcmTagSize) {
			return total, fmt.Errorf("pipeline: chunk size %d below minimum", size)
		}
		frame := make([]byte, size)
		if _, err := io.ReadFull(in, frame); err != nil {
			return total, fmt.Errorf("pipeline: read chunk body: %w", err)
		}
		nonce := frame[:nonceSize]
		ct := frame[nonceSize:]
		pt, err := e.aead.Open(nil, nonce, ct, nil)
		if err != nil {
			return total, fmt.Errorf("pipeline: gcm open: %w", err)
		}
		if _, err := out.Write(pt); err != nil {
			return total, fmt.Errorf("pipeline: write plaintext: %w", err)
		}
		total += int64(len(pt))
	}
}
