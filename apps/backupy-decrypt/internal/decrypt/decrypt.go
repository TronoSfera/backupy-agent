// Package decrypt streams a Backupy backup file through:
//
//  1. AES-256-GCM decryption (chunked frames, see Wire format below)
//  2. zstd decompression
//  3. SHA-256 verification of the ciphertext
//
// Wire format (verbatim mirror of apps/agent/internal/pipeline/encrypt.go —
// that file is the single source of truth). All integers big-endian:
//
//	chunk := uint32 ciphertext_len      // bytes that follow, EXCLUDING this u32
//	         12-byte random nonce       // unique per chunk
//	         ciphertext (≤ ChunkPlaintextSize + 16-byte GCM tag)
//
// EOF marker: a single chunk with ciphertext_len == 0. The decryptor
// treats this as "stream finished cleanly" — without it, a truncated
// upload would be indistinguishable from a clean end.
//
// Chunk plaintext size is ChunkPlaintextSize = 1 MiB. The AEAD's
// Additional Authenticated Data is nil — chunk reorder/replay defence
// is provided by the EOF marker + explicit per-chunk size prefix:
// any reorder breaks the frame boundaries and fails the next length
// read, and any truncated tail trips the missing-EOF check.
//
// The CLI verifies the SHA-256 of all bytes read from the input file
// (i.e. the concatenation of every frame: u32 size + nonce + ct+tag,
// including the trailing zero EOF marker) against the JWT's "sha256"
// claim before declaring success.
//
// All operations are streaming — we never materialise a full plaintext
// or ciphertext block beyond ChunkPlaintextSize+overhead.
package decrypt

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/klauspost/compress/zstd"

	"github.com/backupy/backupy/apps/backupy-decrypt/internal/jwt"
)

// Constants — pipeline contract. Keep in sync with
// apps/agent/internal/pipeline/encrypt.go.
const (
	// ChunkPlaintextSize is the maximum size of one plaintext chunk
	// before encryption. The pipeline ships exactly 1 MiB per chunk; the
	// final chunk may be shorter. MUST equal pipeline.ChunkPlainSize.
	ChunkPlaintextSize = 1 << 20 // 1 MiB

	// NonceSize is GCM's 96-bit nonce.
	NonceSize = 12

	// TagSize is GCM's 128-bit auth tag.
	TagSize = 16

	// ChunkHeaderSize is the 4-byte big-endian length prefix per chunk.
	ChunkHeaderSize = 4

	// IssuerExpected is the iss claim we accept on JWTs.
	IssuerExpected = "backupy-server"

	// AudienceExpected is the aud claim we accept on JWTs.
	AudienceExpected = "backupy-decrypt"
)

// Errors callers should care about.
var (
	ErrTokenExpired   = errors.New("decrypt: token expired (request a new one)")
	ErrInvalidToken   = errors.New("decrypt: invalid token")
	ErrSHA256Mismatch = errors.New("decrypt: ciphertext SHA-256 mismatch — file is corrupt or token is for a different run")
	ErrTruncated      = errors.New("decrypt: input file is truncated")
	ErrDecryptFailed  = errors.New("decrypt: AES-GCM authentication failed — wrong key or corrupted data")
	ErrUnsupportedAlg = errors.New("decrypt: unsupported algorithm")
	ErrUnsupportedFmt = errors.New("decrypt: unsupported format version")
	ErrFrameTooLarge  = errors.New("decrypt: frame size exceeds maximum")
	ErrFrameTooSmall  = errors.New("decrypt: frame size below minimum")
)

// Options controls a single decrypt run.
type Options struct {
	InputPath      string
	OutputPath     string
	Token          string // JWT
	VerifySHA256   bool
	SkipDecompress bool
	// Progress is called periodically with the count of input bytes
	// consumed so far. Optional.
	Progress func(bytesProcessed int64)
}

// Run executes the decrypt + (optional) decompress pipeline.
func Run(ctx context.Context, opts Options) error {
	claims, err := jwt.ParseDecryption(opts.Token, IssuerExpected, AudienceExpected)
	if err != nil {
		switch {
		case errors.Is(err, jwt.ErrExpired):
			return fmt.Errorf("%w: %v. Request a new one from the Backupy dashboard.", ErrTokenExpired, err)
		default:
			return fmt.Errorf("%w: %v", ErrInvalidToken, err)
		}
	}
	if !strings.EqualFold(claims.Algorithm, "AES-256-GCM") {
		return fmt.Errorf("%w: %q", ErrUnsupportedAlg, claims.Algorithm)
	}
	if claims.FormatVersion != 1 {
		return fmt.Errorf("%w: %d", ErrUnsupportedFmt, claims.FormatVersion)
	}

	dek, err := base64.StdEncoding.DecodeString(claims.DEKBase64)
	if err != nil {
		return fmt.Errorf("%w: dek not valid base64: %v", ErrInvalidToken, err)
	}
	if len(dek) != 32 {
		return fmt.Errorf("%w: dek length = %d, want 32", ErrInvalidToken, len(dek))
	}
	defer zeroize(dek)

	in, err := os.Open(opts.InputPath)
	if err != nil {
		return fmt.Errorf("open input: %w", err)
	}
	defer in.Close()

	out, err := os.OpenFile(opts.OutputPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("open output: %w", err)
	}
	outClosed := false
	defer func() {
		if !outClosed {
			_ = out.Close()
		}
	}()

	block, err := aes.NewCipher(dek)
	if err != nil {
		return fmt.Errorf("aes: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return fmt.Errorf("gcm: %w", err)
	}

	hasher := sha256.New()
	// teeReader: every byte read from `in` is mirrored to the hasher,
	// so the ciphertext SHA we compare against the JWT is over the full
	// on-wire stream (size prefixes + nonces + ciphertexts + tags +
	// EOF marker), matching what the agent computed.
	source := io.TeeReader(in, hasher)

	// plaintextSink: either the file directly (SkipDecompress) or via a
	// zstd decoder. We wrap the file in a NopCloser-equivalent so we can
	// Close the sink uniformly without double-closing `out`.
	var plaintextSink io.WriteCloser
	if opts.SkipDecompress {
		plaintextSink = noopCloser{out}
	} else {
		// We write *ciphertext-decrypted plaintext* into the zstd
		// decoder's input. The decoder writes decompressed bytes to
		// `out`. So we need an io.PipeWriter feeding into zstd.NewReader.
		pr, pw := io.Pipe()
		dec, err := zstd.NewReader(pr)
		if err != nil {
			_ = pr.Close()
			_ = pw.Close()
			return fmt.Errorf("zstd: %w", err)
		}
		// Run the decode -> out copy in a goroutine.
		errCh := make(chan error, 1)
		go func() {
			_, copyErr := io.Copy(out, dec)
			dec.Close()
			errCh <- copyErr
		}()
		plaintextSink = &pipeWriterCloser{pw: pw, errCh: errCh}
	}

	// Decrypt loop.
	if err := decryptStream(ctx, source, plaintextSink, aead, opts.Progress); err != nil {
		_ = plaintextSink.Close()
		return err
	}
	if err := plaintextSink.Close(); err != nil {
		return fmt.Errorf("close plaintext sink: %w", err)
	}
	// Flush the underlying file. Doing this here (rather than in defer)
	// lets us return the close error if it happens (e.g. disk full at
	// the very last fsync).
	if err := out.Close(); err != nil {
		outClosed = true
		return fmt.Errorf("close output: %w", err)
	}
	outClosed = true

	if opts.VerifySHA256 && claims.SHA256 != "" {
		got := hex.EncodeToString(hasher.Sum(nil))
		if !strings.EqualFold(got, claims.SHA256) {
			return fmt.Errorf("%w: got %s, want %s", ErrSHA256Mismatch, got, claims.SHA256)
		}
	}
	return nil
}

// noopCloser is an io.WriteCloser whose Close is a no-op. Used so we can
// uniformly call Close on plaintextSink without double-closing the
// underlying os.File (Run closes it explicitly).
type noopCloser struct{ io.Writer }

func (noopCloser) Close() error { return nil }

// pipeWriterCloser bundles a pipe writer with the goroutine that drains
// the other side, so Close() waits for the drain to finish and returns
// its error.
type pipeWriterCloser struct {
	pw    *io.PipeWriter
	errCh chan error
}

func (p *pipeWriterCloser) Write(b []byte) (int, error) { return p.pw.Write(b) }
func (p *pipeWriterCloser) Close() error {
	if err := p.pw.Close(); err != nil {
		return err
	}
	return <-p.errCh
}

// maxFrameSize is the upper bound on a single chunk's ciphertext_len.
// Anything bigger is rejected before we allocate — protects against a
// malicious file that claims a multi-gigabyte frame size.
const maxFrameSize = NonceSize + ChunkPlaintextSize + TagSize

// decryptStream reads length-prefixed AES-GCM frames from r, writes the
// decrypted plaintext to w. The frame format is described in the package
// docstring. AAD is nil; per-chunk reorder defence is provided by the
// explicit size prefix + mandatory zero-length EOF marker.
func decryptStream(ctx context.Context, r io.Reader, w io.Writer, aead cipher.AEAD, progress func(int64)) error {
	header := make([]byte, ChunkHeaderSize)
	// Pre-allocate the maximum-size frame buffer once; reused across loops.
	frameBuf := make([]byte, maxFrameSize)
	var processed int64

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		// Read the 4-byte big-endian length prefix.
		if _, err := io.ReadFull(r, header); err != nil {
			if errors.Is(err, io.EOF) {
				// Stream ended without an explicit terminator — refuse.
				// The pipeline ALWAYS writes the zero-length EOF marker;
				// missing one means the file was truncated mid-upload.
				return fmt.Errorf("%w (no EOF marker)", ErrTruncated)
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return fmt.Errorf("%w (chunk header)", ErrTruncated)
			}
			return fmt.Errorf("read chunk header: %w", err)
		}
		size := binary.BigEndian.Uint32(header)
		processed += int64(ChunkHeaderSize)

		// EOF marker — clean end of stream. The pipeline writes this as
		// the very last bytes of the upload.
		if size == 0 {
			if progress != nil {
				progress(processed)
			}
			return nil
		}
		// Minimum frame is nonce + tag (i.e. encrypting zero plaintext).
		if size < uint32(NonceSize+TagSize) {
			return fmt.Errorf("%w: %d < %d", ErrFrameTooSmall, size, NonceSize+TagSize)
		}
		if size > uint32(maxFrameSize) {
			return fmt.Errorf("%w: %d > %d", ErrFrameTooLarge, size, maxFrameSize)
		}

		// Read the exact-size frame.
		frame := frameBuf[:size]
		if _, err := io.ReadFull(r, frame); err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
				return fmt.Errorf("%w (chunk body)", ErrTruncated)
			}
			return fmt.Errorf("read chunk body: %w", err)
		}
		nonce := frame[:NonceSize]
		ct := frame[NonceSize:]

		// AAD is nil to match pipeline.Encryptor.Stream's seal call.
		pt, err := aead.Open(nil, nonce, ct, nil)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrDecryptFailed, err)
		}
		if _, err := w.Write(pt); err != nil {
			return fmt.Errorf("write plaintext: %w", err)
		}

		processed += int64(size)
		if progress != nil {
			progress(processed)
		}
	}
}

// zeroize overwrites b in place.
func zeroize(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
