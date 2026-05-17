package pipeline

import (
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

// CompressZstd streams `in` through a zstd encoder into `out`.
//
// Returns:
//   - originalBytes  : plaintext bytes consumed from `in`
//   - compressedBytes: zstd-framed bytes written to `out`
//
// The encoder is created with the default level (SpeedDefault, ~level 3) —
// a sensible balance between ratio and CPU for streaming DB dumps. Callers
// who need a different level should use the lower-level zstdWriter directly.
func CompressZstd(in io.Reader, out io.Writer) (int64, int64, error) {
	cw := &countingWriter{w: out}
	enc, err := zstd.NewWriter(cw, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return 0, 0, fmt.Errorf("pipeline: zstd new writer: %w", err)
	}
	// NOTE: do NOT defer enc.Close() here. klauspost's zstd writer emits
	// an additional empty frame on a second Close() — if we both defer and
	// close explicitly, the trailing 4 bytes corrupt the downstream stream
	// and skew `cw.n`. We close exactly once below and ensure the encoder
	// is released on error paths too.

	n, err := io.Copy(enc, in)
	if err != nil {
		_ = enc.Close()
		return n, cw.n, fmt.Errorf("pipeline: zstd copy: %w", err)
	}
	if err := enc.Close(); err != nil {
		return n, cw.n, fmt.Errorf("pipeline: zstd close: %w", err)
	}
	return n, cw.n, nil
}

// NewZstdWriter wraps `out` in a zstd encoder. Callers MUST Close the
// returned writer before the stream is considered final — the trailer
// would otherwise be missing.
func NewZstdWriter(out io.Writer) (io.WriteCloser, error) {
	enc, err := zstd.NewWriter(out, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, fmt.Errorf("pipeline: zstd new writer: %w", err)
	}
	return enc, nil
}

// countingWriter is an io.Writer that counts bytes written to the
// wrapped writer. Used to measure compressed output size without
// double-buffering.
type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}
