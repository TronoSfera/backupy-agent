package pipeline

import (
	"bytes"
	"crypto/rand"
	"io"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

func TestCompressZstd_RoundTrip(t *testing.T) {
	plaintext := make([]byte, 1<<20)
	_, err := rand.Read(plaintext)
	require.NoError(t, err)

	var compressed bytes.Buffer
	orig, ccount, err := CompressZstd(bytes.NewReader(plaintext), &compressed)
	require.NoError(t, err)
	require.Equal(t, int64(len(plaintext)), orig)
	require.Equal(t, int64(compressed.Len()), ccount)

	dec, err := zstd.NewReader(&compressed)
	require.NoError(t, err)
	defer dec.Close()
	round, err := io.ReadAll(dec)
	require.NoError(t, err)
	require.Equal(t, plaintext, round, "decompressed bytes must match the original")
}

func TestCompressZstd_Empty(t *testing.T) {
	var compressed bytes.Buffer
	orig, ccount, err := CompressZstd(bytes.NewReader(nil), &compressed)
	require.NoError(t, err)
	require.Equal(t, int64(0), orig)
	// klauspost/compress >=1.17 emits zero bytes for an empty input
	// (no frame header is written when nothing was buffered). We just
	// require ccount to match the bytes actually written to `compressed`
	// and the round-trip to decompress cleanly to an empty payload.
	require.Equal(t, int64(compressed.Len()), ccount)

	dec, err := zstd.NewReader(&compressed)
	require.NoError(t, err)
	defer dec.Close()
	round, err := io.ReadAll(dec)
	require.NoError(t, err)
	require.Empty(t, round, "empty input must round-trip to empty output")
}

func TestCompressZstd_CompressibleInputShrinks(t *testing.T) {
	plaintext := bytes.Repeat([]byte("ABCDEFGH"), 4096) // 32 KiB of repetition
	var compressed bytes.Buffer
	_, _, err := CompressZstd(bytes.NewReader(plaintext), &compressed)
	require.NoError(t, err)
	require.Less(t, compressed.Len(), len(plaintext)/4, "repetitive input must compress >4×")
}
