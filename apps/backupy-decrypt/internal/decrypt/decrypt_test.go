package decrypt

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

// encryptFixture mirrors the agent pipeline's encryption format so tests
// can produce input files that the CLI must decrypt successfully.
//
// MUST match decryptStream's layout. The pipeline's wire format is:
//
//	repeated:
//	    uint32 big-endian   ciphertext_len (= nonce + ct + tag bytes)
//	    12 bytes            random nonce
//	    ct+tag              AES-256-GCM Seal output (AAD = nil)
//	terminator:
//	    uint32 big-endian   0
//
// The final plaintext chunk may be shorter than ChunkPlaintextSize.
func encryptFixture(t *testing.T, dek []byte, plaintext []byte) []byte {
	t.Helper()
	block, err := aes.NewCipher(dek)
	require.NoError(t, err)
	aead, err := cipher.NewGCM(block)
	require.NoError(t, err)

	var out []byte
	header := make([]byte, ChunkHeaderSize)

	off := 0
	for off < len(plaintext) {
		end := off + ChunkPlaintextSize
		if end > len(plaintext) {
			end = len(plaintext)
		}
		chunk := plaintext[off:end]
		nonce := make([]byte, NonceSize)
		_, err := rand.Read(nonce)
		require.NoError(t, err)
		ct := aead.Seal(nil, nonce, chunk, nil)
		binary.BigEndian.PutUint32(header, uint32(len(nonce)+len(ct)))
		out = append(out, header...)
		out = append(out, nonce...)
		out = append(out, ct...)
		off = end
	}
	// EOF marker — always present, even for empty plaintext.
	binary.BigEndian.PutUint32(header, 0)
	out = append(out, header...)
	return out
}

// zstdCompress is the agent-side compression step.
func zstdCompress(t *testing.T, plaintext []byte) []byte {
	t.Helper()
	enc, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	defer enc.Close()
	return enc.EncodeAll(plaintext, nil)
}

// craftToken builds a JWT identical in shape to what the server issues,
// so the CLI accepts it.
func craftToken(t *testing.T, dek []byte, sha string) string {
	t.Helper()
	hdr := map[string]string{"alg": "HS256", "typ": "JWT"}
	hdrJSON, err := json.Marshal(hdr)
	require.NoError(t, err)
	claims := map[string]any{
		"iss":            "backupy-server",
		"sub":            "user-1",
		"aud":            "backupy-decrypt",
		"iat":            time.Now().Unix(),
		"exp":            time.Now().Add(15 * time.Minute).Unix(),
		"run_id":         "run-1",
		"company_id":     "co-1",
		"dek":            base64.StdEncoding.EncodeToString(dek),
		"alg":            "AES-256-GCM",
		"format_version": 1,
		"sha256":         sha,
	}
	pld, err := json.Marshal(claims)
	require.NoError(t, err)
	enc := base64.RawURLEncoding
	signingInput := enc.EncodeToString(hdrJSON) + "." + enc.EncodeToString(pld)
	// Signature value doesn't matter — CLI doesn't verify HMAC.
	return signingInput + "." + enc.EncodeToString([]byte("ignored"))
}

func writeFile(t *testing.T, dir, name string, data []byte) string {
	t.Helper()
	p := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(p, data, 0o600))
	return p
}

func TestDecrypt_RoundTrip_Compressed(t *testing.T) {
	dir := t.TempDir()
	plaintext := []byte("Hello, Backupy! This is a small test backup.\n")
	compressed := zstdCompress(t, plaintext)

	dek := make([]byte, 32)
	_, _ = rand.Read(dek)
	encrypted := encryptFixture(t, dek, compressed)
	sum := sha256.Sum256(encrypted)
	sha := hex.EncodeToString(sum[:])

	in := writeFile(t, dir, "backup.enc", encrypted)
	out := filepath.Join(dir, "backup.sql")
	tok := craftToken(t, dek, sha)

	err := Run(context.Background(), Options{
		InputPath:    in,
		OutputPath:   out,
		Token:        tok,
		VerifySHA256: true,
	})
	require.NoError(t, err)

	got, err := os.ReadFile(out)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)
}

func TestDecrypt_RoundTrip_MultiChunk(t *testing.T) {
	dir := t.TempDir()
	// 3 full chunks + a small remainder.
	plaintext := make([]byte, ChunkPlaintextSize*3+1234)
	_, _ = rand.Read(plaintext)

	dek := make([]byte, 32)
	_, _ = rand.Read(dek)
	encrypted := encryptFixture(t, dek, plaintext)
	sum := sha256.Sum256(encrypted)

	in := writeFile(t, dir, "backup.enc", encrypted)
	out := filepath.Join(dir, "backup.bin")
	tok := craftToken(t, dek, hex.EncodeToString(sum[:]))

	err := Run(context.Background(), Options{
		InputPath:      in,
		OutputPath:     out,
		Token:          tok,
		VerifySHA256:   true,
		SkipDecompress: true,
	})
	require.NoError(t, err)

	got, err := os.ReadFile(out)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)
}

func TestDecrypt_RoundTrip_EmptyPlaintext(t *testing.T) {
	// Even an empty payload must produce a valid stream: just the EOF
	// marker. Round-tripping it must yield no plaintext and no error.
	dir := t.TempDir()
	dek := make([]byte, 32)
	_, _ = rand.Read(dek)
	encrypted := encryptFixture(t, dek, nil)
	sum := sha256.Sum256(encrypted)

	in := writeFile(t, dir, "empty.enc", encrypted)
	out := filepath.Join(dir, "empty.out")
	tok := craftToken(t, dek, hex.EncodeToString(sum[:]))

	err := Run(context.Background(), Options{
		InputPath:      in,
		OutputPath:     out,
		Token:          tok,
		VerifySHA256:   true,
		SkipDecompress: true,
	})
	require.NoError(t, err)

	got, err := os.ReadFile(out)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestDecrypt_WrongDEK(t *testing.T) {
	dir := t.TempDir()
	plaintext := []byte("secret stuff")
	correctDEK := make([]byte, 32)
	_, _ = rand.Read(correctDEK)
	encrypted := encryptFixture(t, correctDEK, plaintext)
	in := writeFile(t, dir, "x.enc", encrypted)
	out := filepath.Join(dir, "x.out")

	wrongDEK := make([]byte, 32)
	_, _ = rand.Read(wrongDEK)
	tok := craftToken(t, wrongDEK, "")

	err := Run(context.Background(), Options{
		InputPath:      in,
		OutputPath:     out,
		Token:          tok,
		SkipDecompress: true,
	})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrDecryptFailed)
}

func TestDecrypt_Truncated(t *testing.T) {
	dir := t.TempDir()
	dek := make([]byte, 32)
	_, _ = rand.Read(dek)
	encrypted := encryptFixture(t, dek, []byte("some data here"))
	// Drop the trailing EOF marker (last 4 bytes).
	bad := encrypted[:len(encrypted)-4]
	in := writeFile(t, dir, "x.enc", bad)
	out := filepath.Join(dir, "x.out")

	tok := craftToken(t, dek, "")
	err := Run(context.Background(), Options{
		InputPath:      in,
		OutputPath:     out,
		Token:          tok,
		SkipDecompress: true,
	})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrTruncated)
}

func TestDecrypt_Truncated_MidFrame(t *testing.T) {
	dir := t.TempDir()
	dek := make([]byte, 32)
	_, _ = rand.Read(dek)
	encrypted := encryptFixture(t, dek, []byte("some data here"))
	// Truncate inside the first frame — even the nonce isn't complete.
	bad := encrypted[:ChunkHeaderSize+5]
	in := writeFile(t, dir, "x.enc", bad)
	out := filepath.Join(dir, "x.out")

	tok := craftToken(t, dek, "")
	err := Run(context.Background(), Options{
		InputPath:      in,
		OutputPath:     out,
		Token:          tok,
		SkipDecompress: true,
	})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrTruncated)
}

func TestDecrypt_SHA256Mismatch(t *testing.T) {
	dir := t.TempDir()
	dek := make([]byte, 32)
	_, _ = rand.Read(dek)
	encrypted := encryptFixture(t, dek, []byte("contents"))
	in := writeFile(t, dir, "x.enc", encrypted)
	out := filepath.Join(dir, "x.out")

	// Use a deliberately wrong sha.
	tok := craftToken(t, dek, "deadbeef")
	err := Run(context.Background(), Options{
		InputPath:      in,
		OutputPath:     out,
		Token:          tok,
		VerifySHA256:   true,
		SkipDecompress: true,
	})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrSHA256Mismatch)
}

func TestDecrypt_ExpiredToken(t *testing.T) {
	dir := t.TempDir()
	dek := make([]byte, 32)
	_, _ = rand.Read(dek)
	// Build a JWT with exp in the past.
	hdr := map[string]string{"alg": "HS256", "typ": "JWT"}
	hdrJSON, _ := json.Marshal(hdr)
	cl := map[string]any{
		"iss":            "backupy-server",
		"aud":            "backupy-decrypt",
		"sub":            "u",
		"iat":            time.Now().Add(-time.Hour).Unix(),
		"exp":            time.Now().Add(-time.Minute).Unix(),
		"run_id":         "r",
		"company_id":     "c",
		"dek":            base64.StdEncoding.EncodeToString(dek),
		"alg":            "AES-256-GCM",
		"format_version": 1,
	}
	pld, _ := json.Marshal(cl)
	enc := base64.RawURLEncoding
	tok := enc.EncodeToString(hdrJSON) + "." + enc.EncodeToString(pld) + "." + enc.EncodeToString([]byte("sig"))

	in := writeFile(t, dir, "x.enc", []byte("anything"))
	out := filepath.Join(dir, "x.out")
	err := Run(context.Background(), Options{
		InputPath:      in,
		OutputPath:     out,
		Token:          tok,
		SkipDecompress: true,
	})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrTokenExpired)
}

func TestDecrypt_ContextCancel(t *testing.T) {
	// Make a large fake encrypted stream and cancel before reading.
	dir := t.TempDir()
	plaintext := make([]byte, ChunkPlaintextSize*4)
	dek := make([]byte, 32)
	_, _ = rand.Read(dek)
	encrypted := encryptFixture(t, dek, plaintext)
	in := writeFile(t, dir, "x.enc", encrypted)
	out := filepath.Join(dir, "x.out")
	tok := craftToken(t, dek, "")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := Run(ctx, Options{InputPath: in, OutputPath: out, Token: tok, SkipDecompress: true})
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
}
