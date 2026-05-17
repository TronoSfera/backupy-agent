//go:build integration

// Cross-binary integration test: prove the agent's pipeline.Encryptor
// produces a byte stream that the backupy-decrypt CLI can consume.
//
// This is the GOLDEN TEST for Phase 1. If this passes, the entire
// backup → download → decrypt loop works end-to-end at the byte level.
//
// Run via:
//
//	cd apps/backupy-decrypt && go test -tags=integration ./...
//
// The test depends on the agent module via a `replace` directive in
// go.mod, which in turn depends on the generated proto bindings. So
// `make proto` must have been run from the repo root first.
package backupydecrypt_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/backupy/backupy/apps/agent/internal/pipeline"
	"github.com/backupy/backupy/apps/backupy-decrypt/internal/decrypt"
)

// craftToken builds a JWT in the shape the CLI expects. The signature
// is ignored — the CLI verifies neither HMAC nor RSA, only the claim
// envelope and the SHA-256 inside it.
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
		"run_id":         "run-integration",
		"company_id":     "co-integration",
		"dek":            base64.StdEncoding.EncodeToString(dek),
		"alg":            "AES-256-GCM",
		"format_version": 1,
		"sha256":         sha,
	}
	pld, err := json.Marshal(claims)
	require.NoError(t, err)
	enc := base64.RawURLEncoding
	signingInput := enc.EncodeToString(hdrJSON) + "." + enc.EncodeToString(pld)
	return signingInput + "." + enc.EncodeToString([]byte("ignored"))
}

// TestCrossBinary_PipelineToDecrypt is the bytewise round-trip check.
func TestCrossBinary_PipelineToDecrypt(t *testing.T) {
	cases := []struct {
		name string
		size int
	}{
		{"small_46B", 46},
		{"single_full_chunk_1MiB", pipeline.ChunkPlainSize},
		{"two_chunks_plus_remainder", 2*pipeline.ChunkPlainSize + 1234},
		{"empty", 0},
	}
	// Compile-time sanity: the two sides MUST agree on the chunk size.
	require.Equal(t, pipeline.ChunkPlainSize, decrypt.ChunkPlaintextSize,
		"chunk size constant drift between pipeline and decrypt — re-sync")

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plaintext := make([]byte, tc.size)
			_, err := rand.Read(plaintext)
			require.NoError(t, err)

			dek := make([]byte, 32)
			_, err = rand.Read(dek)
			require.NoError(t, err)

			// Encrypt with the agent's pipeline.
			enc, err := pipeline.NewEncryptor(dek)
			require.NoError(t, err)
			var ctBuf bytes.Buffer
			n, err := enc.Stream(bytes.NewReader(plaintext), &ctBuf)
			require.NoError(t, err)
			require.Equal(t, int64(len(plaintext)), n)
			ciphertext := ctBuf.Bytes()

			// SHA-256 of the on-wire bytes — what the JWT carries.
			sum := sha256.Sum256(ciphertext)
			shaHex := hex.EncodeToString(sum[:])

			// Write to disk, decrypt with the CLI's package.
			dir := t.TempDir()
			inPath := filepath.Join(dir, "in.enc")
			outPath := filepath.Join(dir, "out.bin")
			require.NoError(t, os.WriteFile(inPath, ciphertext, 0o600))

			tok := craftToken(t, dek, shaHex)
			err = decrypt.Run(context.Background(), decrypt.Options{
				InputPath:      inPath,
				OutputPath:     outPath,
				Token:          tok,
				VerifySHA256:   true,
				SkipDecompress: true,
			})
			require.NoError(t, err, "decrypt.Run must accept pipeline output")

			got, err := os.ReadFile(outPath)
			require.NoError(t, err)
			require.Equal(t, plaintext, got, "round-trip must be bytewise identical")
		})
	}
}
