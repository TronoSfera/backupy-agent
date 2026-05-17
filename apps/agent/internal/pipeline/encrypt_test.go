package pipeline

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func newDEK(t *testing.T) []byte {
	t.Helper()
	dek := make([]byte, 32)
	_, err := rand.Read(dek)
	require.NoError(t, err)
	return dek
}

func TestEncryptor_RoundTrip_5MB(t *testing.T) {
	plaintext := make([]byte, 5<<20)
	_, err := rand.Read(plaintext)
	require.NoError(t, err)

	e, err := NewEncryptor(newDEK(t))
	require.NoError(t, err)

	var ct bytes.Buffer
	consumed, err := e.Stream(bytes.NewReader(plaintext), &ct)
	require.NoError(t, err)
	require.Equal(t, int64(len(plaintext)), consumed)

	var round bytes.Buffer
	pt, err := e.Decrypt(&ct, &round)
	require.NoError(t, err)
	require.Equal(t, int64(len(plaintext)), pt)
	require.Equal(t, plaintext, round.Bytes())
}

func TestEncryptor_RoundTrip_Empty(t *testing.T) {
	e, err := NewEncryptor(newDEK(t))
	require.NoError(t, err)

	var ct bytes.Buffer
	consumed, err := e.Stream(bytes.NewReader(nil), &ct)
	require.NoError(t, err)
	require.Equal(t, int64(0), consumed)

	var round bytes.Buffer
	pt, err := e.Decrypt(&ct, &round)
	require.NoError(t, err)
	require.Equal(t, int64(0), pt)
	require.Empty(t, round.Bytes())
}

func TestEncryptor_WrongKeyFails(t *testing.T) {
	plaintext := bytes.Repeat([]byte("hello"), 1000)
	e1, err := NewEncryptor(newDEK(t))
	require.NoError(t, err)
	e2, err := NewEncryptor(newDEK(t))
	require.NoError(t, err)

	var ct bytes.Buffer
	_, err = e1.Stream(bytes.NewReader(plaintext), &ct)
	require.NoError(t, err)

	var round bytes.Buffer
	_, err = e2.Decrypt(&ct, &round)
	require.Error(t, err, "decrypt with a different DEK must fail GCM tag check")
}

func TestEncryptor_TamperingDetected(t *testing.T) {
	dek := newDEK(t)
	e, err := NewEncryptor(dek)
	require.NoError(t, err)

	plaintext := bytes.Repeat([]byte("abcd"), 4096)
	var ct bytes.Buffer
	_, err = e.Stream(bytes.NewReader(plaintext), &ct)
	require.NoError(t, err)

	// Flip a bit inside the first ciphertext chunk (skip header + nonce).
	tampered := ct.Bytes()
	flipAt := chunkHeaderSize + nonceSize + 1
	require.Greater(t, len(tampered), flipAt)
	tampered[flipAt] ^= 0x01

	var round bytes.Buffer
	_, err = e.Decrypt(bytes.NewReader(tampered), &round)
	require.Error(t, err, "bit-flip in ciphertext must trigger a GCM tag failure")
}

func TestEncryptor_RejectsBadDEKLength(t *testing.T) {
	_, err := NewEncryptor(make([]byte, 16))
	require.Error(t, err)
	_, err = NewEncryptor(make([]byte, 31))
	require.Error(t, err)
	_, err = NewEncryptor(make([]byte, 33))
	require.Error(t, err)
}

func TestEncryptor_TruncatedStreamFails(t *testing.T) {
	e, err := NewEncryptor(newDEK(t))
	require.NoError(t, err)

	var ct bytes.Buffer
	_, err = e.Stream(bytes.NewReader(bytes.Repeat([]byte("X"), 4096)), &ct)
	require.NoError(t, err)

	// Drop the last 4 bytes of the EOF marker.
	b := ct.Bytes()
	truncated := b[:len(b)-2]

	var round bytes.Buffer
	_, err = e.Decrypt(bytes.NewReader(truncated), &round)
	require.Error(t, err, "missing EOF marker must be detected")
}
