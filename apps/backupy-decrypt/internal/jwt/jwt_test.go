package jwt

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// mintToken builds a JWT the same way the server does. The signature is
// intentionally not verified by the CLI; we still produce one so the
// token's structure is valid.
func mintToken(t *testing.T, claims map[string]any) string {
	t.Helper()
	hdr := map[string]string{"alg": "HS256", "typ": "JWT"}
	hdrJSON, err := json.Marshal(hdr)
	require.NoError(t, err)
	pld, err := json.Marshal(claims)
	require.NoError(t, err)
	enc := base64.RawURLEncoding
	signingInput := enc.EncodeToString(hdrJSON) + "." + enc.EncodeToString(pld)
	mac := hmac.New(sha256.New, []byte("any-secret"))
	mac.Write([]byte(signingInput))
	return signingInput + "." + enc.EncodeToString(mac.Sum(nil))
}

func validClaims() map[string]any {
	return map[string]any{
		"iss":            "backupy-server",
		"sub":            "user-123",
		"aud":            "backupy-decrypt",
		"iat":            time.Now().Unix(),
		"exp":            time.Now().Add(time.Minute).Unix(),
		"run_id":         "run-1",
		"company_id":     "company-1",
		"dek":            base64.StdEncoding.EncodeToString(make([]byte, 32)),
		"alg":            "AES-256-GCM",
		"format_version": 1,
		"sha256":         "abc",
	}
}

func TestParseDecryption_Success(t *testing.T) {
	tok := mintToken(t, validClaims())
	c, err := ParseDecryption(tok, "backupy-server", "backupy-decrypt")
	require.NoError(t, err)
	require.Equal(t, "backupy-server", c.Issuer)
	require.Equal(t, "user-123", c.Subject)
	require.Equal(t, "run-1", c.RunID)
	require.Equal(t, "company-1", c.CompanyID)
	require.Equal(t, "AES-256-GCM", c.Algorithm)
	require.Equal(t, 1, c.FormatVersion)
}

func TestParseDecryption_Expired(t *testing.T) {
	cl := validClaims()
	cl["exp"] = time.Now().Add(-time.Minute).Unix()
	tok := mintToken(t, cl)
	_, err := ParseDecryption(tok, "backupy-server", "backupy-decrypt")
	require.ErrorIs(t, err, ErrExpired)
}

func TestParseDecryption_WrongIssuer(t *testing.T) {
	cl := validClaims()
	cl["iss"] = "evil"
	tok := mintToken(t, cl)
	_, err := ParseDecryption(tok, "backupy-server", "backupy-decrypt")
	require.ErrorIs(t, err, ErrWrongIssuer)
}

func TestParseDecryption_WrongAudience(t *testing.T) {
	cl := validClaims()
	cl["aud"] = "other"
	tok := mintToken(t, cl)
	_, err := ParseDecryption(tok, "backupy-server", "backupy-decrypt")
	require.ErrorIs(t, err, ErrWrongAudience)
}

func TestParseDecryption_MissingDEK(t *testing.T) {
	cl := validClaims()
	delete(cl, "dek")
	tok := mintToken(t, cl)
	_, err := ParseDecryption(tok, "backupy-server", "backupy-decrypt")
	require.ErrorIs(t, err, ErrMissingClaim)
}

func TestParseDecryption_Malformed(t *testing.T) {
	_, err := ParseDecryption("not.a.jwt", "backupy-server", "backupy-decrypt")
	require.ErrorIs(t, err, ErrMalformed)
}
