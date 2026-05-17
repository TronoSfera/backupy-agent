// Package jwt parses and validates JWTs issued by the Backupy server
// without verifying the HMAC signature.
//
// Rationale: the CLI cannot know the server's HS256 signing secret, so
// any signature check would be theatre. What we DO check is the token
// structure, the issuer/audience claims, and expiry — that's enough to
// produce a clear error message if the user pastes the wrong string.
//
// If a future server release ships a public-key signature scheme we can
// move to full crypto verification here. For now this is intentionally
// minimal — see apps/server/internal/jwt for the signing side.
package jwt

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Sentinel errors.
var (
	ErrMalformed     = errors.New("jwt: malformed token")
	ErrExpired       = errors.New("jwt: token expired")
	ErrWrongIssuer   = errors.New("jwt: wrong issuer")
	ErrWrongAudience = errors.New("jwt: wrong audience")
	ErrMissingClaim  = errors.New("jwt: missing claim")
)

// DecryptionClaims is the typed view of a decryption-token JWT.
type DecryptionClaims struct {
	Issuer        string
	Subject       string
	Audience      string
	IssuedAt      time.Time
	ExpiresAt     time.Time
	RunID         string
	CompanyID     string
	DEKBase64     string
	Algorithm     string
	FormatVersion int
	SHA256        string
}

// Parse decodes a JWT without verifying the HMAC signature, returns the
// raw claims map. ErrMalformed is returned on any structural failure.
func Parse(token string) (map[string]any, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("%w: want 3 segments, got %d", ErrMalformed, len(parts))
	}
	enc := base64.RawURLEncoding
	pld, err := enc.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("%w: decode payload: %v", ErrMalformed, err)
	}
	var claims map[string]any
	if err := json.Unmarshal(pld, &claims); err != nil {
		return nil, fmt.Errorf("%w: payload not JSON: %v", ErrMalformed, err)
	}
	return claims, nil
}

// ParseDecryption parses the token and pulls out the strongly-typed
// decryption claims. Validates iss, aud, exp and that all required
// fields are present.
//
// expectedIssuer and expectedAudience cannot be empty.
func ParseDecryption(token, expectedIssuer, expectedAudience string) (*DecryptionClaims, error) {
	claims, err := Parse(token)
	if err != nil {
		return nil, err
	}

	getStr := func(k string) (string, error) {
		v, ok := claims[k]
		if !ok {
			return "", fmt.Errorf("%w: %s", ErrMissingClaim, k)
		}
		s, ok := v.(string)
		if !ok || s == "" {
			return "", fmt.Errorf("%w: %s (not a non-empty string)", ErrMalformed, k)
		}
		return s, nil
	}

	iss, err := getStr("iss")
	if err != nil {
		return nil, err
	}
	if iss != expectedIssuer {
		return nil, fmt.Errorf("%w: got %q, want %q", ErrWrongIssuer, iss, expectedIssuer)
	}
	aud, err := getStr("aud")
	if err != nil {
		return nil, err
	}
	if aud != expectedAudience {
		return nil, fmt.Errorf("%w: got %q, want %q", ErrWrongAudience, aud, expectedAudience)
	}

	exp, ok := toUnix(claims["exp"])
	if !ok {
		return nil, fmt.Errorf("%w: exp", ErrMissingClaim)
	}
	iat, _ := toUnix(claims["iat"])

	expiresAt := time.Unix(exp, 0).UTC()
	if time.Now().UTC().After(expiresAt) {
		return nil, ErrExpired
	}

	sub, err := getStr("sub")
	if err != nil {
		return nil, err
	}
	runID, err := getStr("run_id")
	if err != nil {
		return nil, err
	}
	companyID, err := getStr("company_id")
	if err != nil {
		return nil, err
	}
	dek, err := getStr("dek")
	if err != nil {
		return nil, err
	}
	alg, err := getStr("alg")
	if err != nil {
		return nil, err
	}
	fv, ok := toUnix(claims["format_version"])
	if !ok {
		return nil, fmt.Errorf("%w: format_version", ErrMissingClaim)
	}

	// sha256 is optional — empty is allowed (run had no recorded hash).
	sha, _ := claims["sha256"].(string)

	return &DecryptionClaims{
		Issuer:        iss,
		Subject:       sub,
		Audience:      aud,
		IssuedAt:      time.Unix(iat, 0).UTC(),
		ExpiresAt:     expiresAt,
		RunID:         runID,
		CompanyID:     companyID,
		DEKBase64:     dek,
		Algorithm:     alg,
		FormatVersion: int(fv),
		SHA256:        sha,
	}, nil
}

func toUnix(v any) (int64, bool) {
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case int64:
		return n, true
	case int:
		return int64(n), true
	case json.Number:
		i, err := n.Int64()
		return i, err == nil
	}
	return 0, false
}
