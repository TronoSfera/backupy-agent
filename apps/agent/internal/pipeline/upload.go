package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Uploader streams an encrypted backup blob to S3 via a presigned PUT
// URL while computing the SHA-256 of the bytes it actually uploads.
//
// Note (design): SHA-256 is computed over the CIPHERTEXT — that is, the
// exact blob persisted in S3. The server uses this to detect bit rot.
// The plaintext hash is not exposed; it could only be derived after a
// successful decrypt, which is what backupy-decrypt is for.
type Uploader struct {
	httpClient *http.Client
}

// NewUploader constructs an Uploader with a sensible default http.Client.
// The client has no overall request timeout because uploads can be
// hours-long for very large dumps — cancellation flows through ctx
// instead.
func NewUploader() *Uploader {
	return &Uploader{
		httpClient: &http.Client{
			// No Timeout: large uploads must be allowed to run long.
			// Per-stage timeouts are enforced by ctx.
			Transport: &http.Transport{
				// Honour HTTP/2 + idle-conn defaults; only override
				// the response-header deadline so a stuck server is
				// detected within 30s rather than hanging forever.
				ResponseHeaderTimeout: 30 * time.Second,
				ExpectContinueTimeout: 5 * time.Second,
			},
		},
	}
}

// NewUploaderWithClient is a constructor used by tests to inject a
// custom http.Client (e.g. one wired to httptest.Server).
func NewUploaderWithClient(c *http.Client) *Uploader {
	return &Uploader{httpClient: c}
}

// Put streams body to presignedURL using HTTP PUT and computes SHA-256
// of the body on the fly. contentLength may be -1 if unknown — in that
// case the request is sent chunked.
//
// Returns the lower-case hex SHA-256 and the number of bytes uploaded.
func (u *Uploader) Put(ctx context.Context, presignedURL string, body io.Reader, contentLength int64) (string, int64, error) {
	h := sha256.New()
	counted := &countingReader{r: io.TeeReader(body, h)}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, presignedURL, counted)
	if err != nil {
		return "", 0, fmt.Errorf("pipeline: build PUT request: %w", err)
	}
	if contentLength >= 0 {
		req.ContentLength = contentLength
	}
	// Match the Content-Type the server's presign was generated for.
	// "application/octet-stream" is the canonical default for raw blobs.
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := u.httpClient.Do(req)
	if err != nil {
		return "", counted.n, fmt.Errorf("pipeline: PUT %s: %w", presignedURL, err)
	}
	defer func() { _, _ = io.Copy(io.Discard, resp.Body); _ = resp.Body.Close() }()

	if resp.StatusCode/100 != 2 {
		return "", counted.n, fmt.Errorf("pipeline: upload non-2xx: HTTP %d", resp.StatusCode)
	}
	return hex.EncodeToString(h.Sum(nil)), counted.n, nil
}

// countingReader counts bytes read from the wrapped reader. Used to
// compute the uploaded size without buffering.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}
