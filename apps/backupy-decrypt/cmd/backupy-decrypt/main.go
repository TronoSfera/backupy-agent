// Command backupy-decrypt decrypts a Backupy backup file produced by the
// Backupy agent.
//
// The CLI takes a JWT (issued by the Backupy server at
// /v1/runs/:id/decryption-token) and an encrypted input file. It streams
// the input through AES-256-GCM decryption, optionally decompresses zstd,
// and writes the plaintext to the chosen output path.
//
// The CLI is OFFLINE-ONLY: it never contacts the server. The JWT carries
// everything we need (the DEK is embedded in the token body).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/backupy/backupy/apps/backupy-decrypt/internal/decrypt"
)

// version is overwritten at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	var (
		token         = flag.String("token", "", "Decryption token (JWT) from the Backupy server.")
		input         = flag.String("in", "", "Encrypted input file (.enc).")
		output        = flag.String("out", "", "Output plaintext file.")
		verifySHA     = flag.Bool("verify-sha256", true, "Verify that the input's SHA-256 matches the token's claim.")
		skipDecomp    = flag.Bool("skip-decompress", false, "Don't decompress zstd after decrypting (the agent always zstd-compresses, so this only helps if you have a non-default pipeline).")
		quiet         = flag.Bool("quiet", false, "Suppress progress output.")
		showVersion   = flag.Bool("version", false, "Show version and exit.")
		flagTokenFile = flag.String("token-file", "", "Read token from this file instead of --token (useful to avoid leaking via ps).")
	)
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "Decrypt a Backupy backup file.")
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "Usage:")
		fmt.Fprintln(os.Stderr, "  backupy-decrypt --token <jwt> --in <encrypted-file> --out <plaintext-file>")
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "Flags:")
		flag.PrintDefaults()
	}
	flag.Parse()

	if *showVersion {
		fmt.Printf("backupy-decrypt %s\n", version)
		return
	}

	tok := *token
	if *flagTokenFile != "" {
		b, err := os.ReadFile(*flagTokenFile)
		if err != nil {
			fail("read --token-file: %v", err)
		}
		tok = trimNewline(string(b))
	}

	if tok == "" || *input == "" || *output == "" {
		flag.Usage()
		os.Exit(2)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	progress := func(n int64) {}
	if !*quiet {
		progress = makeProgressFn()
	}

	if err := decrypt.Run(ctx, decrypt.Options{
		InputPath:      *input,
		OutputPath:     *output,
		Token:          tok,
		VerifySHA256:   *verifySHA,
		SkipDecompress: *skipDecomp,
		Progress:       progress,
	}); err != nil {
		fail("%v", err)
	}

	if !*quiet {
		fmt.Fprintln(os.Stderr, "OK — decrypted to", *output)
	}
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
	os.Exit(1)
}

func trimNewline(s string) string {
	for len(s) > 0 && (s[len(s)-1] == '\n' || s[len(s)-1] == '\r' || s[len(s)-1] == ' ' || s[len(s)-1] == '\t') {
		s = s[:len(s)-1]
	}
	return s
}

// makeProgressFn returns a Progress callback that overwrites a single
// line on stderr with the byte count. Outputs a final newline when the
// caller stops calling it (best-effort).
func makeProgressFn() func(int64) {
	var last int64
	return func(n int64) {
		// Only redraw on ~1 MiB granularity to avoid spamming stderr.
		if n-last < 1<<20 {
			return
		}
		last = n
		fmt.Fprintf(os.Stderr, "\rprocessed %s", humanBytes(n))
	}
}

func humanBytes(n int64) string {
	const (
		KB = 1 << 10
		MB = 1 << 20
		GB = 1 << 30
	)
	switch {
	case n >= GB:
		return fmt.Sprintf("%.2f GiB", float64(n)/GB)
	case n >= MB:
		return fmt.Sprintf("%.2f MiB", float64(n)/MB)
	case n >= KB:
		return fmt.Sprintf("%.2f KiB", float64(n)/KB)
	default:
		return fmt.Sprintf("%d B", n)
	}
}
