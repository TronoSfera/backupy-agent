# backupy-decrypt

Offline CLI to decrypt a [Backupy](https://backupy.ru) backup file.

The Backupy server never sees your plaintext data — it stores only the
AES-256-GCM ciphertext your agent uploaded. To recover a backup you:

1. Generate a **download URL** from the Backupy dashboard (`GET
   /v1/runs/:id/download-url`).
2. Generate a **decryption token** from the same dashboard (`POST
   /v1/runs/:id/decryption-token`). This token is a JWT that contains the
   plaintext data-encryption key (DEK) plus integrity metadata. It is
   valid for **15 minutes**.
3. Download the encrypted blob with `curl` (or any HTTP client).
4. Run `backupy-decrypt --token <jwt> --in <blob> --out <plaintext>`.

The CLI is fully offline — it never contacts Backupy infrastructure.

## Install

Download the release archive for your platform from the [GitHub
releases](https://github.com/backupy/backupy/releases) page, extract it,
and put the `backupy-decrypt` binary somewhere on `$PATH`:

```bash
# macOS (arm64) — adjust for your platform
curl -L https://github.com/backupy/backupy/releases/latest/download/backupy-decrypt_Darwin_arm64.tar.gz \
  | tar -xz
sudo mv backupy-decrypt /usr/local/bin/
```

Or build from source:

```bash
git clone https://github.com/backupy/backupy.git
cd backupy/apps/backupy-decrypt
make build
./bin/backupy-decrypt --version
```

## Usage

```bash
$ backupy-decrypt --help
Decrypt a Backupy backup file.

Usage:
  backupy-decrypt --token <jwt> --in <encrypted-file> --out <plaintext-file>

Flags:
  --token            Decryption token (JWT) from the Backupy server
  --token-file       Read token from a file (avoids leaking via `ps`)
  --in               Encrypted input file (.enc)
  --out              Output plaintext file
  --verify-sha256    Verify the input's SHA-256 matches the token's claim (default true)
  --skip-decompress  Don't decompress zstd after decryption (default false — the
                     agent always zstd-compresses, so leave this off unless your
                     pipeline used compression=none)
  --quiet            Suppress progress output
  --version          Show version
```

### Full example

```bash
# 1) Download the ciphertext.
curl -o backup.enc "<presigned_url_from_dashboard>"

# 2) Save the JWT into a file so it doesn't leak via your shell history.
echo "<jwt_from_dashboard>" > /tmp/backup.token

# 3) Decrypt + decompress in one shot. Output is the raw pg_dump / mysqldump.
backupy-decrypt \
    --token-file /tmp/backup.token \
    --in backup.enc \
    --out backup.sql

# 4) Restore yourself. Backupy intentionally does not run this step —
#    you stay in control of where the data goes.
psql -d mydb < backup.sql
```

## Encryption format (verbatim)

The CLI implements the inverse of `apps/agent/internal/pipeline`. All
integers are **big-endian**.

```
chunk := uint32 ciphertext_len      // bytes that follow, EXCLUDING this u32
         12-byte random nonce       // unique per chunk
         ciphertext (≤ 1 MiB + 16-byte GCM tag)

EOF marker := uint32 0              // appended after the final chunk
```

- Plaintext chunk size is **1 MiB**. The final chunk may be shorter.
- `tag` is the 16-byte AES-GCM authentication tag, appended by Go's
  `cipher.AEAD.Seal`.
- The Additional Authenticated Data (AAD) is **nil**. Per-chunk reorder
  and truncation defence comes from the explicit `uint32` size prefix
  plus the mandatory zero-length EOF marker: any reordered or truncated
  stream either trips a frame-boundary error or fails the missing-EOF
  check.
- The DEK is exactly **32 bytes** (AES-256).

This format is canonical — `apps/agent/internal/pipeline/encrypt.go` is
the single source of truth, and the CLI is implemented to be byte-for-
byte compatible. This is enough to reimplement the decrypt step in any
language — see the package doc for `apps/backupy-decrypt/internal/decrypt`.

## Troubleshooting

| Symptom                                                | Cause / fix                                                                                          |
|--------------------------------------------------------|------------------------------------------------------------------------------------------------------|
| `error: decrypt: token expired (request a new one)`    | The JWT lifetime is 15 min. Issue a new token from the dashboard.                                    |
| `error: decrypt: AES-GCM authentication failed`        | Wrong DEK or corrupted input. Re-download the file and re-issue the token.                           |
| `error: decrypt: ciphertext SHA-256 mismatch`          | The file you downloaded is not the file the server recorded. Re-download — the URL may have expired. |
| `error: decrypt: input file is truncated`              | Download didn't complete. Retry with `curl -C - ...` or a fresh URL.                                 |
| Plaintext looks like binary garbage after decompress   | Your job was set to `compression=none`. Re-run with `--skip-decompress`.                             |

## Security notes

- The decryption token is **as sensitive as your data** — anyone with the
  JWT in their possession can decrypt the file. Treat it like a temporary
  password: do not paste it into chat, do not commit it.
- The CLI never writes the DEK to disk; it lives only in process memory
  and is zeroized before exit.
- For an extra layer of safety pass `--token-file` so the token does not
  appear in your shell history or in `ps`.

## License

Same as the parent Backupy repository.
