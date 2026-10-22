# User guide

Start with the [README](README.md) for installation, credentials, and a first
copy. Use `ytclone --help` or `ytclone-parallel --help` for the full option list.

## Python installation

Install with `pip install ytclone` in a Python virtual environment. Requires
CPython 3.10+ on Linux x86_64 with glibc or macOS ARM64 (Apple Silicon). Other
platforms are unsupported because the required
[`ytsaurus-yson`](https://pypi.org/project/ytsaurus-yson/) dependency has no wheels
for them.

## Connection settings

| Option | Environment variable | Default |
| --- | --- | --- |
| `--yt-proxy` | `YT_PROXY` | Required |
| `--endpoint` | `S3_ENDPOINT` | `https://s3.amazonaws.com` |
| `--region` | `AWS_DEFAULT_REGION` | `us-east-1` |

For local Docker YTsaurus, use `--yt-proxy http://localhost:8000` with
`--no-proxy-discovery`. See [local integration tests](CONTRIBUTING.md#test-with-local-ytsaurus-and-s3).

The S3 backend must honor `If-Match` and `If-None-Match: *` on object PUTs and
provide atomic object visibility with ETags. S3 API compatibility alone does not
establish these guarantees; verify them for the service you use.

## Archive compatibility and updates

Only the `yt-s3-v2` archive format is supported. Existing older archives are
rejected on export and restore; re-export the source to a new S3 prefix. There is
no in-place format migration.

An updated node gets a new generation containing its attributes and data parts.
The previous manifest remains visible until all uploads and the YT snapshot
complete successfully. The new manifest is published conditionally, then the
exact objects referenced by its predecessor are deleted immediately. No TTL or
archive history is retained.

A restore reads one manifest and uses that generation throughout. If a concurrent
export deletes its generation, the unfinished restore fails and rolls back; it
does not automatically switch to another generation. Rerun restore to use the
current manifest. These guarantees apply per node, not to a snapshot of the
entire directory tree.

## Client-side encryption

Use an explicit opt-in on both export and restore:

```bash
ytclone //home/analytics/events s3://my-bucket/encrypted-export \
  --encrypt --encryption-password-file /secure/ytclone-password

ytclone s3://my-bucket/encrypted-export/home/analytics/events \
  //home/analytics/events_copy \
  --encrypt --encryption-password-file /secure/ytclone-password
```

The same flags work with `ytclone-parallel`. Each worker reads the same password
file or inherits `YTCLONE_ENCRYPTION_PASSWORD`; the coordinator sends only the
encryption flag, absolute file path, and a nonsecret key identity through its
control pipe. Workers reject a changed password before starting clients,
including after a worker restart. Keep the password file unchanged during a run.

The password must contain at least 32 bytes and no NUL, CR, or LF. For a file,
one final LF or CRLF is removed; spaces are preserved. An explicitly supplied
file takes precedence over the environment variable. Without `--encrypt`, an
environment password is ignored, and `--encryption-password-file` is rejected.
Password values are not parser defaults, log fields, or worker configuration.
Protect the password file and keep a separate recovery copy of the password.
The backup process holds a symmetric secret and can decrypt existing backups
made with that secret.

Encryption happens locally through `cryptography` and its native OpenSSL
library. It does not launch the `openssl` command. Table data retains the
Zstandard frames received from YT; encryption adds no decompression or
recompression. Files and documents retain their existing local Zstandard
compression before encryption.

All data parts, attributes, and manifest contents are encrypted. Node and directory
names remain readable. New encrypted data parts add `.enc` to their existing
suffixes, for example `part-000000.skiff.zst.enc`; encrypted attributes use
`@attributes.yson.enc`. The authenticated JSON manifest keeps `@manifest.json`.
Early encrypted archives without `.enc` remain readable; unchanged exports keep
their existing generation rather than rewriting it solely to rename objects.
Encryption is determined by the authenticated manifest, not the filename.
Data and attribute objects use the OpenSSL
`enc` format: `Salted__`, an eight-byte random salt, and AES-256-CBC ciphertext
with PKCS#7 padding. Key and IV derivation uses PBKDF2-HMAC-SHA256 with 100,000
iterations. Each object gets a fresh salt.

The manifest is an authenticated JSON envelope containing the base64-encoded
OpenSSL-compatible ciphertext and an HMAC. Archive references retain SHA-256
and size checks for the stored ciphertext; the encrypted manifest also records
checksums of the contents before encryption. For data parts these are the
already-compressed Zstandard bytes, including the frames received from YT.
Restore authenticates the manifest and checks both the ciphertext and decrypted
contents before committing the destination node. Raw `openssl enc` alone does
not perform these archive integrity checks.

Existing unencrypted v2 archives remain readable without `--encrypt`. On restore,
`--encrypt` requires authenticated encrypted manifests and rejects plaintext
nodes. An encrypted manifest needs the matching password; missing or wrong
credentials fail before replacing that node. Repeated encrypted exports also
need the matching password to inspect the current manifest.

Prefer a new S3 prefix when enabling encryption. Export can replace an existing
plaintext archive with encrypted generations, but an interrupted conversion
leaves a mixed tree that requires separate per-node restore modes. Old S3
versions and abandoned generations may still contain plaintext; enabling
encryption does not remove them. Use a new prefix when changing passwords;
automatic password rotation is not supported. Keep the old password as long as
its backups are needed. These checks are per node; a tree restore is not one
transaction.

### Manual data recovery

Keep this format description and the password available independently of the
backup machine. Also keep [`scripts/recover_openssl.py`](scripts/recover_openssl.py)
for password-file normalization. This standalone helper needs only Python 3.10+
and OpenSSL 1.1.1 or newer on Linux or macOS; it does not import ytclone or any
third-party Python packages. Decrypt one downloaded data part:

```bash
python3 scripts/recover_openssl.py \
  --password-file /secure/ytclone-password \
  --input downloaded-part.zst.enc --output part.zst
zstd -d part.zst -o part.raw
```

The helper removes exactly one final LF or CRLF and preserves every other byte,
including spaces and non-UTF-8 bytes. It passes the password in the OpenSSL
child's environment, without putting it in command arguments or changing the
calling shell's environment. Direct `openssl -pass file:...` is not equivalent:
it retains CR from CRLF and truncates long passwords. The helper supports
passwords longer than 1023 bytes, subject to the operating system's process
environment size limit; use normal ytclone restore if an unusually large
password exceeds that limit. Run recovery under an account whose process
environment is protected from other users.

The OpenSSL invocation uses `enc -d -aes-256-cbc -pbkdf2 -iter 100000 -md sha256`
with `-pass env:YTCLONE_RECOVERY_PASSWORD`. The helper detects support for
`-saltlen 8` and adds it for OpenSSL 3.2 or newer; older versions already use an
eight-byte salt. Use `--openssl /path/to/openssl` to select an executable.
The result after Zstandard decompression is the archived YSON or Skiff stream,
as specified by the manifest. Attribute decryption produces YSON directly, with
no Zstandard step. The JSON manifest envelope must first be authenticated and
its base64 ciphertext extracted; it is not itself a raw `openssl enc` file. Use
`ytclone` for normal restoration and its complete integrity checks. A successful
OpenSSL exit status is not an integrity check for CBC data.

## Standalone downloads

Download an archive and its matching `.tar.gz.sha256` file from
[GitHub Releases](https://github.com/alvassin/ytclone/releases):
`linux-x86_64` requires glibc 2.28 or newer; `macos-arm64` requires macOS 14 or
newer on Apple Silicon. No installed Python or Docker is needed.

Replace `VERSION` and `PLATFORM` with the downloaded archive's values, then verify
and unpack it from the directory containing both downloaded files:

```bash
archive=ytclone-VERSION-PLATFORM.tar.gz
sha256sum -c "$archive.sha256"
tar -xzf "$archive"
./ytclone-VERSION-PLATFORM/ytclone --help
./ytclone-VERSION-PLATFORM/ytclone-parallel --help
```

On macOS, use `shasum -a256` instead of `sha256sum`. Keep the extracted directory
intact: both commands share its `_internal` libraries. Use the same credentials
and arguments as the PyPI commands. The macOS build is ad-hoc signed, without
Apple Developer signing or notarization; macOS may require approval before its
first launch.

## Docker

Choose a published version from [GHCR](https://github.com/alvassin/ytclone/pkgs/container/ytclone)
and replace `VERSION` below. Images use Python 3.14 on Debian slim and support
`linux/amd64`; Apple Silicon requires x86_64 emulation. Tags match PyPI versions;
there is no `latest` tag.

With the [access variables](README.md#quick-start) already exported:

```bash
IMAGE=ghcr.io/alvassin/ytclone:VERSION
mkdir -p logs
docker run --rm --name ytclone-copy --platform linux/amd64 --stop-timeout 30 \
  --user "$(id -u):$(id -g)" -v "$PWD/logs:/logs" \
  -e YT_PROXY -e YT_TOKEN -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
  -e AWS_SESSION_TOKEN -e AWS_DEFAULT_REGION -e S3_ENDPOINT \
  "$IMAGE" ytclone-parallel //home/analytics/events s3://my-bucket/export \
  --processes 2 --concurrency 2 --log-file /logs/export.log
```

Use `ytclone` instead of `ytclone-parallel` for ordinary copying or restore, with
the same arguments as a local installation. `docker run --rm "$IMAGE"` prints
help. The default container user is non-root (UID 10001); the example uses your
host UID so it can write the mounted logs. Credentials are passed at runtime.

Stop a running copy with `docker stop --time 30 ytclone-copy`. The image includes
an init process to forward signals and reap children; the grace period allows
the supervisor to stop its workers. The known [aiomisc shutdown limitation](CONTRIBUTING.md#run-checks)
also occurs on Python 3.14 with aiomisc 18.0.33.

## Copying a subset

To restore one table, file, or document, point to its path inside the archive:

```bash
ytclone s3://my-bucket/export/home/analytics/events/2026-01-01 \
  //home/analytics/events_copy_one
```

To expand an export from a child directory to its parent, keep the same S3 prefix.
For example, export both `//home/analytics/events` and `//home/analytics` to
`s3://my-bucket/export`. Existing unchanged nodes are skipped; siblings are added.

## Tuning

| Option | Default | Effect |
| --- | --- | --- |
| `--concurrency` | `4` | Concurrent node lifecycles; per process in parallel mode. |
| `--max-streams` | `0` | Shared export stream count per process; `0` uses `--concurrency`. Export only; restore rejects nonzero values. |
| `--queue-size` | `16` | Pending paths in ordinary mode; control messages per process in parallel mode. |
| `--part-size-mib` | `256` | Target **uncompressed** bytes per archive object; approximate for tables. |
| `--buffer-size-mib` | `32` | S3 multipart buffer per active stream, from 5 to 512 MiB. |
| `--table-format` | `yson` | Export table rows as binary YSON or `skiff`. The manifest records the format and wire schema; restore chooses automatically. |
| `--compression` | `zstd` | Export compression codec. Zstandard is the only supported codec. |
| `--compression-level` | `3` | Zstandard level, from -7 to 22; higher levels use more CPU and memory. Tables are compressed by the YT proxy; files and documents are compressed locally. |
| `--yt-zstd-passthrough` | off | Deprecated export-only no-op. All table exports already request Zstandard from YT. |

The supported table combinations are YSON + Zstandard and Skiff + Zstandard.
Table data arrives compressed from YT in both cases; there is no local table
compression fallback. Skiff's recorded wire schema includes a YSON catch-all
field for complex values and columns outside the declared schema. Restore does
not require these export options, including when reading older YSON manifests.
Changing the table format or compression level invalidates unchanged detection
for that table. Use a fresh S3 prefix for timing comparisons so earlier backups
do not turn a measured run into skipped work.

For example, allow four active nodes to share eight export streams in one process:

```bash
ytclone //home/analytics/events s3://my-bucket/export \
  --concurrency 4 --max-streams 8 --log-file export.log
```

An archive part is one S3 object; multipart buffers are chunks within that object.
`--part-size-mib` does not allocate a whole part in RAM. Objects smaller than the
buffer use a single PUT; objects at or above that size use multipart upload.

Export uses a fixed pool of `--max-streams` workers shared by all active nodes.
A single static table can use the whole pool. Ready nodes take turns supplying
ranges as workers become free; files and documents use one range at a time.
`--concurrency` bounds node lifecycles, including metadata checks and snapshots,
separately from this stream limit. Restore keeps each node's parts sequential.

Lower the stream count or buffer size to reduce export memory use; lower
`--concurrency` to reduce the number of active snapshots or restore transfers.
The startup RAM estimate
is a planning budget, **not a limit**: large rows, metadata, and allocator overhead
can exceed it. Parallel mode multiplies transfer memory by process count and also
keeps discovered metadata in RAM; `--queue-size` does not bound that metadata.

## Progress and logs

`--log-file copy.log` appends detailed logs to a file and prints console summaries.
Set their interval with `--progress-interval` (default: 5 seconds). Without a log
file, ordinary mode writes detailed logs to the console.

| Counter | Meaning |
| --- | --- |
| `copied / unchanged / skipped / errors` | Completed nodes, matching copies, reported skips, and node/discovery errors. |
| `queued / active` | Pending nodes and active node lifecycles, not export streams. |
| `uploaded / published` | Stored bytes sent versus bytes belonging to completed archive nodes; includes encryption overhead when enabled. |
| `downloaded / restored` | Stored bytes received versus bytes belonging to completed restores; includes encryption overhead when enabled. |

Transfer counters can include failed attempts; completion counters do not. Source
sizes are YT storage weights, including replication and compression, **not S3
bytes**. `discovered` totals and export ETA are estimates: `?` means unavailable,
`~` means approximate. Restore has no ETA. Request timings overlap and should not
be added together as total elapsed time.

## Parallel export

For trees with many nodes, use multiple copy processes:

```bash
ytclone-parallel //home/analytics/events s3://my-bucket/export \
  --processes 2 --concurrency 2 --log-file export.log
```

`--processes P --concurrency C --max-streams N` allows up to P × C active nodes
and P × N export streams. With the default `--max-streams 0`, N equals C.
Each process has its own stream pool; a table stays in one process and can use
that process's N streams. This command supports YT → S3 only; restore its archives
with `ytclone`.

- `--discovery-concurrency` (default: 4) controls concurrent directory listings.
- Directories and smaller known nodes go first. While discovery runs, one slot is
  reserved for nodes up to `--small-table-mib` (default: 64). Running transfers
  are not preempted.
- `--worker-restarts` (default: 3) limits crash replacements per process. Ordinary
  node failures use transfer retries instead of restarting the process.

With `--log-file export.log`, workers write `export-worker-N.log`. The coordinator
aggregates node and byte counts, but not worker ETA, source-size estimates, or
request timings. An abrupt worker death can lose its last unreported byte counts.
Independent exports may target the same archive nodes. Their conditional manifest
writes detect competing updates and fail the conflicting node; they never blindly
overwrite another publication. Coordinator scheduling remains local to one
invocation.

## Errors and recovery

Temporary failures are retried automatically and logged:

- **Table reads:** retry the failed archive part within the same snapshot, up to
  three attempts. A lost snapshot fails the node.
- **S3 requests:** up to 30 attempts, with a 45-minute budget per retried request.
  Conditional publication conflicts stop that node rather than rebasing its
  snapshot onto a newer manifest. A lost publication acknowledgement is checked
  against the stored manifest before treating the publication as successful.
- **Restore:** after a confirmed rollback, retry the entire node in a new
  transaction, up to 30 attempts. Permission and checksum errors are not retried.

An uncertain commit or rollback stops automatic restore retries. A failure while
recording completion can occur **after the data was committed**; inspect the log
before assuming that the old destination is still present.

On an export error, the node stops scheduling new ranges and waits for its active
ranges to finish cleanup before closing the snapshot. Cancellation also drains
active range workers before closing their snapshots. The manifest is published
only after all parts and the snapshot have completed successfully.

Failed or interrupted uploads leave the previous manifest and its generation
available. A cleanup failure after confirmed publication is logged as a warning:
the new backup is complete, but old objects may remain. Crashed or conflicted
uploads, uncertain publication outcomes, and interrupted cleanup can leave unused
generations. There is no automatic global garbage collection; a later successful
export deletes only the generation it directly replaces, not all older objects.

After fixing a failure, [rerun the command](README.md#repeated-runs). Completed,
unchanged nodes are skipped; an unfinished table, file, or document starts from
the beginning.

Configure an S3 lifecycle rule to expire incomplete multipart uploads. Known
uploads are aborted on failure, but an upload whose creation response was lost
cannot be aborted by the client.

Exit codes: `0` completed, `1` copy/discovery failure, `2` invalid arguments,
`130` interrupted. Dynamic-table skips do not fail the run; check `skipped`.

See [copying limitations](README.md#before-you-copy) and
[replacement semantics](ARCHITECTURE.md#completion-and-replacement) before
overwriting existing data. The [archive format and skip rules](ARCHITECTURE.md)
are documented separately.
