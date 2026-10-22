# ytclone

Copy static tables, files, YSON documents, and directories between YTsaurus and S3,
preserving schemas and portable attributes.

Restoring from S3 requires a `yt-s3-v2` archive created by `ytclone`. Earlier archive
formats are rejected; re-export the source to a new S3 prefix.

## Installation

```bash
pip install ytclone
```

Requires **CPython 3.10+** on **Linux x86_64 with glibc** or **macOS ARM64**
(Apple Silicon).

Also available as [standalone downloads](https://github.com/alvassin/ytclone/blob/main/USAGE.md#standalone-downloads)
and [Docker images](https://github.com/alvassin/ytclone/blob/main/USAGE.md#docker).

## Quick start

Configure access to YTsaurus and S3:

```bash
export YT_PROXY="https://yt.example.com"
export YT_TOKEN="..."
export AWS_ACCESS_KEY_ID="..."
export AWS_SECRET_ACCESS_KEY="..."
export AWS_DEFAULT_REGION="us-east-1"
```

The S3 bucket must already exist. For an S3-compatible service, also set
`S3_ENDPOINT` to its URL. Use `AWS_SESSION_TOKEN` for temporary credentials.

**Copy to S3:**

```bash
ytclone //home/analytics/events s3://my-bucket/export --log-file export.log
```

The full YT path is appended to the S3 prefix: this command stores `events` at
`s3://my-bucket/export/home/analytics/events/`.

**Restore to YTsaurus:**

```bash
ytclone s3://my-bucket/export/home/analytics/events \
  //home/analytics/events_copy --log-file restore.log
```

The restore destination is used exactly as given: the children of `events` go
directly under `//home/analytics/events_copy`. With `--log-file`, progress appears
on the console and detailed logs are appended to the file.

## Client-side encryption

Add `--encrypt --encryption-password-file /secure/ytclone-password` to export
or restore. The password file must contain at least 32 bytes; one trailing LF or
CRLF is accepted. Alternatively, provide `YTCLONE_ENCRYPTION_PASSWORD` and
`--encrypt`. No password value is accepted on the command line.

Encryption uses the native OpenSSL library through Python `cryptography`, without
an external process. Table data is encrypted directly after removing YT's
transport framing, preserving its Zstandard frames. Files and documents are
encrypted after their existing local compression. Attributes and manifest
contents are also encrypted; S3 object names remain readable.

The same password is needed to resume exports and restore. Keep it separately
from the archive. Existing unencrypted v2 archives remain supported. See
[encryption and manual recovery](USAGE.md#client-side-encryption) for the format,
integrity checks, and password rotation limits.

## Repeated runs

Rerun the command after an interruption or source update. Completed, unchanged
nodes are skipped; other nodes are copied again. An unfinished table, file, or
document restarts from the beginning.

Source data is never deleted. Destination nodes absent from the source are kept.
Use a separate S3 prefix for each source cluster.

## Parallel export

Use [`--max-streams`](https://github.com/alvassin/ytclone/blob/main/USAGE.md#tuning)
to export large tables through several streams, or
[`ytclone-parallel`](https://github.com/alvassin/ytclone/blob/main/USAGE.md#parallel-export)
for multiple copy processes. Both apply to YT → S3; restore with `ytclone`.

### Table format and compression

Both export commands now request Zstandard-compressed table data from the YT
proxy by default. They remove the `z-zstd_<level>` transport container and upload
its standard Zstandard frames without local decompression or recompression.
Files and documents continue to use local Zstandard compression.

The initial supported combinations are `--table-format yson` (the default) or
`--table-format skiff`, with `--compression zstd` and `--compression-level 3`.
For example:

```bash
ytclone //home/analytics/events s3://my-bucket/skiff-export \
  --table-format skiff --compression-level 3 --log-file export.log
```

Each table's manifest records its wire format and schema. Restore selects them
automatically, so no format option is needed. Skiff keeps complex values and
columns outside the declared schema in its YSON catch-all field. Schemas,
unsigned integers, binary strings and nulls are preserved.

The deprecated `--yt-zstd-passthrough` export option is accepted as a no-op;
omitting it now gives the same table compression behavior. Unsupported proxy
compression fails explicitly, without falling back to local table compression.
Proxy blocks may compress less efficiently than a continuous local stream.
Use fresh destination prefixes when comparing formats or compression levels;
repeated unchanged copies are skipped. Changing a table's format or compression
level forces a fresh export of that table. Existing v2 YSON archives remain
readable, but upgrading from local table compression causes a one-time table
re-export. Use this version or newer to restore newly written manifests; older
readers do not understand the added format metadata.

## Before you copy

- Dynamic tables are skipped and reported; links are unsupported.
- **One current archive version.** Failed uploads keep the previous copy available.
  Updates publish first, then try to delete the old copy. No history is kept.
- **Permissions are not copied.** ACLs, ownership, and account settings are
  excluded. Replacing an existing YT table, file, or document recreates the node,
  so its previous settings are not preserved either.
- **Consistency is per node.** A directory tree is not a single point-in-time
  snapshot.
- **Concurrent exports can fail on conflicts.** Updating an archive during restore
  can also fail the restore; rerun it. Concurrent restores to the same YT nodes
  are unsupported.
- S3 must support [atomic conditional writes](https://github.com/alvassin/ytclone/blob/main/USAGE.md#connection-settings).
  See [update and recovery details](https://github.com/alvassin/ytclone/blob/main/USAGE.md#errors-and-recovery)
  for rollback and cleanup behavior.

## Documentation

Run `ytclone --help` or `ytclone-parallel --help` for all command-line options.

- [User guide](https://github.com/alvassin/ytclone/blob/main/USAGE.md): paths,
  configuration, progress, retries, and parallel export.
- [Architecture](https://github.com/alvassin/ytclone/blob/main/ARCHITECTURE.md):
  archive format, change detection, and execution diagrams.
- [Contributing](https://github.com/alvassin/ytclone/blob/main/CONTRIBUTING.md):
  installation from source, development, and tests.

License: [MIT](https://github.com/alvassin/ytclone/blob/main/LICENSE).
