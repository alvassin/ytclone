# ytclone

Copy static tables, files, YSON documents, and directories between YTsaurus and S3,
preserving schemas and portable attributes. Repeat a command to skip completed, unchanged
nodes and retry unfinished copies.

Restoring from S3 requires a `yt-s3-v2` archive created by `ytclone`. Earlier archive
formats are rejected; re-export the source to a new S3 prefix.

## Installation

Install from PyPI in your Python virtual environment:

```bash
pip install ytclone
```

Requires **CPython 3.10+** on **Linux x86_64 with glibc** or **macOS ARM64**
(Apple Silicon). Other platforms are currently unsupported because the required
[`ytsaurus-yson`](https://pypi.org/project/ytsaurus-yson/) dependency has no wheels
for them.

Standalone archives include both commands and need no installed Python or Docker.
See [standalone downloads](https://github.com/alvassin/ytclone/blob/main/USAGE.md#standalone-downloads)
for Linux x86_64 and macOS ARM64.

Docker images include both commands and use the same version numbers as PyPI.
See [running with Docker](https://github.com/alvassin/ytclone/blob/main/USAGE.md#docker) for commands (`linux/amd64` only).

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
directly under `//home/analytics/events_copy`. The same commands work for a single
table, file, or document. With `--log-file`, progress appears on the console and
detailed logs are appended to the file.

## Repeated runs

Run the same command again after an interruption or a source update. Completed,
unchanged nodes are skipped. New, changed, or incomplete nodes are copied again;
an interrupted table, file, or document starts from the beginning, not from its
last part.

Source data is never deleted. Destination nodes absent from the source are kept.
Use a separate S3 prefix for each source cluster.

## Parallel export

Export large tables through several streams in one process:

```bash
ytclone //home/analytics/events s3://my-bucket/export \
  --concurrency 4 --max-streams 8 --log-file export.log
```

Up to four nodes are active, sharing eight export streams. A single large table
can use all eight streams; ready tables take turns when streams become available.
Files and documents use one stream at a time. By default, the stream count equals
`--concurrency`.

For trees with many nodes, use multiple copy processes:

```bash
ytclone-parallel //home/analytics/events s3://my-bucket/export \
  --processes 2 --concurrency 2 --log-file export.log
```

This runs two processes with two active nodes and two export streams each,
prioritizing directories and small nodes. Each process owns its stream pool;
one table stays in one process. Parallel mode supports YT → S3; restore its
archives with `ytclone` as above. Restore reads each node's parts sequentially.

## Before you copy

- **Supported node types:** static tables, files, YSON documents, and directories.
  Dynamic tables are skipped and reported; links are unsupported.
- **One current S3 generation.** An update uploads new objects before atomically
  publishing its manifest, then immediately deletes the previous generation.
  A failed upload leaves the previous copy available. Crashes or cleanup errors
  can leave unused objects; there is no automatic garbage collection.
- **Permissions are not copied.** ACLs, ownership, and account settings are
  excluded. Replacing an existing YT table, file, or document recreates the node,
  so its previous settings are not preserved either.
- **Consistency is per node.** A directory tree is not a single point-in-time
  snapshot.
- **Concurrent exports detect conflicts.** Writers use separate generations and
  conditional manifest publication; a competing update can fail the node instead
  of overwriting another writer. The S3 backend must honor conditional PUTs and
  provide atomic object visibility with ETags. Concurrent writes to the same YT
  restore destination are unsupported.
- **Restore pins one generation.** If an export replaces and deletes it, restore
  fails and rolls back the unfinished node. Rerun restore to use the new generation.

## Documentation

Run `ytclone --help` or `ytclone-parallel --help` for all command-line options.

- [User guide](https://github.com/alvassin/ytclone/blob/main/USAGE.md): paths,
  configuration, progress, retries, and parallel export.
- [Architecture](https://github.com/alvassin/ytclone/blob/main/ARCHITECTURE.md):
  archive format, change detection, and execution diagrams.
- [Contributing](https://github.com/alvassin/ytclone/blob/main/CONTRIBUTING.md):
  installation from source, development, and tests.

## License

[MIT](https://github.com/alvassin/ytclone/blob/main/LICENSE).
