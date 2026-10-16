# User guide

Start with the [README](README.md) for installation, credentials, and a first
copy. Use `ytclone --help` or `ytclone-parallel --help` for the full option list.

## Connection settings

| Option | Environment variable | Default |
| --- | --- | --- |
| `--yt-proxy` | `YT_PROXY` | Required |
| `--endpoint` | `S3_ENDPOINT` | `https://s3.amazonaws.com` |
| `--region` | `AWS_DEFAULT_REGION` | `us-east-1` |

S3 settings and AWS credentials are used only for S3 endpoints. Local copies
require only YTsaurus access and filesystem permissions.

For local Docker YTsaurus, use `--yt-proxy http://localhost:8000` with
`--no-proxy-discovery`. See [local integration tests](CONTRIBUTING.md#test-with-local-ytsaurus-and-s3).

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

## Local archives

One argument must be a `//YT/path`. The other may be an S3 URL, an absolute or
relative filesystem path, or a `file:///absolute/path` URL. Plain filesystem
paths are used literally; `file:` URLs use URL escaping. Remote file hosts,
queries, fragments and unknown URL schemes are rejected.

```bash
ytclone //home/analytics/events ./dump
ytclone ./dump/home/analytics/events //home/analytics/events_copy
ytclone-parallel //home/analytics/events ./dump --processes 2 --concurrency 2
```

Export creates `@archive.json` at the destination root and appends the complete
YT path beneath it, using the same encoded names, attributes, manifests and
compressed data parts as an S3 archive. Tables are stored as binary YSON with
their schemas; this command does not import arbitrary CSV/Parquet or ordinary
files as YT nodes.

The first export requires a new or empty directory. Later exports into the same
root skip unchanged nodes and retry unfinished nodes. Put logs outside a new
archive root, so they do not make it nonempty before initialization. Archive
roots cannot be nested. Restoring a subtree locates its enclosing `@archive.json`;
move the entire root, including that marker, to relocate an archive without
changing its manifests. Moving only a subtree loses the root context.

Local data parts are streamed through bounded buffers. Each object is written
to a temporary sibling, synchronized and renamed into place; its node manifest
is still published last. Disk-full, permission and checksum errors fail the
copy. After correcting the cause, rerun the command. This does not retain the
previous complete version while replacing a node, and overlapping writers or
restoring during replacement remain unsupported.

The local adapter targets POSIX filesystems on the supported Linux/macOS
platforms. Symlinks inside archives and paths escaping the root are rejected.
Names that collide on the destination filesystem or exceed its filename limits
fail explicitly. Network filesystems must provide the required synchronization
and atomic rename semantics. A killed process can leave temporary files, which
are ignored when restoring; completed nodes remain usable.

When running in Docker, mount the archive directory and pass its **container**
path to `ytclone`, for example `-v "$PWD/dump:/archive"` and destination `/archive`.
The container user needs write permission on the mount.

## Copying a subset

To restore one table or file, point to its path inside the archive:

```bash
ytclone s3://my-bucket/export/home/analytics/events/2026-01-01 \
  //home/analytics/events_copy_one
```

To expand an export from a child directory to its parent, keep the same S3 prefix.
For example, export both `//home/analytics/events` and `//home/analytics` to
`s3://my-bucket/export`. Existing unchanged nodes are skipped; siblings are added.
The same rule applies to a local archive root such as `./dump`.

## Tuning

| Option | Default | Effect |
| --- | --- | --- |
| `--concurrency` | `4` | Concurrent node transfers; per process in parallel mode. |
| `--queue-size` | `16` | Pending paths in ordinary mode; control messages per process in parallel mode. |
| `--part-size-mib` | `256` | Target **uncompressed** bytes per archive object; approximate for tables. |
| `--buffer-size-mib` | `32` | Transfer buffer, from 5 to 512 MiB; multipart buffer for S3. |
| `--compression-level` | `3` | Zstandard level, from -7 to 22; higher levels use more CPU and memory. |

An archive part is one S3 object or one local file; multipart buffers are chunks
within an S3 object. `--part-size-mib` does not allocate a whole part in RAM.
S3 objects smaller than the buffer use a single PUT; larger ones use multipart
upload. Local objects are streamed directly into temporary files.

Lower concurrency or buffer size to reduce memory use. The startup RAM estimate
is a planning budget, **not a limit**: large rows, metadata, and allocator overhead
can exceed it. Parallel mode multiplies transfer memory by process count and also
keeps discovered metadata in RAM; `--queue-size` does not bound that metadata.
Local exports use the same conservative estimate, including allowances for S3
multipart buffers that they do not allocate.

## Progress and logs

`--log-file copy.log` appends detailed logs to a file and prints console summaries.
Set their interval with `--progress-interval` (default: 5 seconds). Without a log
file, ordinary mode writes detailed logs to the console.

| Counter | Meaning |
| --- | --- |
| `copied / unchanged / skipped / errors` | Completed nodes, matching copies, reported skips, and node/discovery errors. |
| `queued / active` | Pending nodes and busy transfer slots. |
| `uploaded / published` | Compressed bytes sent versus bytes belonging to completed archive nodes. |
| `downloaded / restored` | Compressed bytes received versus bytes belonging to completed restores. |

Transfer counters can include failed attempts; completion counters do not. Source
sizes are YT storage weights, including replication and compression, **not S3
bytes**. `discovered` totals and export ETA are estimates: `?` means unavailable,
`~` means approximate. Restore has no ETA. Request timings overlap and should not
be added together as total elapsed time.

## Parallel export

`--processes N --concurrency C` allows up to N × C concurrent transfers. It
supports YT → S3 and YT → local disk; one table still occupies one slot regardless
of its size.

- `--discovery-concurrency` (default: 4) controls concurrent directory listings.
- Directories and smaller known nodes go first. While discovery runs, one slot is
  reserved for nodes up to `--small-table-mib` (default: 64). Running transfers
  are not preempted.
- `--worker-restarts` (default: 3) limits crash replacements per process. Ordinary
  node failures use transfer retries instead of restarting the process.

With `--log-file export.log`, workers write `export-worker-N.log`. The coordinator
aggregates node and byte counts, but not worker ETA, source-size estimates, or
request timings. An abrupt worker death can lose its last unreported byte counts.
Do not start another copy targeting the same nodes; coordination is limited to
one invocation.

## Errors and recovery

Temporary failures are retried automatically and logged:

- **Table reads:** retry the failed archive part within the same snapshot, up to
  three attempts. A lost snapshot fails the node.
- **S3 requests:** up to 30 attempts, with a 45-minute budget per retried request.
- **Restore:** after a confirmed rollback, retry the entire node in a new
  transaction, up to 30 attempts. Permission and checksum errors are not retried.

An uncertain commit or rollback stops automatic restore retries. A failure while
recording completion can occur **after the data was committed**; inspect the log
before assuming that the old destination is still present.

After fixing a failure, [rerun the command](README.md#repeated-runs). Completed,
unchanged nodes are skipped; an unfinished table or file starts from the beginning.

Configure an S3 lifecycle rule to expire incomplete multipart uploads. Known
uploads are aborted on failure, but an upload whose creation response was lost
cannot be aborted by the client.

Exit codes: `0` completed, `1` copy/discovery failure, `2` invalid arguments,
`130` interrupted. Dynamic-table skips do not fail the run; check `skipped`.

See [copying limitations](README.md#before-you-copy) and
[replacement semantics](ARCHITECTURE.md#completion-and-replacement) before
overwriting existing data. The [archive format and skip rules](ARCHITECTURE.md)
are documented separately.
