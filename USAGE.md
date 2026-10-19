# User guide

Start with the [README](README.md) for installation, credentials, and a first
copy. Use `ytclone --help` or `ytclone-parallel --help` for the full option list.

## Connection settings

| Option | Environment variable | Default |
| --- | --- | --- |
| `--yt-proxy` | `YT_PROXY` | Required |
| `--endpoint` | `S3_ENDPOINT` | `https://s3.amazonaws.com` |
| `--region` | `AWS_DEFAULT_REGION` | `us-east-1` |

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
| `--compression-level` | `3` | Zstandard level, from -7 to 22; higher levels use more CPU and memory. |

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
| `uploaded / published` | Compressed bytes sent versus bytes belonging to completed archive nodes. |
| `downloaded / restored` | Compressed bytes received versus bytes belonging to completed restores. |

Transfer counters can include failed attempts; completion counters do not. Source
sizes are YT storage weights, including replication and compression, **not S3
bytes**. `discovered` totals and export ETA are estimates: `?` means unavailable,
`~` means approximate. Restore has no ETA. Request timings overlap and should not
be added together as total elapsed time.

## Parallel export

`--processes P --concurrency C --max-streams N` allows up to P × C active nodes
and P × N export streams. With the default `--max-streams 0`, N equals C.
Each process has its own stream pool; a table stays in one process and can use
that process's N streams. This command supports YT → S3 only.

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

On an export error, the node stops scheduling new ranges and waits for its active
ranges to finish cleanup before closing the snapshot. Cancellation also drains
active range workers before closing their snapshots. The manifest is published
only after all parts and the snapshot have completed successfully.

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
