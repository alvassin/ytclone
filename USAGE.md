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

## Copying a subset

To restore one table or file, point to its path inside the archive:

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
| `--concurrency` | `4` | Concurrent node transfers; per process in parallel mode. |
| `--queue-size` | `16` | Pending paths in ordinary mode; control messages per process in parallel mode. |
| `--part-size-mib` | `256` | Target **uncompressed** bytes per archive object; approximate for tables. |
| `--buffer-size-mib` | `32` | S3 multipart buffer per transfer, from 5 to 512 MiB. |
| `--compression-level` | `3` | Zstandard level, from -7 to 22; higher levels use more CPU and memory. |

An archive part is one S3 object; multipart buffers are chunks within that object.
`--part-size-mib` does not allocate a whole part in RAM. Objects smaller than the
buffer use a single PUT; objects at or above that size use multipart upload.

Lower concurrency or buffer size to reduce memory use. The startup RAM estimate
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
supports YT → S3 only; one table still occupies one slot regardless of its size.

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
