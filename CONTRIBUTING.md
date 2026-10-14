# Contributing

## Set up a checkout

See the [installation requirements](README.md#installation) for supported Python
versions and platforms. With [uv](https://docs.astral.sh/uv/) installed:

```bash
git clone https://github.com/alvassin/ytclone.git
cd ytclone
uv sync --locked --group dev
uv run --locked ytclone --help
uv run --locked ytclone-parallel --help
```

Run either command from the checkout with `uv run --locked`. See the
[usage guide](USAGE.md) for examples and options, and
[architecture guide](ARCHITECTURE.md) for module responsibilities, archive format,
and export/restore sequences.

## Run checks

Run the complete offline suite and lint checks:

```bash
YTCLONE_LOCAL_TESTS=0 uv run --locked pytest
uv run --locked ruff check src tests
uv run --locked ruff format --check src tests
```

The offline suite uses in-memory fakes, local HTTP servers, and subprocesses. It
needs no YTsaurus or S3 services or credentials. The five local integration cases
are skipped unless explicitly enabled.

## Test with local YTsaurus and S3

Docker Compose provides YTsaurus on `http://localhost:8000` and an S3-compatible
SeaweedFS service on `http://localhost:8333`, bound to the loopback interface.
The pinned YTsaurus image is x86_64 and requires emulation on Apple Silicon.

```bash
docker compose up -d --wait --wait-timeout 360 yt s3
YTCLONE_LOCAL_TESTS=1 uv run --locked pytest -s tests/test_local.py
```

The tests use these local endpoints explicitly, disable YT token authentication
and proxy discovery, and use the dummy S3 credentials from `compose.yml`. No
production credentials are needed.

Each run creates unique `//tmp/ytclone-main-*` and `//tmp/ytclone-raw-retry-*`
paths, plus `main-smoke-*` and `raw-retry-*` prefixes in the local `yt-backup`
bucket. Tests cover export and restore, repeated copies, changed metadata, empty
nodes, file parts, corruption rollback, and table-read retries within a snapshot.

Fixtures are retained for inspection while the local containers and their storage
exist. To stop the services while keeping containers and data:

```bash
docker compose stop
```
