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
uv run --locked ruff check src tests scripts
uv run --locked ruff format --check src tests scripts
```

The offline suite uses in-memory fakes, local HTTP servers, and subprocesses. It
needs no YTsaurus or S3 services or credentials. The six local integration cases
are skipped unless explicitly enabled.

Known limitation: aiomisc can exit incorrectly after coroutine cleanup completes.
Version 18.0.33 reports a false shutdown timeout and exits 70 on SIGINT/SIGTERM;
17.10.3 on Linux/Python 3.10 can exit 1 on SIGINT because it stops the event loop
before shutdown finishes. The signal test marks only these exact versions,
exit codes, and diagnostics as expected failures (`xfail`), after checking
cleanup, protocol output, and timely process exit. Other failures still fail CI;
the dependency fix is deferred.

## Test with local YTsaurus and S3

Docker Compose provides YTsaurus on `http://localhost:8000` and an S3-compatible
SeaweedFS service on `http://localhost:8333`, bound to the loopback interface.
The pinned YTsaurus image is x86_64 and requires emulation on Apple Silicon.

```bash
docker compose up -d --wait --wait-timeout 360 yt s3
YTCLONE_LOCAL_TESTS=1 uv run --locked pytest -s tests/test_local.py tests/test_local_cli.py
```

The tests use these local endpoints explicitly, disable YT token authentication
and proxy discovery, and use the dummy S3 credentials from `compose.yml`. No
production credentials are needed.

Each run creates unique `//tmp/ytclone-main-*`, `//tmp/ytclone-raw-retry-*`, and
`//tmp/ytclone-cli-*` paths, plus `main-smoke-*`, `raw-retry-*`, and `cli-smoke-*`
prefixes in the local `yt-backup` bucket. Tests cover export and restore, repeated
copies, changed metadata, empty nodes, file parts, corruption rollback, and
table-read retries within a snapshot. The CLI roundtrip exports five nodes using
two real worker processes, restores them, and repeats both commands to verify
unchanged objects are skipped.

Fixtures are retained for inspection while the local containers and their storage
exist. To stop the services while keeping containers and data:

```bash
docker compose stop
```

## CI and releases

[`ci.yml`](.github/workflows/ci.yml) runs on pull requests and pushes to `main`.
The shared checks run Ruff and the offline suite on Linux x86_64 and macOS ARM64
with Python 3.10, 3.11, and 3.14. They build and validate wheel/sdist packages and
run all local integration tests against an installed wheel on Linux with Python
3.10 and 3.14.

[`release.yml`](.github/workflows/release.yml) publishes only after a pull request
is merged into `main`. It reruns the checks for that exact merged commit and
uploads the tested artifacts through PyPI Trusted Publishing. Direct pushes and
tag pushes do not publish packages.

Versions come from [`scripts/release_version.py`](scripts/release_version.py):
the nearest `vMAJOR.MINOR.PATCH` tag on the first-parent history sets the base;
all commits since that tag, including commits from merged branches and merge
commits themselves, are added to its patch number. For example, `v0.1.0` plus five
commits becomes `0.1.5`. Tags on side branches do not change the release series.
Release jobs require a tag and complete Git history; untagged CI checkouts use
`0.1.0` at the initial commit as their fallback base.

The initial series starts at `v0.1.0` on the initial commit. Push this baseline
tag before the first release:

```bash
git push origin v0.1.0
```

To start a new minor series, update local `main`, then run:

```bash
git tag v0.2.0 main
git push origin v0.2.0
```

Use `v1.0.0` for a new major series. The next PR merge publishes the next computed
version. Build jobs stamp that version in their checkout; the committed
`pyproject.toml` version stays `0.1.0` for local development. No automatic version
commits or patch tags are created.

For the initial PyPI setup, create the GitHub environment `pypi` and configure a
[pending publisher](https://docs.pypi.org/trusted-publishers/creating-a-project-through-oidc/)
with project `ytclone`, owner `alvassin`, repository `ytclone`, workflow
`release.yml`, and environment `pypi`. The first successful upload creates the
PyPI project; no PyPI API token is needed in GitHub.
