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

Export has separate limits for node lifecycles (`--concurrency`) and a shared pool
of range workers (`--max-streams`, defaulting to the node limit). When changing
scheduling, verify that one table can fill the pool, multiple ready nodes take
turns, files stay sequential, and failures or cancellation drain active ranges
before their snapshot closes. Restore and the archive format remain unchanged;
parallel export does not provide persistent progress within an unfinished node.

## Run checks

Run the complete offline suite and lint checks:

```bash
YTCLONE_LOCAL_TESTS=0 uv run --locked pytest
uv run --locked ruff check src tests scripts packaging
uv run --locked ruff format --check src tests scripts packaging
```

The offline suite uses in-memory fakes, local HTTP servers, and subprocesses. It
needs no YTsaurus or S3 services or credentials. Local integration cases
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
`//tmp/ytclone-cli-*` paths, plus `main-smoke-*`, `raw-retry-*`, `cli-smoke-*`, and `cli-streams-*`
prefixes in the local `yt-backup` bucket. Tests cover export and restore, repeated
copies, changed metadata, empty nodes, file parts, corruption rollback, and
table-read retries within a snapshot. Document tests also cover scalar, empty and
attributed YSON, a value exceeding the default node-list limit, concurrent source
changes, changed destinations, and corrupt-archive rollback. The CLI roundtrips
cover six nodes using two real worker processes and a 24 MiB table using
`--concurrency 1 --max-streams 3` with both CLI commands. They run with and without
`--yt-zstd-passthrough`, restore without the flag, and repeat the commands to
verify unchanged objects are skipped. The large table exercises multiple archive
parts and S3 multipart uploads. Tests also read the stored objects, verify their
checksums, and decode every Zstandard frame: passthrough must retain multiple
proxy frames per part, while local compression produces one frame. This catches
a flag that is accepted but not forwarded to the table readers.

Fixtures are retained for inspection while the local containers and their storage
exist. To stop the services while keeping containers and data:

```bash
docker compose stop
```

## Build and test the Docker image

The Dockerfile installs a prebuilt wheel and runtime dependencies from `uv.lock`.
Build into an empty temporary directory so stale wheels cannot enter the image:

```bash
context=$(mktemp -d)
cp Dockerfile .dockerignore pyproject.toml uv.lock "$context/"
uv build --out-dir "$context/dist"
docker build --platform linux/amd64 -t ytclone:ci "$context"
# With the disposable local services above running:
YTCLONE_LOCAL_TESTS=1 YTCLONE_DOCKER_IMAGE=ytclone:ci \
  uv run --locked pytest -s tests/test_local_cli.py
```

The Docker test uses host networking, supported by Linux Docker Engine and
compatible desktop runtimes. It runs the image's two installed commands with real
workers and retains logs in pytest's temporary directory. The image includes no
build tools or dependency caches. Base images are pinned by digest; update the
pins explicitly when taking upstream updates.

## Build standalone archives

Portable builds use CPython 3.14 and PyInstaller from the locked `portable`
dependency group. Install the wheel into a separate environment so the bundle
contains that exact package, rather than the editable checkout. For the local
development version:

```bash
build_dir=$(mktemp -d)
uv build --wheel --out-dir "$build_dir/wheels"
uv export --locked --no-dev --group portable --no-emit-project \
  --output-file "$build_dir/requirements.txt"
uv venv --python 3.14 "$build_dir/venv"
uv pip install --python "$build_dir/venv/bin/python" --require-hashes \
  -r "$build_dir/requirements.txt"
uv pip install --python "$build_dir/venv/bin/python" --no-deps "$build_dir"/wheels/*.whl
"$build_dir/venv/bin/python" scripts/build_portable.py \
  --version 0.1.0 --output-dir "$build_dir/dist"
```

Build separately on each target platform. CI uses [`packaging/Dockerfile`](packaging/Dockerfile), with shared Python
on `manylinux_2_28_x86_64`, to set the Linux glibc baseline and macOS 14 ARM64 for Apple Silicon. Building on a newer
Linux system does not preserve the published glibc 2.28 compatibility promise.
Each archive contains one executable, a `ytclone-parallel` symlink, and shared
`_internal` libraries; it is accompanied by a `.tar.gz.sha256` file. macOS builds
use ad-hoc signing, without an Apple Developer certificate or notarization.

Extract the archive, then check its runtime and worker lifecycle:

```bash
uv run --locked python scripts/check_portable.py \
  --bundle /path/to/extracted/ytclone-VERSION-PLATFORM \
  --version VERSION
# With the disposable local services above running:
YTCLONE_LOCAL_TESTS=1 YTCLONE_PORTABLE_DIR=/path/to/extracted/ytclone-VERSION-PLATFORM \
  uv run --locked pytest -s tests/test_local_cli.py
```

The offline check covers both commands, native codecs, TLS certificates, worker
protocol and shutdown. Linux CI also runs the full CLI roundtrip against the
extracted bundle; the known aiomisc shutdown exception is checked explicitly.

## CI and releases

[`ci.yml`](.github/workflows/ci.yml) runs on pull requests and pushes to `main`.
The shared checks run Ruff and the offline suite on Linux x86_64 and macOS ARM64
with Python 3.10, 3.11, and 3.14. They build and validate wheel/sdist packages and
run all local integration tests against an installed wheel on Linux with Python
3.10 and 3.14. A separate Docker job tests both commands, export/restore and
repeated runs, and limits the unpacked image size to 200 MiB. Standalone jobs
build and check Linux x86_64 and macOS ARM64 archives from the same wheel.

[`release.yml`](.github/workflows/release.yml) starts on pushes to `main` and
publishes only when GitHub confirms that the exact commit belongs to a merged
PR targeting this repository's `main`. This covers merge, squash, and rebase
merges. Direct pushes without a matching merged PR skip publication; tag pushes
do not start the workflow. The release reruns the checks for that exact commit
and uploads the tested distributions through PyPI Trusted Publishing. After
PyPI succeeds, it loads the tested Docker image artifact, verifies its image ID,
and publishes `ghcr.io/alvassin/ytclone:<version>` without rebuilding. Only the
image publishing job has `packages: write`. It uses the `push` event because
PyPI forbids Trusted Publishing from `pull_request_target`.

After PyPI and GHCR succeed, the workflow creates a GitHub Release at the exact
merged commit and uploads the two tested standalone archives and their SHA256
files. Assets are prepared in a draft before publication. Its `release-VERSION`
tag does not affect the `vMAJOR.MINOR.PATCH` version baselines below. Published
assets are never overwritten; a retry verifies that their bytes match. Releases
do not automatically change GitHub's `latest` designation.

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
commits or `vMAJOR.MINOR.PATCH` patch tags are created.

For the initial PyPI setup, create the GitHub environment `pypi` and configure a
[pending publisher](https://docs.pypi.org/trusted-publishers/creating-a-project-through-oidc/)
with project `ytclone`, owner `alvassin`, repository `ytclone`, workflow
`release.yml`, and environment `pypi`. The first successful upload creates the
PyPI project; no PyPI API token is needed in GitHub.

After the first GHCR publication, set the `ytclone` package visibility to public
in its package settings and verify an anonymous pull. GitHub creates new packages
private by default. Only version tags are published, so concurrent releases
cannot overwrite a shared `latest` tag. PyPI, GHCR, and GitHub Releases are
separate publication steps; if one fails, rerun the failed jobs.
