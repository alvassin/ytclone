"""CLI parsing, resource wiring and exit codes without network access."""

import asyncio
import logging
import sys

import pytest
from aiomisc.log import LogFormat

from ytclone import cli
from ytclone.engine import CopyOptions, CopyResult
from ytclone.format import MIB


@pytest.fixture(autouse=True)
def isolated_parser_environment(monkeypatch):
    for name in ("YT_PROXY", "S3_ENDPOINT", "AWS_DEFAULT_REGION", "AIOMISC_LOG_FORMAT"):
        monkeypatch.delenv(name, raising=False)


def arguments(*, restore=False, extra=()):
    parser = cli.Arguments()
    paths = (
        ["s3://bucket/archive", "//copy"]
        if restore
        else ["//source", "s3://bucket/archive"]
    )
    return parser.parse_args(
        args=[*paths, "--yt-proxy", "https://yt.example.test", *extra]
    )


def test_defaults_match_engine_and_aiomisc():
    parsed = arguments()
    defaults = CopyOptions()
    assert parsed.concurrency == defaults.concurrency == 4
    assert parsed.max_streams == defaults.max_streams == 0
    assert parsed.queue_size == defaults.queue_size == 16
    assert parsed.part_size_mib * MIB == defaults.part_size
    assert parsed.buffer_size_mib * MIB == defaults.chunk_size
    assert parsed.compression_level == defaults.compression_level
    assert parsed.log_format == LogFormat.default()
    assert not parsed.no_proxy_discovery


def test_environment_and_explicit_cli_override(monkeypatch):
    monkeypatch.setenv("YT_PROXY", "https://from-env.example.test")
    monkeypatch.setenv("S3_ENDPOINT", "https://s3-env.example.test")
    parser = cli.Arguments()
    parser.parse_args(args=["//source", "s3://bucket"])
    assert parser.yt_proxy == "https://from-env.example.test"
    assert parser.endpoint == "https://s3-env.example.test"
    assert arguments().yt_proxy == "https://yt.example.test"


@pytest.mark.parametrize(
    "source,destination,expected",
    [
        ("//source", "s3://bucket/archive", (True, "//source", "bucket", "archive")),
        (
            "s3://bucket/archive/home/source",
            "//copy",
            (False, "//copy", "bucket", "archive/home/source"),
        ),
    ],
)
def test_direction_comes_from_argument_order(source, destination, expected):
    assert cli.direction(source=source, destination=destination) == expected


@pytest.mark.parametrize(
    "source,destination",
    [
        ("//source", "//copy"),
        ("s3://one", "s3://two"),
        ("relative/path", "s3://bucket"),
        ("//", "s3://bucket"),
        ("//source/../other", "s3://bucket"),
        ("//source[#1:#2]", "s3://bucket"),
        ("//source&", "s3://bucket"),
        ("//source", "s3://bucket/path?query=1"),
    ],
)
def test_invalid_addresses_are_rejected(source, destination):
    with pytest.raises(ValueError):
        cli.direction(source=source, destination=destination)


@pytest.mark.parametrize("restore", [False, True])
@pytest.mark.parametrize("failures", [0, 1])
def test_run_wires_keyword_only_adapters_and_closes_session(
    monkeypatch, caplog, restore, failures
):
    """Log the memory budget before constructing transfer resources."""
    calls = {}
    options = CopyOptions(
        concurrency=2,
        max_streams=0 if restore else 6,
        chunk_size=8 * MIB,
        compression_level=7,
    )
    parsed = arguments(
        restore=restore, extra=("--no-proxy-discovery", "--endpoint", "http://s3.test/")
    )

    class Session:
        def __init__(self, *, timeout):
            calls["timeout"] = timeout

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            calls["session_closed"] = True

    def estimate_memory(
        *, concurrency, snapshot_concurrency, chunk_size, compression_level, export
    ):
        """Record the actual transfer settings used for the startup estimate."""
        calls["estimate"] = (
            concurrency,
            snapshot_concurrency,
            chunk_size,
            compression_level,
            export,
        )
        return 512

    def yt_adapter(*, proxy, proxy_discovery, progress):
        """Capture logs at the first resource-construction boundary."""
        calls["startup_logs"] = [
            record for record in caplog.records if record.name == cli.__name__
        ]
        calls["yt"] = (proxy, proxy_discovery)
        return "yt-adapter"

    def s3_client(*, session, url, region):
        assert isinstance(session, Session)
        calls["s3"] = (url, region)
        return "s3-client"

    def storage(*, client, chunk_size, progress):
        calls["storage"] = (client, chunk_size)
        return "storage-adapter"

    async def export_tree(*, yt, storage, source, prefix, cluster, options, progress):
        calls["transfer"] = ("export", yt, storage, source, prefix, cluster, options)
        return CopyResult(completed=2, skipped=1, failed=failures)

    async def restore_tree(
        *, yt, storage, source_prefix, destination, options, progress
    ):
        calls["transfer"] = (
            "restore",
            yt,
            storage,
            destination,
            source_prefix,
            options,
        )
        return CopyResult(completed=2, skipped=1, failed=failures)

    caplog.set_level(logging.INFO, logger=cli.__name__)
    monkeypatch.setattr(cli, "estimate_memory_mib", estimate_memory)
    monkeypatch.setattr(cli.aiohttp, "ClientSession", Session)
    monkeypatch.setattr(cli, "YtAdapter", yt_adapter)
    monkeypatch.setattr(cli, "S3Client", s3_client)
    monkeypatch.setattr(cli, "S3Storage", storage)
    monkeypatch.setattr(cli, "export_tree", export_tree)
    monkeypatch.setattr(cli, "restore_tree", restore_tree)

    result = asyncio.run(cli.run(arguments=parsed, options=options))
    assert result == failures
    assert calls["estimate"] == (
        2 if restore else 6,
        0 if restore else 2,
        8 * MIB,
        7,
        not restore,
    )
    startup = calls["startup_logs"]
    expected_direction = "s3-to-yt" if restore else "yt-to-s3"
    expected_start = (
        f"copy_started direction={expected_direction} concurrency=2 "
        "buffer_size_mib=8 memory_estimate_mib=512"
    )
    expected_note = (
        "Memory estimate is a planning budget; large rows, metadata and allocator "
        "overhead can exceed it."
    )
    expected_logs = [expected_start, expected_note]
    if not restore:
        expected_logs.append("export_parallelism max_streams=6")
    assert [record.getMessage() for record in startup] == expected_logs
    assert all(record.levelno == logging.INFO for record in startup)
    assert calls["yt"] == ("https://yt.example.test", False)
    assert calls["s3"] == ("http://s3.test/bucket", "us-east-1")
    assert calls["storage"] == ("s3-client", 8 * MIB)
    assert calls["transfer"][:5] == (
        "restore" if restore else "export",
        "yt-adapter",
        "storage-adapter",
        "//copy" if restore else "//source",
        "archive",
    )
    assert calls["transfer"][-1] is options
    assert calls["timeout"].total is None
    assert calls["session_closed"]


@pytest.mark.parametrize(
    "extra",
    [
        ("--concurrency", "0"),
        ("--max-streams", "-1"),
        ("--queue-size", "0"),
        ("--buffer-size-mib", "4"),
        ("--part-size-mib", "0"),
        ("--compression-level", "23"),
        ("--progress-interval", "0"),
        ("--progress-interval", "nan"),
        ("--progress-interval", "inf"),
    ],
)
def test_main_rejects_invalid_options_before_starting_resources(monkeypatch, extra):
    def unexpected_run(*args, **kwargs):
        pytest.fail("Runtime must not start for invalid arguments")

    monkeypatch.setattr(cli.aiomisc, "run", unexpected_run)
    monkeypatch.setattr(
        sys,
        "argv",
        ["ytclone", "//source", "s3://bucket", "--yt-proxy", "test", *extra],
    )
    with pytest.raises(SystemExit) as exc:
        cli.main()
    assert exc.value.code == 2


def test_main_rejects_export_stream_setting_for_restore(monkeypatch, capsys):
    """Reject an export-only setting instead of silently changing restore limits."""
    monkeypatch.setattr(
        cli.aiomisc,
        "run",
        lambda *args, **kwargs: pytest.fail("Invalid restore must not start"),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "ytclone",
            "s3://bucket/archive",
            "//copy",
            "--yt-proxy",
            "test",
            "--max-streams",
            "4",
        ],
    )
    with pytest.raises(SystemExit) as error:
        cli.main()
    assert error.value.code == 2
    assert "export only" in capsys.readouterr().err


@pytest.mark.parametrize(
    "requested_format,effective_format,date_format",
    [
        ("plain", "stream", cli.LOG_DATE_FORMAT),
        ("stream", "stream", cli.LOG_DATE_FORMAT),
        ("color", "color", cli.LOG_DATE_FORMAT),
        ("rich", "rich", cli.LOG_DATE_FORMAT),
        ("rich_tb", "rich_tb", cli.LOG_DATE_FORMAT),
        ("json", "json", cli.LOG_DATE_FORMAT),
        ("disabled", "disabled", cli.LOG_DATE_FORMAT),
        ("syslog", "syslog", None),
        ("journald", "journald", None),
    ],
)
def test_main_passes_aiomisc_logging_options_and_copy_settings(
    monkeypatch, requested_format, effective_format, date_format
):
    """Enable dated output while keeping native structured and system formats."""
    seen = {}

    async def run(*, arguments, options):
        seen["arguments"] = arguments
        seen["options"] = options
        return 1

    def aiomisc_run(
        coro, *, pool_size, log_level, log_format, log_date_format, log_config
    ):
        assert log_config
        seen["runtime"] = (pool_size, log_level, log_format, log_date_format)
        return asyncio.run(coro)

    monkeypatch.setattr(cli, "run", run)
    monkeypatch.setattr(cli.aiomisc, "run", aiomisc_run)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "ytclone",
            "//source",
            "s3://bucket",
            "--yt-proxy",
            "test",
            "--concurrency",
            "5",
            "--max-streams",
            "7",
            "--log-level",
            "debug",
            "--log-format",
            requested_format,
        ],
    )
    assert cli.main() == 1
    assert seen["options"].concurrency == 5
    assert seen["options"].max_streams == 7
    assert seen["runtime"] == (6, "debug", effective_format, date_format)


@pytest.mark.parametrize(
    "failure,expected", [(KeyboardInterrupt(), 130), (OSError("connection failed"), 1)]
)
def test_main_returns_actionable_exit_status(monkeypatch, failure, expected):
    def aiomisc_run(coro, **kwargs):
        coro.close()
        raise failure

    monkeypatch.setattr(cli.aiomisc, "run", aiomisc_run)
    monkeypatch.setattr(
        sys, "argv", ["ytclone", "//source", "s3://bucket", "--yt-proxy", "test"]
    )
    assert cli.main() == expected


def test_log_file_and_progress_interval_parse():
    """Enable summaries explicitly without changing the existing default mode."""
    parsed = arguments(extra=("--log-file", "copy.log", "--progress-interval", "2.5"))
    assert parsed.log_file == "copy.log"
    assert parsed.progress_interval == 2.5
    assert arguments().log_file is None


@pytest.mark.parametrize(
    "outcome,code,state",
    [
        ("ok", 0, "finished"),
        ("error", 1, "failed"),
        ("cancel", 130, "cancelled"),
    ],
)
def test_main_file_mode_real_runtime(tmp_path, outcome, code, state):
    """Exercise aiomisc shutdown and both output streams in an isolated process."""
    import os
    import subprocess
    from pathlib import Path

    log_path = tmp_path / "copy.log"
    script = r"""
import asyncio
import logging
import sys
from contextlib import asynccontextmanager
from ytclone import cli
from ytclone.engine import CopyResult
from ytclone.format import SourceEstimate

log_path, outcome = sys.argv[1:]

class Backend:
    async def estimate_source(self, *, path):
        return SourceEstimate(node_count=12, disk_size=4096)

class Yt:
    def __init__(self, **kwargs):
        pass

    @asynccontextmanager
    async def worker(self):
        yield Backend()

async def transfer(*, progress, **kwargs):
    await asyncio.sleep(0)
    progress.discovered = 3
    progress.completed = 1
    progress.unchanged = 1
    progress.skipped = 1
    progress.transferred_bytes = 2048
    progress.completed_bytes = 1024
    logging.getLogger('ytclone.engine').info('node_completed detail_only')
    if outcome == 'error':
        raise RuntimeError('test_error_details')
    if outcome == 'cancel':
        raise asyncio.CancelledError
    return CopyResult(completed=1, skipped=2)

cli.YtAdapter = Yt
cli.S3Client = lambda **kwargs: object()
cli.export_tree = transfer
sys.argv = ['ytclone', '//source', 's3://bucket', '--yt-proxy', 'unused',
            '--log-file', log_path, '--log-format', 'stream']
raise SystemExit(cli.main())
"""
    environment = os.environ.copy()
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    process = subprocess.run(
        args=[sys.executable, "-c", script, str(log_path), outcome],
        cwd=Path(__file__).parents[1],
        env=environment,
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert process.returncode == code, process.stderr
    assert "RAM estimate:" in process.stdout
    assert f" {state} " in process.stdout.splitlines()[-1]
    assert "copied=1 unchanged=1 skipped=1" in process.stdout
    assert "uploaded=2.0 KiB published=1.0 KiB" in process.stdout
    assert "node_completed" not in process.stdout
    assert process.stderr == ""
    details = log_path.read_text()
    assert "copy_started" in details
    assert f"copy_progress {state}" in details
    assert "node_completed detail_only" in details
    if outcome == "error":
        assert "RuntimeError: test_error_details" in details
        assert "copy_failed" in details
    elif outcome == "cancel":
        assert "copy_cancelled" in details
    else:
        assert "copy_finished" in details


def test_estimate_failure_does_not_fail_copy(caplog):
    """An unavailable optional count keeps the total unknown."""
    from contextlib import asynccontextmanager

    from ytclone.progress import Progress

    caplog.set_level(logging.WARNING, logger=cli.__name__)

    class Yt:
        @asynccontextmanager
        async def worker(self):
            """Simulate a failure before the optional estimate can be read."""
            raise OSError("metadata unavailable")
            yield  # pragma: no cover

    progress = Progress()
    asyncio.run(cli.estimate_total(yt=Yt(), path="//source", progress=progress))
    assert progress.estimated_total is None
    assert progress.estimated_source_bytes is None
    assert "source_estimate_unavailable" in caplog.text


def test_estimate_sets_independent_node_and_disk_totals(caplog):
    """Both summary values come from one optional metadata operation."""
    from contextlib import asynccontextmanager

    from ytclone.format import SourceEstimate
    from ytclone.progress import Progress

    class Backend:
        async def estimate_source(self, *, path):
            """Return both values from one query."""
            assert path == "//source"
            return SourceEstimate(node_count=12, disk_size=4096)

    class Yt:
        @asynccontextmanager
        async def worker(self):
            """Provide a standalone metadata worker."""
            yield Backend()

    progress = Progress()
    with caplog.at_level("INFO"):
        asyncio.run(cli.estimate_total(yt=Yt(), path="//source", progress=progress))
    assert progress.estimated_total == 12
    assert progress.estimated_source_bytes == 4096
    assert "nodes=12 disk_space=4096" in caplog.text
