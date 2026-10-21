"""Public parallel CLI validation and adapter configuration."""

import asyncio
import logging
import sys

import pytest

from ytclone.parallel import cli
from ytclone.parallel.protocol import MAX_MESSAGE_SIZE, decode_message, encode_message


def arguments(*extra):
    """Parse a complete command without reading credentials or making requests."""
    result = cli.Arguments()
    result.parse_args(
        [
            "//home/test",
            "s3://bucket/prefix",
            "--yt-proxy",
            "https://yt.invalid",
            *extra,
        ]
    )
    return result


@pytest.mark.parametrize(
    "flag,value",
    [
        ("--processes", "0"),
        ("--discovery-concurrency", "0"),
        ("--worker-restarts", "-1"),
        ("--small-table-mib", "-1"),
        ("--progress-interval", "nan"),
        ("--concurrency", "0"),
        ("--max-streams", "-1"),
    ],
)
def test_invalid_settings(flag, value):
    """Fail unsafe counts before child or network initialization."""
    with pytest.raises(ValueError):
        cli.validate(arguments(flag, value))


@pytest.mark.parametrize("extra", [(), ("--yt-zstd-passthrough",)])
def test_reject_restore(extra):
    """Do not apply export ordering to restore directory dependencies."""
    args = cli.Arguments()
    args.parse_args(
        ["s3://bucket/prefix", "//home/test", "--yt-proxy", "yt.invalid", *extra]
    )
    with pytest.raises(ValueError, match="YT to S3"):
        cli.validate(args)


def test_main_rejects_restore_passthrough_before_starting_resources(monkeypatch):
    """An export transport option cannot start clients or workers on restore."""
    monkeypatch.setattr(
        cli.aiomisc,
        "run",
        lambda *args, **kwargs: pytest.fail("Invalid restore must not start"),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "ytclone-parallel",
            "s3://bucket/prefix",
            "//home/test",
            "--yt-proxy",
            "yt.invalid",
            "--yt-zstd-passthrough",
        ],
    )
    with pytest.raises(SystemExit) as error:
        cli.main()
    assert error.value.code == 2


@pytest.mark.parametrize("yt_zstd_passthrough", [False, True])
def test_run_passes_nonsecret_config_and_reports_failure(
    monkeypatch, capsys, caplog, yt_zstd_passthrough
):
    """Preserve endpoint and worker options without performing live transfers."""
    args = arguments(
        "--processes",
        "3",
        "--concurrency",
        "2",
        "--max-streams",
        "7",
        "--endpoint",
        "https://s3.invalid",
        "--region",
        "custom",
        "--small-table-mib",
        "8",
        "--table-format",
        "skiff",
        "--compression-level",
        "1",
        *(("--yt-zstd-passthrough",) if yt_zstd_passthrough else ()),
    )
    assert args.yt_zstd_passthrough is yt_zstd_passthrough
    captured = {}

    def adapter(**kwargs):
        """Discovery keeps the default transport regardless of data worker mode."""
        captured["discovery_options"] = kwargs
        return object()

    monkeypatch.setattr(cli, "YtAdapter", adapter)

    async def supervise(**kwargs):
        """Observe passed settings and simulate a failed node."""
        captured.update(kwargs)
        kwargs["progress"].failed = 1

    monkeypatch.setattr(cli, "supervise", supervise)
    caplog.set_level(logging.INFO, logger=cli.__name__)
    assert asyncio.run(cli.run(arguments=args, options=cli.validate(args))) == 1
    assert captured["processes"] == 3
    assert captured["small_size"] == 8 * 1024**2
    config = captured["config"]
    assert config["concurrency"] == 2
    assert config["max_streams"] == 7
    assert config["table_format"] == "skiff"
    assert config["compression_level"] == 1
    assert config["compression"] == "zstd"
    assert "yt_zstd_passthrough" not in config
    assert "yt_zstd_passthrough" not in captured["discovery_options"]
    assert (
        "table_compression format=skiff mode=yt-zstd-passthrough level=1" in caplog.text
    )
    assert config["bucket"] == "bucket" and config["prefix"] == "prefix"
    assert config["endpoint"] == "https://s3.invalid" and config["region"] == "custom"
    assert not any("secret" in key.lower() or "token" in key.lower() for key in config)
    output = capsys.readouterr().out
    assert "2 node slots / 7 data workers" in output
    assert "failed" in output


@pytest.mark.parametrize(
    "line",
    [
        b"[]\n",
        b"{}\n",
        b'{"type": 1}\n',
        b'{"type":"ready"}',
        b"x" * (MAX_MESSAGE_SIZE + 1),
    ],
)
def test_bad_protocol(line):
    """Reject truncated, untyped and oversized messages."""
    with pytest.raises((ValueError, TypeError)):
        decode_message(line)


def test_protocol_roundtrip():
    """Transport Unicode paths without multiple protocol lines."""
    value = {"type": "job", "path": "//home/пример"}
    assert decode_message(encode_message(value)) == value
