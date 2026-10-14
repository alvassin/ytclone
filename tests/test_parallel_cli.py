"""Public parallel CLI validation and adapter configuration."""

import asyncio

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
    ],
)
def test_invalid_settings(flag, value):
    """Fail unsafe counts before child or network initialization."""
    with pytest.raises(ValueError):
        cli.validate(arguments(flag, value))


def test_reject_restore():
    """Do not apply export ordering to restore directory dependencies."""
    args = cli.Arguments()
    args.parse_args(["s3://bucket/prefix", "//home/test", "--yt-proxy", "yt.invalid"])
    with pytest.raises(ValueError, match="YT to S3"):
        cli.validate(args)


def test_run_passes_nonsecret_config_and_reports_failure(monkeypatch, capsys):
    """Preserve endpoint and worker options without performing live transfers."""
    args = arguments(
        "--processes",
        "3",
        "--concurrency",
        "2",
        "--endpoint",
        "https://s3.invalid",
        "--region",
        "custom",
        "--small-table-mib",
        "8",
    )
    captured = {}
    monkeypatch.setattr(cli, "YtAdapter", lambda **kwargs: object())

    async def supervise(**kwargs):
        """Observe passed settings and simulate a failed node."""
        captured.update(kwargs)
        kwargs["progress"].failed = 1

    monkeypatch.setattr(cli, "supervise", supervise)
    assert asyncio.run(cli.run(arguments=args, options=cli.validate(args))) == 1
    assert captured["processes"] == 3
    assert captured["small_size"] == 8 * 1024**2
    config = captured["config"]
    assert config["concurrency"] == 2
    assert config["bucket"] == "bucket" and config["prefix"] == "prefix"
    assert config["endpoint"] == "https://s3.invalid" and config["region"] == "custom"
    assert not any("secret" in key.lower() or "token" in key.lower() for key in config)
    assert "failed" in capsys.readouterr().out


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
