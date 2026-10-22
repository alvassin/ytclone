"""Explicit encryption opt-in and private credential handling across CLIs."""

import asyncio
import json
import sys
import threading
from pathlib import Path

import pytest

from ytclone import cli
from ytclone.encryption import ArchiveEncryption
from ytclone.parallel import cli as parallel_cli
from ytclone.parallel import worker

PASSWORD = b"synthetic-password-for-offline-tests-only"


@pytest.fixture(autouse=True)
def isolated_encryption_environment(monkeypatch):
    monkeypatch.delenv("YTCLONE_ENCRYPTION_PASSWORD", raising=False)


def arguments(parser_type, *extra, restore=False):
    parser = parser_type()
    paths = (
        ["s3://bucket/prefix", "//copy"]
        if restore
        else ["//source", "s3://bucket/prefix"]
    )
    return parser.parse_args([*paths, "--yt-proxy", "yt.invalid", *extra])


def test_environment_secret_requires_explicit_opt_in(monkeypatch):
    monkeypatch.setenv("YTCLONE_ENCRYPTION_PASSWORD", "invalid-short-secret")
    assert cli.load_encryption(enabled=False, password_file=None) is None
    with pytest.raises(ValueError):
        cli.load_encryption(enabled=True, password_file=None)


def test_invalid_environment_encoding_does_not_disclose_password(monkeypatch):
    monkeypatch.setenv("YTCLONE_ENCRYPTION_PASSWORD", PASSWORD.decode() + "\udcff")
    with pytest.raises(ValueError) as error:
        cli.load_encryption(enabled=True, password_file=None)
    assert str(error.value) == "Encryption password must be valid UTF-8"


@pytest.mark.parametrize("ending", [b"", b"\n", b"\r\n"])
def test_file_password_preserves_spaces_and_takes_precedence(
    monkeypatch, tmp_path, ending
):
    expected = b" " + PASSWORD + b" "
    path = tmp_path / "password"
    path.write_bytes(expected + ending)
    monkeypatch.setenv("YTCLONE_ENCRYPTION_PASSWORD", "ignored-environment-value")
    captured = {}

    def encryption(*, password):
        captured["password"] = password
        return object()

    monkeypatch.setattr(cli, "ArchiveEncryption", encryption)
    cli.load_encryption(enabled=True, password_file=str(path))
    assert captured["password"] == expected


@pytest.mark.parametrize(
    "payload", [b"short", PASSWORD + b"\n\n", PASSWORD + b"\x00", PASSWORD + b"\r"]
)
def test_invalid_file_password_fails_without_disclosing_contents(tmp_path, payload):
    path = tmp_path / "password"
    path.write_bytes(payload)
    with pytest.raises(ValueError) as error:
        cli.load_encryption(enabled=True, password_file=str(path))
    assert payload.decode("utf-8") not in str(error.value)


def test_missing_password_and_file_without_opt_in_are_rejected(tmp_path):
    path = str(tmp_path / "missing")
    with pytest.raises(ValueError, match="requires --encrypt"):
        cli.load_encryption(enabled=False, password_file=path)
    with pytest.raises(ValueError, match="requires --encryption-password-file"):
        cli.load_encryption(enabled=True, password_file=None)
    with pytest.raises(ValueError, match="Cannot read encryption password file"):
        cli.load_encryption(enabled=True, password_file=path)


@pytest.mark.parametrize("module", [cli, parallel_cli])
def test_secret_is_not_a_parser_default_or_help_text(monkeypatch, module):
    secret = PASSWORD.decode()
    monkeypatch.setenv("YTCLONE_ENCRYPTION_PASSWORD", secret)
    parsed = arguments(module.Arguments, "--encrypt")
    assert secret not in repr(vars(parsed))
    assert secret not in parsed.create_parser().format_help()
    assert "--encryption-password " not in parsed.create_parser().format_help()


@pytest.mark.parametrize("module", [cli, parallel_cli])
def test_invalid_credentials_fail_before_starting_runtime(monkeypatch, capsys, module):
    secret = "short-fake-password"
    monkeypatch.setenv("YTCLONE_ENCRYPTION_PASSWORD", secret)
    monkeypatch.setattr(
        sys,
        "argv",
        ["ytclone", "//source", "s3://bucket", "--yt-proxy", "yt.invalid", "--encrypt"],
    )
    monkeypatch.setattr(
        module.aiomisc,
        "run",
        lambda *a, **k: pytest.fail("Runtime started before credential validation"),
    )
    with pytest.raises(SystemExit) as error:
        module.main()
    assert error.value.code == 2
    assert secret not in capsys.readouterr().err


@pytest.mark.parametrize("restore", [False, True])
def test_single_cli_passes_encryption_to_copy_options(monkeypatch, restore):
    monkeypatch.setenv("YTCLONE_ENCRYPTION_PASSWORD", PASSWORD.decode())
    paths = ["s3://bucket", "//copy"] if restore else ["//source", "s3://bucket"]
    monkeypatch.setattr(
        sys, "argv", ["ytclone", *paths, "--yt-proxy", "yt.invalid", "--encrypt"]
    )

    async def run(*, arguments, options):
        assert isinstance(options.encryption, ArchiveEncryption)
        assert PASSWORD.decode() not in repr(options)
        assert PASSWORD.decode() not in repr(options.encryption)
        return 0

    monkeypatch.setattr(cli, "run", run)
    monkeypatch.setattr(cli.aiomisc, "run", lambda coro, **kw: asyncio.run(coro))
    assert cli.main() == 0


@pytest.mark.parametrize("from_file", [False, True])
def test_parallel_config_has_only_credential_reference(
    monkeypatch, tmp_path, from_file
):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("YTCLONE_ENCRYPTION_PASSWORD", PASSWORD.decode())
    extra = ["--encrypt"]
    if from_file:
        Path("password").write_bytes(PASSWORD + b"\n")
        extra += ["--encryption-password-file", "password"]
    parsed = arguments(parallel_cli.Arguments, *extra)
    options = parallel_cli.validate(parsed)
    assert isinstance(options.encryption, ArchiveEncryption)
    monkeypatch.setattr(parallel_cli, "YtAdapter", lambda **kw: object())

    async def supervise(**kwargs):
        config = kwargs["config"]
        assert config["encrypt"] is True
        assert config["encryption_key_id"] == options.encryption.policy["key_id"]
        assert config["encryption_password_file"] == (
            str(tmp_path / "password") if from_file else None
        )
        assert PASSWORD.decode() not in json.dumps(config)
        assert "encryption" not in config

    monkeypatch.setattr(parallel_cli, "supervise", supervise)
    assert asyncio.run(parallel_cli.run(arguments=parsed, options=options)) == 0


def test_worker_reloads_credentials_before_constructing_clients(monkeypatch):
    monkeypatch.setenv("YTCLONE_ENCRYPTION_PASSWORD", "short-worker-password")
    monkeypatch.setattr(
        worker,
        "YtAdapter",
        lambda **kw: pytest.fail("Clients started before credential validation"),
    )
    config = {
        "concurrency": 1,
        "max_streams": 1,
        "part_size": 256 * 1024**2,
        "chunk_size": 32 * 1024**2,
        "compression_level": 3,
        "encrypt": True,
    }

    async def scenario():
        with pytest.raises(ValueError):
            await worker.run_worker(
                reader=asyncio.StreamReader(), writer=None, config=config
            )

    asyncio.run(scenario())


def test_worker_rejects_password_file_changed_after_coordinator_validation(
    monkeypatch, tmp_path
):
    path = tmp_path / "password"
    path.write_bytes(PASSWORD + b"\n")
    parsed = arguments(
        parallel_cli.Arguments, "--encrypt", "--encryption-password-file", str(path)
    )
    options = parallel_cli.validate(parsed)
    path.write_bytes(b"different-synthetic-password-after-validation\n")
    monkeypatch.setattr(
        worker,
        "YtAdapter",
        lambda **kw: pytest.fail("Clients started with a changed password"),
    )
    config = {
        "encrypt": True,
        "encryption_password_file": str(path),
        "encryption_key_id": options.encryption.policy["key_id"],
    }

    async def scenario():
        with pytest.raises(ValueError, match="differs from coordinator"):
            await worker.run_worker(
                reader=asyncio.StreamReader(), writer=None, config=config
            )

    asyncio.run(scenario())


def test_worker_loads_matching_password_off_loop_before_clients(monkeypatch):
    monkeypatch.setenv("YTCLONE_ENCRYPTION_PASSWORD", PASSWORD.decode())
    encryption = ArchiveEncryption(password=PASSWORD)
    main_thread = threading.get_ident()
    original = worker.load_encryption
    loader_threads = []

    def load(**kwargs):
        loader_threads.append(threading.get_ident())
        return original(**kwargs)

    class ClientReached(Exception):
        pass

    def client(**kwargs):
        assert loader_threads and main_thread not in loader_threads
        raise ClientReached

    monkeypatch.setattr(worker, "load_encryption", load)
    monkeypatch.setattr(worker, "YtAdapter", client)
    config = {
        "concurrency": 1,
        "max_streams": 1,
        "part_size": 256 * 1024**2,
        "chunk_size": 32 * 1024**2,
        "compression_level": 3,
        "yt_proxy": "yt.invalid",
        "proxy_discovery": False,
        "encrypt": True,
        "encryption_key_id": encryption.policy["key_id"],
    }

    async def scenario():
        with pytest.raises(ClientReached):
            await worker.run_worker(
                reader=asyncio.StreamReader(), writer=None, config=config
            )

    asyncio.run(scenario())
