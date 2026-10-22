"""Manual recovery uses the same password-file bytes as ordinary restore."""

import asyncio
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from ytclone.cli import load_encryption

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "recover_openssl.py"
PASSWORD = b"synthetic independent recovery password"


@pytest.fixture(scope="module")
def openssl():
    executable = shutil.which("openssl")
    if executable is None:
        pytest.skip("OpenSSL CLI is not installed")
    result = subprocess.run(
        [executable, "enc", "-help"], capture_output=True, check=False
    )
    if b"-pbkdf2" not in result.stdout + result.stderr:
        pytest.skip("OpenSSL CLI does not support PBKDF2")
    return executable


@pytest.mark.parametrize(
    "password_file_bytes",
    [
        PASSWORD,
        PASSWORD + b"\n",
        PASSWORD + b"\r\n",
        b"p" * 1023 + b"\n",
        b"p" * 1024 + b"\n",
        b"p" * 16384 + b"\r\n",
        b"  " + PASSWORD + b"\t  \r\n",
        b"\xff\x80" + PASSWORD + b"\xfe\n",
    ],
    ids=["no-newline", "LF", "CRLF", "1023", "1024", "16KiB", "spaces", "binary"],
)
def test_manual_openssl_recovery_matches_ytclone(
    openssl, tmp_path, password_file_bytes
):
    password_file = tmp_path / "synthetic password"
    password_file.write_bytes(password_file_bytes)
    password_file.chmod(0o600)
    encryption = load_encryption(enabled=True, password_file=str(password_file))
    plaintext = b"Synthetic recovery payload\x00\xff" * 43
    ciphertext = asyncio.run(encryption.encrypt_bytes(plaintext))
    source = tmp_path / "downloaded-part.zst.enc"
    source.write_bytes(ciphertext)
    target = tmp_path / "part.zst"
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-S",
            str(SCRIPT),
            "--openssl",
            openssl,
            "--password-file",
            str(password_file),
            "--input",
            str(source),
            "--output",
            str(target),
        ],
        # An inherited value must never override the explicitly supplied file.
        env={**os.environ, "YTCLONE_RECOVERY_PASSWORD": "wrong inherited password"},
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert target.read_bytes() == plaintext
    assert not result.stdout
    assert password_file_bytes not in result.stderr


@pytest.mark.parametrize(
    "password_file_bytes",
    [b"short", PASSWORD + b"\x00", PASSWORD + b"\r", PASSWORD + b"\n\n"],
    ids=["short", "NUL", "CR", "extra-newline"],
)
def test_manual_recovery_rejects_invalid_password_before_starting_openssl(
    tmp_path, password_file_bytes
):
    password_file = tmp_path / "synthetic password"
    password_file.write_bytes(password_file_bytes)
    with pytest.raises(ValueError):
        load_encryption(enabled=True, password_file=str(password_file))

    target = tmp_path / "part.zst"
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-S",
            str(SCRIPT),
            "--openssl",
            str(tmp_path / "unavailable-openssl"),
            "--password-file",
            str(password_file),
            "--input",
            str(tmp_path / "downloaded-part.zst.enc"),
            "--output",
            str(target),
        ],
        capture_output=True,
        check=False,
    )
    assert result.returncode == 1
    assert b"Encryption password must" in result.stderr
    assert password_file_bytes not in result.stdout + result.stderr
    assert not target.exists()
