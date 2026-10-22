import asyncio
import base64
import hashlib
import json
import os
import shutil
import subprocess
import threading

import pytest

from ytclone import encryption
from ytclone.encryption import (
    ENCRYPTED_MANIFEST_FORMAT,
    ITERATIONS,
    PROFILE,
    ArchiveEncryption,
    PlaintextDigest,
)

PASSWORD = b"synthetic test password, not a real archive key"


def run(coro):
    return asyncio.run(asyncio.wait_for(coro, timeout=15))


@pytest.fixture(scope="module")
def codec():
    return ArchiveEncryption(PASSWORD)


async def chunks(data, widths=(1, 7, 16, 31, 4096, 1024 * 1024)):
    yield b""
    offset = 0
    index = 0
    while offset < len(data):
        width = widths[index % len(widths)]
        yield data[offset : offset + width]
        offset += width
        index += 1
    yield b""


async def collect(source):
    return b"".join([chunk async for chunk in source])


@pytest.mark.parametrize(
    "size", [0, 1, 15, 16, 17, 1024 * 1024 - 1, 1024 * 1024, 1024 * 1024 + 17]
)
def test_stream_round_trip_and_plaintext_digest(codec, size):
    data = (b"abc0123456789" * (size // 13 + 1))[:size]

    async def main():
        digest = PlaintextDigest()
        encrypted = await collect(codec.encrypt_stream(chunks(data), digest=digest))
        assert encrypted.startswith(b"Salted__")
        assert len(encrypted) == 16 + (size // 16 + 1) * 16
        assert digest.size == size
        assert digest.sha256 == hashlib.sha256(data).hexdigest()
        assert (
            await collect(
                codec.decrypt_stream(
                    chunks(encrypted),
                    size=size,
                    sha256=digest.sha256,
                )
            )
            == data
        )
        assert await codec.decrypt_bytes(encrypted) == data

    run(main())


def test_policy_is_stable_costly_and_does_not_expose_password(codec, monkeypatch):
    original = encryption._derive
    calls = []

    def derive(password, salt, length=48):
        calls.append((salt, length))
        return original(password, salt, length)

    monkeypatch.setattr(encryption, "_derive", derive)
    duplicate = ArchiveEncryption(PASSWORD)
    assert len(calls) == 1
    assert calls[0][1] == 32
    assert duplicate.policy == codec.policy
    assert duplicate.policy["format"] == PROFILE
    assert duplicate.policy["iterations"] == ITERATIONS == 100_000
    assert len(duplicate.policy["key_id"]) == 64
    assert ArchiveEncryption(PASSWORD + b"different").policy != codec.policy
    policy = codec.policy
    policy["key_id"] = "overwritten"
    assert codec.policy != policy
    assert PASSWORD.decode() not in repr(codec)
    assert PASSWORD.decode() not in json.dumps(codec.policy)


@pytest.mark.parametrize(
    "password", [b"short", b"a" * 32 + b"\x00", b"a" * 32 + b"\n", b"a" * 32 + b"\r"]
)
def test_invalid_password_is_rejected_without_echoing_it(password):
    with pytest.raises(ValueError) as error:
        ArchiveEncryption(password)
    assert repr(password) not in str(error.value)


def test_password_must_be_bytes():
    with pytest.raises(TypeError, match="bytes"):
        ArchiveEncryption("synthetic password as text" * 2)


def test_each_payload_uses_new_salt(codec):
    async def main():
        first = await codec.encrypt_bytes(b"content")
        second = await codec.encrypt_bytes(b"content")
        assert first[8:16] != second[8:16]
        assert await codec.decrypt_bytes(first) == b"content"
        assert await codec.decrypt_bytes(second) == b"content"

    run(main())


def test_manifest_round_trip_and_authentication_before_decryption(codec, monkeypatch):
    async def main():
        data = b'{"private":"schema and paths","key_id":"also private"}'
        sealed = await codec.seal_manifest(data)
        envelope = json.loads(sealed)
        assert set(envelope) == {"format", "payload", "hmac"}
        assert envelope["format"] == ENCRYPTED_MANIFEST_FORMAT
        assert b"schema and paths" not in sealed
        assert b"key_id" not in sealed
        assert await codec.open_manifest(sealed) == data
        assert await codec.decrypt_bytes(base64.b64decode(envelope["payload"])) == data

        def fail_if_decrypted(*args):
            pytest.fail("Unauthenticated ciphertext reached the decryptor")

        monkeypatch.setattr(codec, "_decrypt", fail_if_decrypted)
        payload = bytearray(base64.b64decode(envelope["payload"]))
        payload[-1] ^= 1
        envelope["payload"] = base64.b64encode(payload).decode()
        with pytest.raises(ValueError, match="authentication failed"):
            await codec.open_manifest(json.dumps(envelope).encode())

    run(main())


def test_manifest_rejects_wrong_key(codec):
    wrong = ArchiveEncryption(PASSWORD + b"wrong")

    async def main():
        sealed = await codec.seal_manifest(b"manifest")
        with pytest.raises(ValueError, match="authentication failed"):
            await wrong.open_manifest(sealed)

    run(main())


@pytest.mark.parametrize(
    "malformed",
    [
        b"",
        b"not-json",
        b"[]",
        b"null",
        b"{}",
        b'{"format":1}',
        b'{"format":"x","format":"x","payload":"x","hmac":"x"}',
    ],
)
def test_invalid_manifest_envelopes(codec, malformed):
    async def main():
        with pytest.raises(ValueError, match="manifest envelope"):
            await codec.open_manifest(malformed)

    run(main())


@pytest.mark.parametrize("change", ["format", "hmac", "payload", "extra", "missing"])
def test_manifest_rejects_modified_envelope(codec, change):
    async def main():
        envelope = json.loads(await codec.seal_manifest(b"manifest"))
        if change == "extra":
            envelope["extra"] = "unauthenticated metadata"
        elif change == "missing":
            del envelope["hmac"]
        elif change == "hmac":
            envelope["hmac"] = "0" * 64
        else:
            envelope[change] = "invalid"
        with pytest.raises(ValueError):
            await codec.open_manifest(json.dumps(envelope).encode())

    run(main())


@pytest.mark.parametrize(
    "mutation",
    [
        "empty",
        "header",
        "short_header",
        "no_body",
        "truncated_byte",
        "truncated_block",
        "corrupt",
        "append",
        "size",
        "sha256",
        "wrong_key",
    ],
)
def test_checked_stream_rejects_invalid_payload(codec, mutation):
    data = b"payload with enough blocks to modify away from padding " * 20

    async def main():
        encrypted = await codec.encrypt_bytes(data)
        size = len(data)
        digest = hashlib.sha256(data).hexdigest()
        reader = codec
        if mutation == "empty":
            encrypted = b""
        elif mutation == "header":
            encrypted = b"Invalid!" + encrypted[8:]
        elif mutation == "short_header":
            encrypted = encrypted[:15]
        elif mutation == "no_body":
            encrypted = encrypted[:16]
        elif mutation == "truncated_byte":
            encrypted = encrypted[:-1]
        elif mutation == "truncated_block":
            encrypted = encrypted[:-16]
        elif mutation == "corrupt":
            encrypted = encrypted[:32] + bytes([encrypted[32] ^ 1]) + encrypted[33:]
        elif mutation == "append":
            encrypted += b"\x00" * 16
        elif mutation == "size":
            size += 1
        elif mutation == "sha256":
            digest = "0" * 64
        elif mutation == "wrong_key":
            reader = ArchiveEncryption(PASSWORD + b"wrong")
        with pytest.raises(ValueError):
            await collect(
                reader.decrypt_stream(chunks(encrypted), size=size, sha256=digest)
            )

    run(main())


def test_corruption_can_pass_cbc_padding_but_checked_stream_rejects(codec):
    async def main():
        data = b"synthetic data with many repeated blocks " * 100
        encrypted = await codec.encrypt_bytes(data)
        damaged = encrypted[:32] + bytes([encrypted[32] ^ 1]) + encrypted[33:]
        assert await codec.decrypt_bytes(damaged) != data
        with pytest.raises(ValueError, match="SHA-256 mismatch"):
            await collect(
                codec.decrypt_stream(
                    chunks(damaged),
                    size=len(data),
                    sha256=hashlib.sha256(data).hexdigest(),
                )
            )

    run(main())


@pytest.mark.parametrize("operation", ["encrypt", "decrypt"])
def test_source_errors_are_preserved_and_source_is_closed(codec, operation):
    class RetryError(Exception):
        pass

    class Source:
        closed = False

        def __aiter__(self):
            return self

        async def __anext__(self):
            raise RetryError("retry the download")

        async def aclose(self):
            self.closed = True
            raise RuntimeError("secondary close failure")

    async def main():
        source = Source()
        if operation == "encrypt":
            stream = codec.encrypt_stream(source, digest=PlaintextDigest())
        else:
            stream = codec.decrypt_stream(source, size=0, sha256="0" * 64)
        with pytest.raises(RetryError, match="retry the download"):
            await collect(stream)
        assert source.closed

    run(main())


def test_early_close_closes_source(codec):
    async def main():
        closed = asyncio.Event()

        async def source():
            try:
                yield b"a" * 1024
                yield b"never needed"
            finally:
                closed.set()

        stream = codec.encrypt_stream(source(), digest=PlaintextDigest())
        await anext(stream)  # OpenSSL header.
        await anext(stream)
        await stream.aclose()
        assert closed.is_set()

    run(main())


def test_encryption_hashing_runs_off_loop_and_is_bounded(codec, monkeypatch):
    thread_ids = []
    sizes = []
    original = PlaintextDigest._update

    def update(self, data):
        thread_ids.append(threading.get_ident())
        sizes.append(len(data))
        return original(self, data)

    monkeypatch.setattr(PlaintextDigest, "_update", update)

    async def main():
        loop_thread = threading.get_ident()
        data = b"x" * (3 * 1024 * 1024 + 7)
        await collect(
            codec.encrypt_stream(chunks(data, (len(data),)), digest=PlaintextDigest())
        )
        assert thread_ids and loop_thread not in thread_ids
        assert max(sizes) <= 1024 * 1024

    run(main())


def test_cancellation_joins_native_work_and_closes_source(codec, monkeypatch):
    entered = threading.Event()
    release = threading.Event()
    finished = threading.Event()
    original = encryption._Encryptor.update

    def update(self, data):
        entered.set()
        assert release.wait(timeout=5)
        try:
            return original(self, data)
        finally:
            finished.set()

    monkeypatch.setattr(encryption._Encryptor, "update", update)

    async def main():
        closed = asyncio.Event()

        async def source():
            try:
                yield b"x" * 1024
            finally:
                closed.set()

        stream = codec.encrypt_stream(source(), digest=PlaintextDigest())
        task = asyncio.create_task(collect(stream))
        try:
            assert await asyncio.to_thread(entered.wait, 5)
            # The loop remains responsive while encryption's worker is blocked.
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert finished.is_set()
            assert closed.is_set()
        finally:
            release.set()

    run(main())


def test_openssl_cli_interoperability(codec, tmp_path):
    executable = shutil.which("openssl")
    if executable is None:
        pytest.skip("OpenSSL CLI is not installed")
    help_result = subprocess.run(
        [executable, "enc", "-help"],
        capture_output=True,
        check=False,
    )
    options = help_result.stdout + help_result.stderr
    if b"-pbkdf2" not in options:
        pytest.skip("OpenSSL CLI does not support PBKDF2")
    password_file = tmp_path / "synthetic-password"
    password_file.write_bytes(PASSWORD + b"\n")
    password_file.chmod(0o600)
    command = [
        executable,
        "enc",
        "-aes-256-cbc",
        "-pbkdf2",
        "-md",
        "sha256",
        "-iter",
        str(ITERATIONS),
        "-pass",
        f"file:{password_file}",
    ]
    if b"-saltlen" in options:
        command.extend(["-saltlen", "8"])

    async def main():
        for size in (0, 1, 15, 16, 17, 1024 * 1024 + 13):
            data = os.urandom(size)
            encrypted = await codec.encrypt_bytes(data)
            decrypted = await asyncio.to_thread(
                subprocess.run,
                command + ["-d"],
                input=encrypted,
                capture_output=True,
                check=True,
            )
            assert decrypted.stdout == data
            encrypted_cli = await asyncio.to_thread(
                subprocess.run,
                command,
                input=data,
                capture_output=True,
                check=True,
            )
            assert await codec.decrypt_bytes(encrypted_cli.stdout) == data

    try:
        run(main())
    finally:
        password_file.unlink()
