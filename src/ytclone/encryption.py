"""Streaming OpenSSL-compatible encryption and authenticated manifests.

Payloads use the ``openssl enc`` format. Their integrity is checked against
plaintext digests from the authenticated manifest, since CBC itself provides
no authentication. Do not use ``decrypt_bytes`` as an integrity check.
"""

import asyncio
import base64
import hashlib
import hmac
import json
import os
import re
from collections.abc import AsyncIterator

from cryptography.hazmat.primitives import hashes, padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

PROFILE = "openssl-aes-256-cbc-pbkdf2-sha256-v1"
ITERATIONS = 100_000
ENCRYPTED_MANIFEST_FORMAT = "ytclone-encrypted-manifest-v1"
_MAGIC = b"Salted__"
_HEADER_SIZE = 16
_CHUNK_SIZE = 1024 * 1024
_POLICY_SALT = b"ytclone encryption key identity v1"
_MAC_DOMAIN = ENCRYPTED_MANIFEST_FORMAT.encode("ascii") + b"\x00"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class PlaintextDigest:
    """Size and SHA-256 of bytes entering encryption, before CBC padding."""

    __slots__ = ("_hash", "size")

    def __init__(self):
        self.size = 0
        self._hash = hashlib.sha256()

    @property
    def sha256(self) -> str:
        return self._hash.hexdigest()

    def _update(self, data):
        self._hash.update(data)
        self.size += len(data)


def _derive(password: bytes, salt: bytes, length: int = 48) -> bytes:
    return PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=length,
        salt=salt,
        iterations=ITERATIONS,
    ).derive(password)


def _mac_key(material: bytes) -> bytes:
    return HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=b"ytclone manifest authentication v1",
        info=_MAC_DOMAIN,
    ).derive(material)


def _header(data: bytes) -> bytes:
    if len(data) < _HEADER_SIZE or data[:8] != _MAGIC:
        raise ValueError("Invalid encrypted payload header")
    return data[8:_HEADER_SIZE]


async def _offload(function, *args):
    """Finish a bounded native call before letting cancellation release state."""
    task = asyncio.create_task(asyncio.to_thread(function, *args))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        # A native operation cannot be interrupted. Join it so neither cipher
        # state nor the caller's digest is still being changed after we exit.
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
            except Exception:  # noqa: BLE001 - preserve the original cancellation
                break
        if not task.cancelled():
            task.exception()
        raise


async def _close(source, failed: bool):
    close = getattr(source, "aclose", None)
    if close is not None:
        try:
            await close()
        except BaseException:
            if not failed:
                raise


def _slices(data: bytes):
    if not isinstance(data, bytes):
        raise TypeError("Encryption input chunks must be bytes")
    view = memoryview(data)
    for offset in range(0, len(data), _CHUNK_SIZE):
        yield view[offset : offset + _CHUNK_SIZE]


class _Encryptor:
    def __init__(self, password: bytes, digest: PlaintextDigest):
        salt = os.urandom(8)
        self.header = _MAGIC + salt
        self.material = _derive(password, salt)
        self.context = Cipher(
            algorithms.AES(self.material[:32]),
            modes.CBC(self.material[32:]),
        ).encryptor()
        self.padder = padding.PKCS7(128).padder()
        self.digest = digest

    def update(self, data):
        self.digest._update(data)
        return self.context.update(self.padder.update(data))

    def finish(self):
        return self.context.update(self.padder.finalize()) + self.context.finalize()


class _Decryptor:
    def __init__(self, material: bytes, size=None, sha256=None):
        self.context = Cipher(
            algorithms.AES(material[:32]),
            modes.CBC(material[32:]),
        ).decryptor()
        self.unpadder = padding.PKCS7(128).unpadder()
        self.digest = PlaintextDigest()
        self.size = size
        self.sha256 = sha256

    def _record(self, data):
        self.digest._update(data)
        if self.size is not None and self.digest.size > self.size:
            raise ValueError("Decrypted payload size mismatch")
        return data

    def update(self, data):
        return self._record(self.unpadder.update(self.context.update(data)))

    def finish(self):
        try:
            data = self.unpadder.update(self.context.finalize())
            data += self.unpadder.finalize()
        except ValueError as error:
            raise ValueError("Invalid encrypted payload or key") from error
        self._record(data)
        if self.size is not None and self.digest.size != self.size:
            raise ValueError("Decrypted payload size mismatch")
        if self.sha256 is not None and not hmac.compare_digest(
            self.digest.sha256,
            self.sha256,
        ):
            raise ValueError("Decrypted payload SHA-256 mismatch")
        return data


class ArchiveEncryption:
    """One encryption password and the archive policy it identifies.

    Construction performs one 100,000-round PBKDF2 derivation for the stable
    key identity; create this object before starting the event loop. Every
    object has its own random salt and performs another PBKDF2 derivation.
    """

    def __init__(self, password: bytes):
        if not isinstance(password, bytes):
            raise TypeError("Encryption password must be bytes")
        if len(password) < 32:
            raise ValueError("Encryption password must contain at least 32 bytes")
        if any(value in password for value in (b"\x00", b"\n", b"\r")):
            raise ValueError("Encryption password must not contain NUL or newlines")
        self._password = password
        identity = _derive(password, _POLICY_SALT, length=32)
        self._key_id = hashlib.sha256(_POLICY_SALT + b"\x00" + identity).hexdigest()

    def __repr__(self):
        return f"{type(self).__name__}(profile={PROFILE!r})"

    @property
    def policy(self) -> dict:
        return {"format": PROFILE, "iterations": ITERATIONS, "key_id": self._key_id}

    def _encrypt(self, data: bytes):
        context = _Encryptor(self._password, PlaintextDigest())
        result = [context.header]
        result.extend(context.update(chunk) for chunk in _slices(data))
        result.append(context.finish())
        return b"".join(result), context.material

    def _decrypt(self, data: bytes, material=None):
        salt = _header(data)
        if material is None:
            material = _derive(self._password, salt)
        context = _Decryptor(material)
        result = [context.update(chunk) for chunk in _slices(data[_HEADER_SIZE:])]
        result.append(context.finish())
        return b"".join(result)

    async def encrypt_bytes(self, data: bytes) -> bytes:
        """Encrypt a small in-memory value in the standard OpenSSL format."""
        result, _ = await _offload(self._encrypt, data)
        return result

    async def decrypt_bytes(self, data: bytes) -> bytes:
        """Decrypt without authenticating; use a trusted digest to check output."""
        return await _offload(self._decrypt, data)

    def _seal_manifest(self, data: bytes) -> bytes:
        payload, material = self._encrypt(data)
        tag = hmac.new(_mac_key(material), _MAC_DOMAIN + payload, "sha256").hexdigest()
        return json.dumps(
            {
                "format": ENCRYPTED_MANIFEST_FORMAT,
                "payload": base64.b64encode(payload).decode("ascii"),
                "hmac": tag,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")

    @staticmethod
    def _unique_fields(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("Duplicate encrypted manifest field")
            result[key] = value
        return result

    def _open_manifest(self, data: bytes) -> bytes:
        try:
            envelope = json.loads(data, object_pairs_hook=self._unique_fields)
            if (
                not isinstance(envelope, dict)
                or set(envelope) != {"format", "payload", "hmac"}
                or envelope["format"] != ENCRYPTED_MANIFEST_FORMAT
                or not isinstance(envelope["payload"], str)
                or not isinstance(envelope["hmac"], str)
                or _SHA256.fullmatch(envelope["hmac"]) is None
            ):
                raise ValueError
            payload = base64.b64decode(envelope["payload"], validate=True)
            salt = _header(payload)
        except (ValueError, TypeError, UnicodeError) as error:
            raise ValueError("Invalid encrypted manifest envelope") from error
        material = _derive(self._password, salt)
        expected = hmac.new(
            _mac_key(material),
            _MAC_DOMAIN + payload,
            "sha256",
        ).hexdigest()
        if not hmac.compare_digest(expected, envelope["hmac"]):
            raise ValueError("Encrypted manifest authentication failed")
        return self._decrypt(payload, material)

    async def seal_manifest(self, data: bytes) -> bytes:
        """Encrypt then authenticate a manifest, with no cleartext metadata."""
        return await _offload(self._seal_manifest, data)

    async def open_manifest(self, data: bytes) -> bytes:
        """Authenticate the full envelope before decrypting any manifest data."""
        return await _offload(self._open_manifest, data)

    async def encrypt_stream(
        self,
        chunks: AsyncIterator[bytes],
        *,
        digest: PlaintextDigest,
    ) -> AsyncIterator[bytes]:
        """Encrypt bounded chunks and record their plaintext digest off-loop."""
        source = chunks.__aiter__()
        failed = False
        try:
            context = await _offload(_Encryptor, self._password, digest)
            yield context.header
            async for data in source:
                for chunk in _slices(data):
                    encrypted = await _offload(context.update, chunk)
                    if encrypted:
                        yield encrypted
            yield await _offload(context.finish)
        except BaseException:
            failed = True
            raise
        finally:
            await _close(source, failed)

    async def decrypt_stream(
        self,
        chunks: AsyncIterator[bytes],
        *,
        size: int,
        sha256: str,
    ) -> AsyncIterator[bytes]:
        """Decrypt and check the expected plaintext size and digest at EOF.

        Callers must consume the complete stream successfully before committing
        restored data. Earlier yielded chunks are not yet authenticated.
        """
        if type(size) is not int or size < 0:
            raise ValueError("Invalid expected plaintext size")
        if not isinstance(sha256, str) or _SHA256.fullmatch(sha256) is None:
            raise ValueError("Invalid expected plaintext SHA-256")
        source = chunks.__aiter__()
        failed = False
        header = b""
        context = None
        try:
            async for data in source:
                for chunk in _slices(data):
                    if context is None:
                        needed = _HEADER_SIZE - len(header)
                        header += bytes(chunk[:needed])
                        chunk = chunk[needed:]
                        if len(header) < _HEADER_SIZE:
                            continue
                        salt = _header(header)
                        material = await _offload(_derive, self._password, salt)
                        context = await _offload(_Decryptor, material, size, sha256)
                    if chunk:
                        plaintext = await _offload(context.update, chunk)
                        if plaintext:
                            yield plaintext
            if context is None:
                raise ValueError("Truncated encrypted payload header")
            plaintext = await _offload(context.finish)
            if plaintext:
                yield plaintext
        except BaseException:
            failed = True
            raise
        finally:
            await _close(source, failed)
