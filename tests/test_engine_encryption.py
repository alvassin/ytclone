"""Encrypted archives preserve streaming, immutable publication and restore checks."""

import asyncio
import base64
import hashlib
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
import zstandard
from test_engine import (
    DESTINATION,
    PREFIX,
    SOURCE,
    MemoryStorage,
    MemoryYt,
    discovered_node,
    export,
    make_node,
    manifest_key,
    options,
)
from test_engine_formats import source_node
from test_restore_retry import make_case
from test_yt import Client, add_node
from test_yt import adapter as adapter  # noqa: PLC0414 - shared fixtures
from test_yt import store as store  # noqa: PLC0414 - shared fixtures
from yt import yson

from ytclone import engine, restore_retry
from ytclone.encryption import ENCRYPTED_MANIFEST_FORMAT, ArchiveEncryption
from ytclone.engine import CopyOptions, export_node, restore_node, restore_tree
from ytclone.export_pool import PartPool
from ytclone.format import Manifest, node_prefix
from ytclone.interfaces import RetryableDownloadError, RetryableReadError


@pytest.fixture(scope="module")
def encryption():
    """Synthetic test password, independent of application credentials."""
    return ArchiveEncryption(b"synthetic archive encryption password")


class EncryptedStorage(MemoryStorage):
    """Model S3 checksum verification over ciphertext rather than extra metadata."""

    async def download(self, *, ref):
        self.downloaded.append(ref.key)
        data = self.objects[ref.key]
        # Split the OpenSSL header and cipher blocks across arbitrary reads.
        for start in range(0, len(data), 7):
            yield data[start : start + 7]
            await asyncio.sleep(0)
        if len(data) != ref.size or hashlib.sha256(data).hexdigest() != ref.sha256:
            raise ValueError("download checksum mismatch")


def memory_archive(*, node=None):
    events = []
    return (
        MemoryYt(nodes=[node or make_node()], events=events),
        EncryptedStorage(events=events),
        events,
    )


async def open_manifest(storage, encryption):
    return Manifest.from_bytes(
        await encryption.open_manifest(storage.objects[manifest_key()])
    )


async def restore(yt, storage, encryption):
    return await restore_tree(
        yt=yt,
        storage=storage,
        source_prefix=node_prefix(prefix=PREFIX, path=SOURCE),
        destination=DESTINATION,
        options=replace(options(), encryption=encryption),
    )


@asynccontextmanager
async def borrow_worker(worker):
    """Use the single-client SDK fake's snapshot for this adapter roundtrip."""
    yield worker


@pytest.mark.parametrize("kind", ["table", "file", "document", "map_node"])
def test_real_adapter_encrypted_roundtrip_preserves_data_and_attributes(
    adapter, store, encryption, kind
):
    attrs = {"note": "sensitive portable metadata"}
    if kind == "table":
        attrs["schema"] = [{"name": "x", "type": "int64"}]
        data = None
    elif kind == "document":
        data = yson.dumps(
            {b"secret": [b"\xff\x00", yson.YsonUint64(2**64 - 1)]},
            yson_format="binary",
        )
    else:
        data = b"" if kind == "map_node" else b"secret file contents\x00\xff" * 5
    source = add_node(store=store, kind=kind, data=data, attributes=attrs)
    original_data = store.nodes[source]["data"]
    storage = EncryptedStorage(events=[])
    selected = CopyOptions(part_size=1024, encryption=encryption)

    async def scenario():
        async with (
            adapter.worker() as worker,
            PartPool(
                yt=SimpleNamespace(worker=lambda: borrow_worker(worker)), concurrency=1
            ) as pool,
        ):
            for expected in [("completed", "copied"), ("skipped", "unchanged")]:
                assert (
                    await export_node(
                        pool=pool,
                        yt=worker,
                        storage=storage,
                        path="//tmp/source",
                        prefix="archive",
                        cluster="test",
                        options=selected,
                    )
                    == expected
                )
            key = "archive/tmp/source/@manifest.json"
            envelope = json.loads(storage.objects[key])
            assert set(envelope) == {"format", "payload", "hmac"}
            assert envelope["format"] == ENCRYPTED_MANIFEST_FORMAT
            assert base64.b64decode(envelope["payload"]).startswith(b"Salted__")
            manifest = Manifest.from_bytes(
                await encryption.open_manifest(storage.objects[key])
            )
            assert manifest.encryption == encryption.policy
            assert manifest.attributes.key.endswith("/@attributes.yson.enc")
            assert all(part.object.key.endswith(".zst.enc") for part in manifest.parts)
            for ref in (manifest.attributes, *(p.object for p in manifest.parts)):
                ciphertext = storage.objects[ref.key]
                plaintext = await encryption.decrypt_bytes(ciphertext)
                assert ciphertext.startswith(b"Salted__")
                assert len(ciphertext) == ref.size
                assert hashlib.sha256(ciphertext).hexdigest() == ref.sha256
                assert len(plaintext) == ref.plaintext_size
                assert hashlib.sha256(plaintext).hexdigest() == ref.plaintext_sha256
                assert b"sensitive portable metadata" not in ciphertext
            assert b"//tmp/source" not in storage.objects[key]
            assert await restore_node(
                yt=worker,
                storage=storage,
                prefix="archive/tmp/source",
                destination="//tmp/restored",
                encryption=encryption,
            ) == ("completed", "copied")
            restored = store.nodes[Client(store).find("//tmp/restored")]
            assert restored["type"] == kind
            assert restored["data"] == original_data
            assert restored["attrs"]["note"] == attrs["note"]
            assert await restore_node(
                yt=worker,
                storage=storage,
                prefix="archive/tmp/source",
                destination="//tmp/restored",
                encryption=encryption,
            ) == ("skipped", "unchanged")

    asyncio.run(scenario())


@pytest.mark.parametrize("kind", ["table", "file", "map_node"])
def test_encrypted_discovery_skip_does_not_reopen_snapshot(encryption, kind):
    async def scenario():
        node = make_node(kind=kind, size=0 if kind == "map_node" else 12)
        yt, storage, events = memory_archive(node=node)
        selected = replace(options(), encryption=encryption)
        assert (
            await export(yt=yt, storage=storage, copy_options=selected)
        ).completed == 1
        old = dict(storage.objects)
        events.clear()
        yt.reads.clear()
        storage.reads.clear()
        result = await export(yt=yt, storage=storage, copy_options=selected)
        assert (result.completed, result.skipped, result.failed) == (0, 1, 0)
        assert events == [] and yt.reads == []
        assert storage.reads == [manifest_key()]
        assert storage.objects == old

    asyncio.run(scenario())


def test_encryption_upgrades_unchanged_plaintext_generation(encryption):
    async def scenario():
        yt, storage, _ = memory_archive()
        assert (await export(yt=yt, storage=storage)).completed == 1
        old = Manifest.from_bytes(storage.objects[manifest_key()])
        selected = replace(options(), encryption=encryption)
        result = await export(yt=yt, storage=storage, copy_options=selected)
        assert (result.completed, result.skipped, result.failed) == (1, 0, 0)
        current = await open_manifest(storage, encryption)
        assert current.generation != old.generation
        assert current.fingerprint != old.fingerprint
        assert current.encryption == encryption.policy
        assert current.source == old.source
        assert not any(
            ref.key in storage.objects
            for ref in [old.attributes, *(p.object for p in old.parts)]
        )
        assert (await restore(yt, storage, encryption)).completed == 1

    asyncio.run(scenario())


@pytest.mark.parametrize("discovery", [False, True])
def test_legacy_encrypted_names_restore_and_skip_without_reupload(
    encryption, discovery
):
    async def scenario():
        yt, storage, events = memory_archive()
        selected = replace(options(), encryption=encryption)
        await export(yt=yt, storage=storage, copy_options=selected)
        current = await open_manifest(storage, encryption)

        def legacy_ref(ref):
            key = ref.key.removesuffix(".enc")
            storage.objects[key] = storage.objects.pop(ref.key)
            return replace(ref, key=key)

        legacy = replace(
            current,
            attributes=legacy_ref(current.attributes),
            parts=tuple(
                replace(part, object=legacy_ref(part.object)) for part in current.parts
            ),
        )
        storage.objects[manifest_key()] = await encryption.seal_manifest(
            legacy.to_bytes()
        )
        before = dict(storage.objects)
        events.clear()
        yt.reads.clear()
        async with PartPool(yt=yt, concurrency=1) as pool:
            assert await export_node(
                pool=pool,
                yt=yt,
                storage=storage,
                path=SOURCE,
                prefix=PREFIX,
                cluster=current.source["cluster"],
                options=selected,
                discovered=discovered_node(yt.nodes[SOURCE]) if discovery else None,
            ) == ("skipped", "unchanged")
        assert storage.objects == before
        assert yt.reads == []
        assert not any(event[0] in {"put", "upload", "delete"} for event in events)
        assert (await restore(yt, storage, encryption)).completed == 1

    asyncio.run(scenario())


@pytest.mark.parametrize("with_child", [False, True])
def test_orphan_encrypted_attributes_are_an_incomplete_archive(encryption, with_child):
    async def scenario():
        yt, storage, _ = memory_archive(node=make_node(path=SOURCE + "/child"))
        if with_child:
            await export(
                yt=yt,
                storage=storage,
                copy_options=replace(options(), encryption=encryption),
            )
        storage.objects[
            "archive/source/@attributes.yson.enc"
        ] = await encryption.encrypt_bytes(b"{}")
        result = await restore(yt, storage, encryption)
        assert result.failed > 0 and result.completed == 0
        assert yt.restored == {}

    asyncio.run(scenario())


@pytest.mark.parametrize("credential", ["missing", "wrong"])
def test_missing_or_wrong_password_cannot_restore_or_replace_archive(
    encryption, credential
):
    supplied = (
        None
        if credential == "missing"
        else ArchiveEncryption(b"another synthetic archive password")
    )

    async def scenario():
        yt, storage, events = memory_archive()
        await export(
            yt=yt,
            storage=storage,
            copy_options=replace(options(), encryption=encryption),
        )
        before = dict(storage.objects)
        yt.nodes[SOURCE] = replace(yt.nodes[SOURCE], content_revision=2)
        events.clear()
        result = await export(
            yt=yt, storage=storage, copy_options=replace(options(), encryption=supplied)
        )
        assert (result.completed, result.failed) == (0, 1)
        assert storage.objects == before
        result = await restore(yt, storage, supplied)
        assert result.failed and not result.completed
        assert not any(
            event[0] in {"put", "upload", "delete", "restore_enter", "ensure_directory"}
            for event in events
        )

    asyncio.run(scenario())


def test_encrypted_restore_refuses_plaintext_manifest_downgrade(encryption):
    async def scenario():
        yt, storage, events = memory_archive()
        await export(yt=yt, storage=storage)
        manifest = Manifest.from_bytes(storage.objects[manifest_key()])
        events.clear()
        result = await restore(yt, storage, encryption)
        assert result.failed and not result.completed
        assert events == []
        with pytest.raises(ValueError, match="encrypted"):
            await restore_node(
                yt=yt,
                storage=storage,
                prefix="archive/source",
                destination=DESTINATION,
                encryption=encryption,
                manifest=manifest,
            )
        assert events == []
        # Legacy archives still restore when encryption was not requested.
        assert (await restore(yt, storage, None)).completed == 1

    asyncio.run(scenario())


def test_corrupt_manifest_authentication_fails_before_destination_writes(encryption):
    async def scenario():
        yt, storage, events = memory_archive()
        await export(
            yt=yt,
            storage=storage,
            copy_options=replace(options(), encryption=encryption),
        )
        envelope = json.loads(storage.objects[manifest_key()])
        envelope["hmac"] = ("0" if envelope["hmac"][0] != "0" else "1") + envelope[
            "hmac"
        ][1:]
        storage.objects[manifest_key()] = json.dumps(envelope).encode()
        events.clear()
        result = await restore(yt, storage, encryption)
        assert result.failed and not result.completed
        assert events == [] and not storage.downloaded

    asyncio.run(scenario())


@pytest.mark.parametrize("damage", ["ciphertext", "truncate", "plaintext_size"])
def test_reauthenticated_corrupt_part_never_commits(encryption, damage):
    async def scenario():
        yt, storage, events = memory_archive(node=make_node(size=128))
        selected = replace(options(), part_size=128, encryption=encryption)
        await export(yt=yt, storage=storage, copy_options=selected)
        manifest = await open_manifest(storage, encryption)
        ref = manifest.parts[0].object
        damaged = bytearray(storage.objects[ref.key])
        if damage == "ciphertext":
            # Mutate an early CBC block, leaving valid final padding intact.
            damaged[16] ^= 1
            assert len(await encryption.decrypt_bytes(bytes(damaged))) == 128
        elif damage == "truncate":
            del damaged[-1:]
        storage.objects[ref.key] = bytes(damaged)
        ref = replace(
            ref, size=len(damaged), sha256=hashlib.sha256(damaged).hexdigest()
        )
        if damage == "plaintext_size":
            ref = replace(ref, plaintext_size=ref.plaintext_size + 1)
        manifest = replace(manifest, parts=(replace(manifest.parts[0], object=ref),))
        storage.objects[manifest_key()] = await encryption.seal_manifest(
            manifest.to_bytes()
        )
        previous = (make_node(path=DESTINATION), [b"previous destination"])
        yt.restored[DESTINATION] = previous
        events.clear()
        with pytest.raises(ValueError, match="SHA-256|size mismatch|Invalid encrypted"):
            await restore_node(
                yt=yt,
                storage=storage,
                prefix="archive/source",
                destination=DESTINATION,
                encryption=encryption,
            )
        assert yt.restored[DESTINATION] == previous
        assert yt.aborted == [DESTINATION]
        assert ("restore_commit", DESTINATION) not in events

    asyncio.run(scenario())


def test_attribute_plaintext_checksum_is_checked_before_parsing_or_restore(encryption):
    async def scenario():
        yt, storage, events = memory_archive()
        await export(
            yt=yt,
            storage=storage,
            copy_options=replace(options(), encryption=encryption),
        )
        manifest = await open_manifest(storage, encryption)
        manifest = replace(
            manifest, attributes=replace(manifest.attributes, plaintext_sha256="0" * 64)
        )
        storage.objects[manifest_key()] = await encryption.seal_manifest(
            manifest.to_bytes()
        )
        events.clear()
        with pytest.raises(ValueError, match="checksum"):
            await restore_node(
                yt=yt,
                storage=storage,
                prefix="archive/source",
                destination=DESTINATION,
                encryption=encryption,
            )
        assert events == [] and not storage.downloaded

    asyncio.run(scenario())


def test_retry_restarts_encryption_for_only_failed_range(encryption, monkeypatch):
    monkeypatch.setattr(engine, "PART_RETRY_PAUSE", 0)

    async def scenario():
        yt, storage, events = memory_archive()
        yt.read_failures[(SOURCE, 1)] = [RetryableReadError("read interrupted")]
        selected = replace(options(), encryption=encryption)
        result = await export(yt=yt, storage=storage, copy_options=selected)
        assert (result.completed, result.failed) == (1, 0)
        assert yt.reads == [(SOURCE, i) for i in (0, 1, 1, 2)]
        assert yt.closed_reads == yt.reads
        assert events.count(("snapshot_enter", SOURCE)) == 1
        manifest = await open_manifest(storage, encryption)
        for part in manifest.parts:
            assert (
                await encryption.decrypt_bytes(storage.objects[part.object.key])
                == b"xxxx"
            )
            assert part.object.plaintext_size == 4
        assert (await restore(yt, storage, encryption)).completed == 1
        assert yt.restored[DESTINATION][1] == [b"xxxx"] * 3

    asyncio.run(scenario())


def test_failed_encrypted_reexport_preserves_previous_generation(
    encryption, monkeypatch
):
    monkeypatch.setattr(engine, "PART_RETRY_PAUSE", 0)

    async def scenario():
        yt, storage, _ = memory_archive()
        selected = replace(options(), encryption=encryption)
        await export(yt=yt, storage=storage, copy_options=selected)
        before = dict(storage.objects)
        yt.nodes[SOURCE] = replace(yt.nodes[SOURCE], content_revision=2)
        yt.read_failures[(SOURCE, 1)] = [
            RetryableReadError("read interrupted") for _ in range(3)
        ]
        result = await export(yt=yt, storage=storage, copy_options=selected)
        assert (result.completed, result.failed) == (0, 1)
        assert all(storage.objects[key] == data for key, data in before.items())
        assert (await restore(yt, storage, encryption)).completed == 1
        yt.read_failures.clear()
        result = await export(yt=yt, storage=storage, copy_options=selected)
        assert (result.completed, result.failed) == (1, 0)
        assert (await open_manifest(storage, encryption)).source[
            "content_revision"
        ] == 2

    asyncio.run(scenario())


def test_already_compressed_skiff_stream_is_encrypted_without_recompression(
    encryption, monkeypatch
):
    wire = zstandard.ZstdCompressor(level=1).compress(b"opaque skiff payload" * 100)
    node = source_node("skiff")
    yt, storage, _ = memory_archive(node=node)

    async def read_part(**kwargs):
        for start in range(0, len(wire), 3):
            yield wire[start : start + 3]

    def forbidden(*args, **kwargs):
        pytest.fail("Encryption must not compress or decompress the YT wire stream")

    yt.read_part = read_part
    monkeypatch.setattr(zstandard, "ZstdCompressor", forbidden)
    monkeypatch.setattr(zstandard, "ZstdDecompressor", forbidden)

    async def scenario():
        selected = replace(
            options(), part_size=1024, table_format="skiff", encryption=encryption
        )
        result = await export(yt=yt, storage=storage, copy_options=selected)
        assert (result.completed, result.failed) == (1, 0)
        manifest = await open_manifest(storage, encryption)
        assert manifest.table_format == node.table_format
        assert len(manifest.parts) == 1
        ref = manifest.parts[0].object
        assert ref.key.endswith(".skiff.zst.enc")
        assert await encryption.decrypt_bytes(storage.objects[ref.key]) == wire
        assert ref.plaintext_size == len(wire)
        assert ref.plaintext_sha256 == hashlib.sha256(wire).hexdigest()
        assert (await restore(yt, storage, encryption)).completed == 1
        restored, parts = yt.restored[DESTINATION]
        assert restored.table_format == node.table_format
        assert parts == [wire]

    asyncio.run(scenario())


@pytest.mark.parametrize("stop", ["last_plaintext_bytes", "close_first_chunk"])
def test_sink_cannot_skip_final_plaintext_verification(encryption, stop):
    async def scenario():
        yt, storage, _ = memory_archive(node=make_node(size=128))
        await export(
            yt=yt,
            storage=storage,
            copy_options=replace(options(), part_size=128, encryption=encryption),
        )
        manifest = await open_manifest(storage, encryption)
        part = manifest.parts[0]
        manifest = replace(
            manifest,
            parts=(
                replace(part, object=replace(part.object, plaintext_sha256="0" * 64)),
            ),
        )
        storage.objects[manifest_key()] = await encryption.seal_manifest(
            manifest.to_bytes()
        )
        committed = False

        @asynccontextmanager
        async def early_sink(**kwargs):
            nonlocal committed

            async def write_part(*, chunks):
                received = 0
                async for chunk in chunks:
                    received += len(chunk)
                    if stop == "close_first_chunk":
                        await chunks.aclose()
                        break
                    if received == 128:
                        break

            yield SimpleNamespace(write_part=write_part)
            committed = True

        yt.restore = early_sink
        with pytest.raises(ValueError, match="did not consume"):
            await restore_node(
                yt=yt,
                storage=storage,
                prefix="archive/source",
                destination=DESTINATION,
                encryption=encryption,
            )
        assert not committed

    asyncio.run(scenario())


@pytest.mark.parametrize("kind", ["table", "file"])
def test_encrypted_download_retry_rolls_back_then_replays_complete_node(
    encryption, monkeypatch, kind
):
    monkeypatch.setattr(restore_retry, "retry_delay", lambda *, attempt: 0)
    case = make_case(kind=kind)
    failed_key = case.manifest.parts[1].object.key

    class FlakyEncryptedStorage(EncryptedStorage):
        failed = False

        async def download(self, *, ref):
            async for chunk in super().download(ref=ref):
                yield chunk
            if ref.key == failed_key and not self.failed:
                self.failed = True
                assert case.store.nodes[case.store.original_id]["data"] == case.old
                raise RetryableDownloadError("connection reset before EOF")

    storage = FlakyEncryptedStorage(events=[])

    async def scenario():
        async def encrypted_ref(ref):
            plaintext = case.storage.objects[ref.key]
            ciphertext = await encryption.encrypt_bytes(plaintext)
            storage.objects[ref.key] = ciphertext
            return replace(
                ref,
                size=len(ciphertext),
                sha256=hashlib.sha256(ciphertext).hexdigest(),
                plaintext_size=len(plaintext),
                plaintext_sha256=hashlib.sha256(plaintext).hexdigest(),
            )

        attributes = await encrypted_ref(case.manifest.attributes)
        parts = tuple(
            [
                replace(part, object=await encrypted_ref(part.object))
                for part in case.manifest.parts
            ]
        )
        manifest = replace(
            case.manifest,
            attributes=attributes,
            parts=parts,
            encryption=encryption.policy,
        )
        storage.objects[
            "archive/source/@manifest.json"
        ] = await encryption.seal_manifest(manifest.to_bytes())
        async with case.adapter.worker() as worker:
            assert await restore_node(
                yt=worker,
                storage=storage,
                prefix="archive/source",
                destination="//tmp/restored",
                encryption=encryption,
                progress=case.progress,
            ) == ("completed", "copied")
        assert case.store.aborts == 1 and case.store.commits == 1
        assert case.store.visible_at_begin == [case.old, case.old]
        assert len(case.store.transactions) == 2
        assert storage.downloaded == [part.object.key for part in parts] * 2
        restored = case.store.nodes[Client(case.store).find("//tmp/restored")]
        assert restored["data"] == b"".join(case.chunks)
        assert case.progress.completed_bytes == sum(part.object.size for part in parts)

    asyncio.run(scenario())
