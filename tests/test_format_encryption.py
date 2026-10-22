"""Encryption metadata cannot weaken checks or alter legacy archive records."""

import json
from dataclasses import replace

import pytest
from test_format import manifest

from ytclone.format import Manifest

POLICY = {
    "format": "openssl-aes-256-cbc-pbkdf2-sha256-v1",
    "iterations": 100000,
    "key_id": "a" * 64,
}


def encrypted_manifest():
    original = manifest()

    def encrypted_ref(ref):
        return replace(ref, plaintext_size=ref.size, plaintext_sha256=ref.sha256)

    return replace(
        original,
        encryption=POLICY,
        attributes=encrypted_ref(original.attributes),
        parts=tuple(
            replace(part, object=encrypted_ref(part.object)) for part in original.parts
        ),
    )


def test_plaintext_manifest_omits_new_fields_and_encrypted_metadata_roundtrips():
    original = manifest()
    record = json.loads(original.to_bytes())
    assert "encryption" not in record
    for ref in [record["attributes"], *(part["object"] for part in record["parts"])]:
        assert "plaintext_size" not in ref and "plaintext_sha256" not in ref
    assert Manifest.from_bytes(original.to_bytes()) == original
    encrypted = encrypted_manifest()
    assert Manifest.from_bytes(encrypted.to_bytes()) == encrypted


@pytest.mark.parametrize("target", ["attributes", "part"])
@pytest.mark.parametrize(
    "changes",
    [
        {"plaintext_size": None},
        {"plaintext_sha256": None},
        {"plaintext_size": -1},
        {"plaintext_size": True},
        {"plaintext_size": 1.5},
        {"plaintext_sha256": "a" * 63},
        {"plaintext_sha256": "A" * 64},
        {"plaintext_sha256": 1},
        {"plaintext_size": None, "plaintext_sha256": None},
    ],
)
def test_encrypted_refs_require_complete_valid_plaintext_integrity(target, changes):
    record = json.loads(encrypted_manifest().to_bytes())
    ref = (
        record["attributes"] if target == "attributes" else record["parts"][0]["object"]
    )
    ref.update(changes)
    with pytest.raises(ValueError):
        Manifest.from_bytes(json.dumps(record).encode())


@pytest.mark.parametrize(
    "changes",
    [
        {"format": "openssl-aes-256-ctr-v1"},
        {"iterations": 1},
        {"iterations": True},
        {"key_id": "short"},
        {"key_id": "A" * 64},
        {"key_id": None},
        {"extra": 1},
    ],
)
def test_manifest_rejects_unknown_or_weakened_encryption_profile(changes):
    record = json.loads(encrypted_manifest().to_bytes())
    record["encryption"].update(changes)
    with pytest.raises(ValueError):
        Manifest.from_bytes(json.dumps(record).encode())


def test_plaintext_manifest_cannot_smuggle_encrypted_object_refs():
    record = json.loads(encrypted_manifest().to_bytes())
    del record["encryption"]
    with pytest.raises(ValueError):
        Manifest.from_bytes(json.dumps(record).encode())
