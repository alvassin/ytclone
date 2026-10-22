"""Encryption metadata cannot weaken checks or alter legacy archive records."""

import json
from dataclasses import replace

import pytest
from test_format import manifest

from ytclone.format import Manifest, validate_manifest_location

PREFIX = "archive/source/table"

POLICY = {
    "format": "openssl-aes-256-cbc-pbkdf2-sha256-v1",
    "iterations": 100000,
    "key_id": "a" * 64,
}


def encrypted_manifest():
    original = manifest()

    def encrypted_ref(ref):
        return replace(
            ref,
            key=ref.key + ".enc",
            plaintext_size=ref.size,
            plaintext_sha256=ref.sha256,
        )

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


def test_encrypted_locations_accept_explicit_names():
    value = encrypted_manifest()
    validate_manifest_location(manifest=value, prefix=PREFIX)


@pytest.mark.parametrize("target", ["attributes", "part", "all"])
def test_encrypted_locations_require_encrypted_names(target):
    value = encrypted_manifest()
    if target in {"attributes", "all"}:
        value = replace(
            value,
            attributes=replace(
                value.attributes, key=value.attributes.key.removesuffix(".enc")
            ),
        )
    if target in {"part", "all"}:
        value = replace(
            value,
            parts=tuple(
                replace(
                    part,
                    object=replace(
                        part.object, key=part.object.key.removesuffix(".enc")
                    ),
                )
                for part in value.parts
            ),
        )
    with pytest.raises(ValueError, match="escapes selected generation"):
        validate_manifest_location(manifest=value, prefix=PREFIX)


@pytest.mark.parametrize("target", ["attributes", "part", "all"])
def test_plaintext_locations_reject_encrypted_names(target):
    value = manifest()
    if target in {"attributes", "all"}:
        value = replace(
            value,
            attributes=replace(value.attributes, key=value.attributes.key + ".enc"),
        )
    if target in {"part", "all"}:
        value = replace(
            value,
            parts=tuple(
                replace(part, object=replace(part.object, key=part.object.key + ".enc"))
                for part in value.parts
            ),
        )
    with pytest.raises(ValueError, match="escapes selected generation"):
        validate_manifest_location(manifest=value, prefix=PREFIX)


@pytest.mark.parametrize("target", ["attributes", "part"])
def test_encrypted_locations_reject_cross_generation_refs(target):
    value = encrypted_manifest()
    ref = value.attributes if target == "attributes" else value.parts[0].object
    changed = replace(ref, key=ref.key.replace(value.generation, "b" * 32))
    if target == "attributes":
        value = replace(value, attributes=changed)
    else:
        value = replace(
            value,
            parts=(replace(value.parts[0], object=changed), *value.parts[1:]),
        )
    with pytest.raises(ValueError, match="escapes selected generation"):
        validate_manifest_location(manifest=value, prefix=PREFIX)


def test_encrypted_locations_reject_reordered_parts():
    value = encrypted_manifest()
    value = replace(value, parts=tuple(reversed(value.parts)))
    with pytest.raises(ValueError, match="invalid ordering"):
        validate_manifest_location(manifest=value, prefix=PREFIX)
