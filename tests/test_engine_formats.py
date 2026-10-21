"""Format switches must publish a new generation and restore its saved schema."""

import asyncio
from dataclasses import replace

import pytest
from test_engine import (
    DESTINATION,
    SOURCE,
    export,
    make_node,
    manifest_key,
    options,
    restore,
    setup,
)

from ytclone.engine import CopyOptions
from ytclone.format import Manifest, canonical_attributes
from ytclone.table_format import make_table_format


def source_node(name):
    node = replace(
        make_node(kind="table", size=8),
        attributes=canonical_attributes(
            {b"schema": [{b"name": b"value", b"type": b"string"}]}
        ),
    )
    return replace(
        node,
        table_format=make_table_format(name=name, attributes=node.attributes),
    )


def test_format_switch_republishes_unchanged_source_and_restore_uses_manifest():
    async def scenario():
        yt, storage, _ = setup(nodes=[source_node("yson")])
        assert (await export(yt=yt, storage=storage)).completed == 1
        original = Manifest.from_bytes(storage.objects[manifest_key()])

        yt.nodes[SOURCE] = source_node("skiff")
        selected = replace(options(), table_format="skiff")
        result = await export(yt=yt, storage=storage, copy_options=selected)
        assert (result.completed, result.skipped, result.failed) == (1, 0, 0)
        current = Manifest.from_bytes(storage.objects[manifest_key()])
        assert current.fingerprint != original.fingerprint
        assert current.generation != original.generation
        assert current.table_format == yt.nodes[SOURCE].table_format
        assert all(p.object.key.endswith(".skiff.zst") for p in current.parts)

        result = await export(yt=yt, storage=storage, copy_options=selected)
        assert (result.completed, result.skipped, result.failed) == (0, 1, 0)
        # The restore helper uses default YSON options: the saved format wins.
        result = await restore(yt=yt, storage=storage)
        assert (result.completed, result.failed) == (1, 0)
        assert yt.restored[DESTINATION][0].table_format == current.table_format

    asyncio.run(scenario())


def test_failed_format_switch_keeps_previous_manifest_and_parts():
    async def scenario():
        yt, storage, _ = setup(nodes=[source_node("yson")])
        await export(yt=yt, storage=storage)
        original = dict(storage.objects)
        yt.nodes[SOURCE] = source_node("skiff")

        async def fail_upload(**kwargs):
            raise OSError("upload interrupted")

        storage.upload = fail_upload
        result = await export(
            yt=yt,
            storage=storage,
            copy_options=replace(options(), table_format="skiff"),
        )
        assert result.failed == 1
        assert all(storage.objects[key] == data for key, data in original.items())
        result = await restore(yt=yt, storage=storage)
        assert (result.completed, result.failed) == (1, 0)
        assert yt.restored[DESTINATION][0].table_format["name"] == "yson"

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "settings", [{"table_format": "arrow"}, {"compression": "gzip"}]
)
def test_unsupported_archive_settings_fail_before_transfer(settings):
    with pytest.raises(ValueError):
        CopyOptions(**settings)
