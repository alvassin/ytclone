"""Restore keeps the generation used to select a node's place in the tree."""

import asyncio

import pytest
from test_engine import (
    DESTINATION,
    SOURCE,
    export,
    make_node,
    manifest_key,
    restore,
    setup,
)

from ytclone.format import Manifest


def run(scenario):
    """Fail a stalled producer or worker without hanging the test run."""

    async def bounded():
        await asyncio.wait_for(scenario(), timeout=5)

    asyncio.run(bounded())


async def switch_after_selection(*, path, initial_kind, child_name, retain):
    """Publish a real replacement just after restore reads the chosen manifest."""
    final_kind = "file" if initial_kind == "map_node" else "map_node"

    def nodes(*, kind, revision):
        result = []
        if path != SOURCE:
            result.append(make_node(kind="map_node", size=0))
        result.append(
            make_node(
                path=path,
                kind=kind,
                size=0 if kind == "map_node" else 12,
                revision=revision,
                attribute_revision=revision,
            )
        )
        if kind == "map_node":
            result.append(
                make_node(
                    path=path + "/" + child_name,
                    size=4,
                    revision=revision,
                    attribute_revision=revision,
                )
            )
        return result

    yt, storage, _ = setup(nodes=nodes(kind="map_node", revision=0))
    # A past directory leaves a legitimate stale child when it becomes a leaf.
    assert (await export(yt=yt, storage=storage)).failed == 0
    yt.nodes = {node.path: node for node in nodes(kind=initial_kind, revision=1)}
    assert (await export(yt=yt, storage=storage)).failed == 0
    previous = dict(storage.objects)
    key = manifest_key(path=path)
    selected = Manifest.from_bytes(previous[key])

    yt.nodes = {node.path: node for node in nodes(kind=final_kind, revision=2)}
    assert (await export(yt=yt, storage=storage)).failed == 0
    replacement = dict(storage.objects)
    if retain:
        # Model delayed cleanup after a confirmed replacement publication.
        for ref in (selected.attributes, *(part.object for part in selected.parts)):
            replacement[ref.key] = previous[ref.key]

    storage.objects = previous
    storage.reads.clear()
    read = storage.read_bytes
    switched = False

    async def read_and_replace(*, key):
        nonlocal switched
        data = await read(key=key)
        if key == manifest_key(path=path) and not switched:
            storage.objects = dict(replacement)
            switched = True
        return data

    storage.read_bytes = read_and_replace
    result = await restore(yt=yt, storage=storage)
    assert switched, "The selected node must be encountered during discovery"
    assert Manifest.from_bytes(storage.objects[key]).kind == final_kind
    return yt, storage, selected, result


@pytest.mark.parametrize("initial_kind", ["map_node", "file"])
@pytest.mark.parametrize("retain", [True, False], ids=["retained", "deleted"])
def test_root_kind_change_does_not_replace_the_selected_generation(
    initial_kind, retain
):
    async def scenario():
        yt, _, selected, result = await switch_after_selection(
            path=SOURCE,
            initial_kind=initial_kind,
            child_name="child",
            retain=retain,
        )

        if not retain:
            assert result.failed > 0
            assert yt.restored == {}
            return

        assert result.failed == 0
        node, parts = yt.restored[DESTINATION]
        assert node.kind == initial_kind
        assert node.content_revision == selected.source["content_revision"]
        if initial_kind == "map_node":
            assert set(yt.restored) == {DESTINATION, DESTINATION + "/child"}
            assert result.completed == 2
        else:
            assert set(yt.restored) == {DESTINATION}
            assert b"".join(parts) == b"x" * selected.data_size
            assert result.completed == 1

    run(scenario)


@pytest.mark.parametrize("initial_kind", ["map_node", "file"])
@pytest.mark.parametrize("retain", [True, False], ids=["retained", "deleted"])
@pytest.mark.parametrize(
    "child_name", ["child", "0child"], ids=["parent-listed-first", "child-listed-first"]
)
def test_descendant_kind_change_keeps_parent_selection_and_stale_child_policy(
    initial_kind, retain, child_name
):
    async def scenario():
        path = SOURCE + "/branch"
        destination = DESTINATION + "/branch"
        yt, storage, selected, result = await switch_after_selection(
            path=path,
            initial_kind=initial_kind,
            child_name=child_name,
            retain=retain,
        )

        assert DESTINATION in yt.restored
        if not retain:
            assert result.failed > 0
            assert destination not in yt.restored
            assert destination + "/" + child_name not in yt.restored
            return

        assert result.failed == 0
        node, parts = yt.restored[destination]
        assert node.kind == initial_kind
        assert node.content_revision == selected.source["content_revision"]
        if initial_kind == "map_node":
            assert set(yt.restored) == {
                DESTINATION,
                destination,
                destination + "/" + child_name,
            }
            assert result.completed == 3
        else:
            # A leaf selected while preparing parents still needs its own restore
            # when its listing entry arrives, but its stale children stay excluded.
            assert set(yt.restored) == {DESTINATION, destination}
            assert b"".join(parts) == b"x" * selected.data_size
            assert result.completed == 2
            assert manifest_key(path=path + "/" + child_name) not in storage.reads

    run(scenario)
