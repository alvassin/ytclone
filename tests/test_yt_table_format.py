"""Persist the snapshot's wire format across raw export and restore boundaries."""

import copy
import io
from dataclasses import replace

import pytest
from test_yt import Client, add_node, compressed, run
from test_yt import adapter as adapter  # noqa: PLC0414 - re-export shared fixtures
from test_yt import store as store  # noqa: PLC0414 - re-export shared fixtures
from yt import yson

import ytclone.yt as yt_module
from ytclone.format import Range
from ytclone.table_format import ROW_FORMAT, make_table_format


@pytest.mark.parametrize("name", ["yson", "skiff"])
def test_snapshot_format_is_pinned_and_read_without_skiff_bindings(
    adapter, store, monkeypatch, name
):
    """Source schema changes after locking cannot change the saved wire schema."""
    key = add_node(store=store)
    adapter.table_format = name
    requests = []
    original = Client.read_response

    def read_response(self, *, params):
        requests.append(params)
        return original(self, params=params)

    def forbidden_skiff(*args, **kwargs):
        pytest.fail("Raw Skiff transport must not initialize native Skiff bindings")

    monkeypatch.setattr(Client, "read_response", read_response)
    monkeypatch.setattr(yt_module.yt, "SkiffFormat", forbidden_skiff)

    async def scenario():
        async with (
            adapter.worker() as worker,
            worker.snapshot(path="//tmp/source") as snapshot,
        ):
            expected = make_table_format(name, snapshot.node.attributes)
            assert snapshot.node.table_format == expected
            store.nodes[key]["attrs"]["schema"] = [{"name": "new", "type": "string"}]
            # Even later configuration changes cannot change an open snapshot.
            adapter.table_format = "yson" if name == "skiff" else "skiff"
            async for _ in worker.read_part(
                snapshot=snapshot,
                part=Range(index=0, start=0, stop=2),
                compression_level=3,
                chunk_size=4,
            ):
                pass
            actual = requests[0]["output_format"].to_yson_type()
            assert str(actual) == expected["name"]
            assert actual.attributes == expected["attributes"]
            assert requests[0]["transaction_id"] == (
                snapshot.session.transaction.transaction_id
            )
            assert snapshot.node.table_format == expected

    run(scenario())


@pytest.mark.parametrize("saved_format", [None, "yson", "skiff"])
def test_restore_uses_saved_format_and_streams_opaque_payload(
    adapter, store, monkeypatch, saved_format
):
    """The server receives exact restored bytes and the manifest's format spec."""
    key = add_node(store=store)
    source_data = store.nodes[key]["data"]
    writes = []
    original = Client.write_table
    # Bytes need not be parseable by the Python SDK; the server owns row parsing.
    wire_data = (
        b"\x00\x00opaque skiff rows\xff" if saved_format == "skiff" else source_data
    )
    adapter.table_format = "yson" if saved_format == "skiff" else "skiff"

    def write_table(self, *, table, input_stream, format, **kwargs):
        writes.append((b"".join(input_stream), format.to_yson_type(), kwargs))
        # Simulate the server's parser at this boundary; this is not a Skiff
        # roundtrip test. Real formats are covered by the local YT integrations.
        return original(
            self, table=table, input_stream=iter([source_data]), format=format, **kwargs
        )

    monkeypatch.setattr(Client, "write_table", write_table)

    async def scenario():
        async with adapter.worker() as worker:
            async with worker.snapshot(path="//tmp/source") as snapshot:
                spec = (
                    None
                    if saved_format is None
                    else make_table_format(saved_format, snapshot.node.attributes)
                )
                node = replace(snapshot.node, table_format=copy.deepcopy(spec))
            async with worker.restore(
                path="//tmp/restored", node=node, fingerprint="saved-format"
            ) as sink:
                await sink.write_part(chunks=compressed(wire_data))
            assert await worker.matches(
                path="//tmp/restored", fingerprint="saved-format"
            )
            assert node.table_format == spec
            payload, output_format, flags = writes[0]
            assert payload == wire_data
            assert str(output_format) == (saved_format or "yson")
            assert output_format.attributes == (
                yt_module.yt.YsonFormat(attributes=ROW_FORMAT).to_yson_type().attributes
                if spec is None
                else spec["attributes"]
            )
            assert flags["raw"] is True

    run(scenario())
    assert len(writes) == 1


def test_raw_request_uses_the_complete_saved_skiff_schema(monkeypatch):
    """Do not drop fallback columns or optional types at the SDK boundary."""
    spec = make_table_format(
        "skiff",
        yson.dumps(
            {
                "schema": [
                    {"name": "x", "type": "int64"},
                    {
                        "name": "payload",
                        "type_v3": {"type_name": "list", "item": "string"},
                    },
                ]
            }
        ),
    )
    captured = {}

    def make_request(**kwargs):
        captured.update(kwargs)
        return io.BytesIO()

    monkeypatch.setattr(yt_module, "make_request", make_request)
    response = yt_module._read_table(
        client=object(),
        pinned="#pinned",
        part=Range(index=0, start=3, stop=9),
        transaction_id="snapshot",
        table_format=spec,
    )
    response.close()
    actual = captured["params"]["output_format"].to_yson_type()
    assert str(actual) == "skiff"
    assert actual.attributes == spec["attributes"]
    assert actual.attributes["table_skiff_schemas"][0]["children"][-1] == {
        "name": "$other_columns",
        "wire_type": "yson32",
    }
