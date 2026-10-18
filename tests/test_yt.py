import asyncio
import contextlib
import copy
import io
import threading
from types import SimpleNamespace

import pytest
import zstandard
from yt import yson

import ytclone.progress as progress_module
import ytclone.yt as yt_module
from ytclone.format import (
    RESTORE_MARKER,
    DiscoveredNode,
    Node,
    Range,
    SourceEstimate,
    canonical_attributes,
)
from ytclone.progress import Progress
from ytclone.yt import YtAdapter


def run(coro):
    return asyncio.run(asyncio.wait_for(coro, timeout=5))


class Store:
    def __init__(self):
        self.nodes = {
            "root": {
                "type": "map_node",
                "parent": None,
                "name": "",
                "attrs": {},
                "data": b"",
                "revision": 1,
            }
        }
        self.next_id = 0
        self.clients = []
        self.commits = 0
        self.aborts = 0
        self.ping_alive = True
        self.incomplete = False
        self.missing_attributes = set()
        self.after_commit = None
        self.chunk_sequence = 0

    def factory(self, *, proxy, config):
        client = Client(self)
        client.config = config
        self.clients.append(client)
        return client


class Transaction:
    def __init__(self, client):
        self.client = client
        self.transaction_id = "snapshot-transaction"

    def __enter__(self):
        self.client.working = copy.deepcopy(self.client.store.nodes)
        self.client.dirty = set()

    def is_pinger_alive(self):
        return self.client.store.ping_alive

    def __exit__(self, error_type, error, traceback):
        client = self.client
        if error_type is None:
            for key in client.dirty:
                if key in client.working:
                    client.working[key]["revision"] += 1
            client.store.nodes = client.working
            client.store.commits += 1
        else:
            client.store.aborts += 1
        client.working = None
        if error_type is None and client.store.after_commit:
            client.store.after_commit(client)


class Client:
    def __init__(self, store):
        self.store = store
        self.working = None
        self.dirty = set()
        self.thread = threading.get_ident()
        self.calls = []

    def record(self, action):
        assert threading.get_ident() == self.thread
        self.calls.append(action)

    @property
    def nodes(self):
        return self.working if self.working is not None else self.store.nodes

    def _cleanup(self):
        self.record("cleanup")

    def Transaction(self, *, timeout, interrupt_on_failed):
        self.record("transaction")
        return Transaction(self)

    def find(self, path):
        path = str(path)
        path = path.split("/@", maxsplit=1)[0]
        follow_links = not path.endswith("&")
        path = path.removesuffix("&")

        if path == "/":
            return "root"

        if path.startswith("#") and "/" not in path:
            key = path[1:]
            if key not in self.nodes:
                raise KeyError(path)
        else:
            parent_path, name = path.rsplit("/", maxsplit=1)
            parent = self.find(parent_path or "/")
            if self.nodes[parent]["type"] != "map_node":
                raise yt_module.yt.YtResponseError(
                    {"message": "Node cannot have children", "code": 500}
                )

            key = next(
                (
                    key
                    for key, record in self.nodes.items()
                    if record["parent"] == parent and record["name"] == name
                ),
                None,
            )
            if key is None:
                raise KeyError(path)

        record = self.nodes[key]
        if (
            follow_links
            and record["type"] == "link"
            and "target_path" in record["attrs"]
        ):
            return self.find(record["attrs"]["target_path"])

        return key

    def attrs(self, key):
        record = self.nodes[key]
        kind = record["type"]
        attrs = dict(record["attrs"])
        attrs.update(
            {
                "id": key,
                "type": kind,
                "content_revision": record["revision"],
                "attribute_revision": record.get("attribute_revision", 1),
                "resource_usage": record.get(
                    "resource_usage", {"disk_space": len(record["data"])}
                ),
                "user_attribute_keys": [
                    k
                    for k in record["attrs"]
                    if k
                    not in (
                        "schema",
                        "compression_codec",
                        "erasure_codec",
                        "optimize_for",
                    )
                ],
            }
        )
        if kind == "table":
            rows = list(yson.loads(string=record["data"], yson_type="list_fragment"))
            attrs.update(
                {
                    "row_count": len(rows),
                    "data_weight": len(record["data"]),
                    "dynamic": record.get("dynamic", False),
                }
            )
        if kind in ("table", "file"):
            attrs.update(
                {
                    "uncompressed_data_size": len(record["data"]),
                    "chunk_ids": record.get("chunks", []),
                }
            )
        for name in self.store.missing_attributes:
            attrs.pop(name, None)
        return attrs

    def get(self, *, path, attributes=None, max_size=None, format=None):
        self.record("get")
        try:
            key = self.find(path)
        except KeyError as error:
            raise yt_module.yt.YtResponseError(
                {"message": "No such node", "code": 500}
            ) from error
        if path.endswith("/@value"):
            return self.nodes[key]["data"]
        attrs = self.attrs(key)
        selected = {name: attrs[name] for name in attributes or () if name in attrs}
        result = (
            selected
            if path.endswith("/@")
            else yson.to_yson_type(None, attributes=selected)
        )
        return yson.dumps(object=result, yson_format="binary")

    def exists(self, *, path):
        self.record("exists")
        if "/@" in path:
            head, name = path.split("/@", maxsplit=1)
            return name in self.nodes[self.find(head)]["attrs"]
        try:
            self.find(path)
            return True
        except KeyError:
            return False

    def lock(self, *, path, mode):
        self.record("lock:" + mode)
        return {"node_id": self.find(path)}

    def create(self, *, type, path, attributes=None, ignore_existing=False):
        self.record("create")
        if ignore_existing and self.exists(path=path):
            key = self.find(path)
            if self.nodes[key]["type"] != type:
                raise ValueError("type mismatch")
            return key
        parent_path, name = path.rsplit("/", maxsplit=1)
        parent = self.find(parent_path or "/")
        self.store.next_id += 1
        key = str(self.store.next_id)
        attrs = yson.loads(string=yson.dumps(object=attributes or {}))
        self.nodes[key] = {
            "type": type,
            "parent": parent,
            "name": name,
            "attrs": attrs,
            "data": b"",
            "revision": 1,
            "chunks": [],
        }
        if self.working is not None:
            self.dirty.add(key)
        return key

    def remove(self, *, path):
        self.record("remove")
        if "/@" in path:
            head, name = path.split("/@", maxsplit=1)
            del self.nodes[self.find(head)]["attrs"][name]
        else:
            del self.nodes[self.find(path)]

    def set(self, *, path, value, format=None):
        self.record("set")
        if "/@" not in path:
            key = self.find(path)
            assert self.nodes[key]["type"] == "document"
            self.nodes[key]["data"] = value
            if self.working is not None:
                self.dirty.add(key)
            else:
                self.nodes[key]["revision"] += 1
            return
        head, name = path.split("/@", maxsplit=1)
        self.nodes[self.find(head)]["attrs"][name] = value

    def list(self, *, path, max_size, format, attributes=None):
        """Return child names with the requested non-following attributes."""
        self.record("list")
        parent = self.find(path)
        children = []
        for key, node in self.nodes.items():
            if node["parent"] != parent:
                continue
            attrs = self.attrs(key)
            children.append(
                yson.to_yson_type(
                    node["name"],
                    attributes={
                        name: attrs[name] for name in attributes or () if name in attrs
                    },
                )
            )
        result = yson.to_yson_type(
            children, attributes={"incomplete": self.store.incomplete}
        )
        return yson.dumps(object=result, yson_format="binary")

    def read_response(self, *, params):
        """Model the raw SDK request without a high-level read_table method."""
        self.record("read_table")
        table = params["path"]
        rows = list(
            yson.loads(
                string=self.nodes[self.find(table)]["data"], yson_type="list_fragment"
            )
        )
        lower, upper = (
            table.attributes["ranges"][0]["lower_limit"]["row_index"],
            table.attributes["ranges"][0]["upper_limit"]["row_index"],
        )
        return io.BytesIO(
            yson.dumps(
                object=rows[lower:upper],
                yson_type="list_fragment",
                yson_format="binary",
            )
        )

    def read_file(self, *, path, offset, length, enable_read_parallel):
        self.record("read_file")
        return io.BytesIO(self.nodes[self.find(path)]["data"][offset : offset + length])

    def write(self, *, path, chunks):
        self.record("write")
        data = b"".join(chunks)
        key = self.find(path)
        record = self.nodes[key]
        record["data"] = (
            record["data"] if path.attributes.get("append") else b""
        ) + data
        self.store.chunk_sequence += 1
        record["chunks"].append(str(self.store.chunk_sequence))
        self.dirty.add(key)

    def write_table(self, *, table, input_stream, **kwargs):
        self.write(path=table, chunks=input_stream)

    def write_file(self, *, destination, stream, **kwargs):
        self.write(path=destination, chunks=stream)


@pytest.fixture
def store():
    store = Store()
    client = Client(store)
    client.create(type="map_node", path="//tmp")
    return store


@pytest.fixture
def adapter(store, monkeypatch):
    """Connect the adapter to a raw request fake bound to its transaction."""

    def make_request(
        *, command_name, params, return_content, use_heavy_proxy, allow_retries, client
    ):
        """Return raw row bytes from the active simulated snapshot."""
        assert command_name == "read_table"
        assert params["transaction_id"] == "snapshot-transaction"
        assert client.working is not None
        assert return_content is False
        assert use_heavy_proxy is True
        assert allow_retries is False
        return client.read_response(params=params)

    monkeypatch.setattr(yt_module, "make_request", make_request)
    return YtAdapter(proxy="test", client_factory=store.factory)


def add_node(*, store, kind="table", name="source", data=None, attributes=None):
    client = Client(store)
    if attributes is None:
        attributes = (
            {"schema": [{"name": "x", "type": "int64"}]} if kind == "table" else {}
        )
    key = client.create(type=kind, path="//tmp/" + name, attributes=attributes)
    if data is None:
        data = (
            yson.dumps(
                object=[{"x": 1}, {"x": 2}],
                yson_type="list_fragment",
                yson_format="binary",
            )
            if kind == "table"
            else b"hello world"
        )
    store.nodes[key]["data"] = data
    return key


async def compressed(data, *, error=None):
    value = zstandard.ZstdCompressor().compress(data)
    for index in range(0, len(value), 7):
        yield value[index : index + 7]
    if error:
        raise error


def test_snapshot_ranges_and_thread_affinity(adapter, store):
    add_node(store=store)
    main_thread = threading.get_ident()

    async def main():
        async with adapter.worker() as worker:
            async with worker.snapshot(path="//tmp/source") as snapshot:
                assert snapshot.node.row_count == 2
                data = b"".join(
                    [
                        value
                        async for value in worker.read_part(
                            snapshot=snapshot,
                            part=Range(index=0, start=1, stop=2),
                            compression_level=3,
                            chunk_size=4,
                        )
                    ]
                )
            with zstandard.ZstdDecompressor().stream_reader(io.BytesIO(data)) as reader:
                assert list(yson.load(stream=reader, yson_type="list_fragment")) == [
                    {"x": 2}
                ]

    run(main())
    assert all(client.thread != main_thread for client in store.clients)
    assert store.commits == 1
    assert store.aborts == 0
    assert all(client.calls[-1] == "cleanup" for client in store.clients)


@pytest.mark.parametrize(
    ("kind", "dynamic", "expected"),
    [
        ("table", False, 1024),
        ("file", False, 1024),
        ("table", True, 1024),
        ("map_node", False, 0),
    ],
)
def test_snapshot_disk_size_is_metadata_only(
    adapter, store, monkeypatch, kind, dynamic, expected
):
    """Fetch own storage weight with existing metadata requests, excluding directories."""
    key = add_node(store=store, kind=kind)
    store.nodes[key]["dynamic"] = dynamic
    store.nodes[key]["resource_usage"] = {"disk_space": 1024}
    original_get = Client.get
    requests = []

    def get(self, *, path, **kwargs):
        """Record selected attributes to detect extra resource requests."""
        requests.append((path, kwargs["attributes"]))
        return original_get(self, path=path, **kwargs)

    monkeypatch.setattr(Client, "get", get)

    async def main():
        """Read valid storage metadata without including it in portable attributes."""
        async with (
            adapter.worker() as worker,
            worker.snapshot(path="//tmp/source") as snapshot,
        ):
            assert snapshot.node.disk_size == expected
            assert b"resource_usage" not in yson.loads(
                string=snapshot.node.attributes, encoding=None
            )

    run(main())
    assert len(requests) == (2 if dynamic else 3)
    assert sum("resource_usage" in attributes for _, attributes in requests) == 1
    assert any(
        "dynamic" in attributes and "resource_usage" in attributes
        for _, attributes in requests
    )


@pytest.mark.parametrize(
    "usage",
    [
        None,
        {},
        {"disk_space": None},
        {"disk_space": -1},
        {"disk_space": True},
        {"disk_space": yson.YsonBoolean(True)},
        {"disk_space": "1024"},
        {"disk_space": 1.5},
        1024,
    ],
)
def test_snapshot_invalid_disk_size_is_optional(adapter, store, usage):
    """Malformed storage metadata never prevents a node snapshot."""
    key = add_node(store=store)
    store.nodes[key]["resource_usage"] = usage

    async def main():
        """Keep the snapshot usable even when its weight is unknown."""
        async with (
            adapter.worker() as worker,
            worker.snapshot(path="//tmp/source") as snapshot,
        ):
            assert snapshot.node.row_count == 2
            assert snapshot.node.disk_size is None

    run(main())


def test_snapshot_missing_disk_size_is_optional(adapter, store):
    """Clusters omitting resource_usage still provide complete data snapshots."""
    add_node(store=store)
    store.missing_attributes.add("resource_usage")

    async def main():
        """Ignore an omitted optional attribute without any fallback request."""
        async with (
            adapter.worker() as worker,
            worker.snapshot(path="//tmp/source") as snapshot,
        ):
            assert snapshot.node.row_count == 2
            assert snapshot.node.disk_size is None

    run(main())


@pytest.mark.parametrize("kind", ["table", "file", "map_node"])
def test_restore_and_matches(adapter, store, kind):
    key = add_node(store=store, kind=kind, data=b"" if kind == "map_node" else None)

    async def main():
        async with adapter.worker() as worker:
            async with worker.snapshot(path="//tmp/source") as snapshot:
                node = snapshot.node
            async with worker.restore(
                path="//tmp/restored", node=node, fingerprint="version"
            ) as sink:
                if kind != "map_node":
                    await sink.write_part(chunks=compressed(store.nodes[key]["data"]))
            assert await worker.matches(path="//tmp/restored", fingerprint="version")
            assert not await worker.matches(
                path="//tmp/restored", fingerprint="different"
            )
            destination = next(
                value for value in store.nodes.values() if value["name"] == "restored"
            )
            assert destination["data"] == store.nodes[key]["data"]
            destination["attrs"]["external"] = 1
            assert not await worker.matches(
                path="//tmp/restored", fingerprint="version"
            )

    run(main())


def test_checksum_failure_aborts_replacement(adapter, store):
    key = add_node(store=store)
    previous = add_node(store=store, name="restored", data=b'{"x"=99;};')

    async def main():
        async with adapter.worker() as worker:
            async with worker.snapshot(path="//tmp/source") as snapshot:
                node = snapshot.node
            with pytest.raises(ValueError, match="checksum"):
                async with worker.restore(
                    path="//tmp/restored", node=node, fingerprint="version"
                ) as sink:
                    await sink.write_part(
                        chunks=compressed(
                            store.nodes[key]["data"],
                            error=ValueError("checksum mismatch"),
                        )
                    )
            assert previous in store.nodes
            assert store.nodes[previous]["data"] == b'{"x"=99;};'
            assert RESTORE_MARKER not in store.nodes[previous]["attrs"]

    run(main())
    assert store.aborts == 1


def test_cancellation_aborts_and_unblocks_thread(adapter, store):
    add_node(store=store)

    async def main():
        async with adapter.worker() as worker:
            ready = asyncio.Event()

            async def unavailable():
                ready.set()
                await asyncio.Event().wait()
                yield b"never"

            async with worker.snapshot(path="//tmp/source") as snapshot:
                node = snapshot.node

            async def restore():
                async with worker.restore(
                    path="//tmp/restored", node=node, fingerprint="v"
                ) as sink:
                    await sink.write_part(chunks=unavailable())

            task = asyncio.create_task(restore())
            await ready.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert not any(
                record["name"] == "restored" for record in store.nodes.values()
            )

    run(main())
    assert store.aborts == 1


def test_wrong_row_count_aborts(adapter, store):
    add_node(store=store)

    async def main():
        async with adapter.worker() as worker:
            async with worker.snapshot(path="//tmp/source") as snapshot:
                node = snapshot.node
            with pytest.raises(ValueError, match="row count mismatch"):
                async with worker.restore(
                    path="//tmp/restored", node=node, fingerprint="v"
                ):
                    pass

    run(main())
    assert store.aborts == 1


@pytest.mark.parametrize("kind", ["table", "file", "map_node"])
@pytest.mark.parametrize("walk_root", ["//tmp", "//tmp/source"])
def test_walk_yields_change_hints_without_extra_requests(
    adapter, store, kind, walk_root
):
    """Root and child descriptors retain revisions and own storage weight."""
    key = add_node(store=store, kind=kind)
    store.nodes[key]["revision"] = yson.YsonUint64(17)
    store.nodes[key]["attribute_revision"] = yson.YsonInt64(23)
    store.nodes[key]["resource_usage"] = {"disk_space": yson.YsonInt64(4096)}

    async def main():
        """Read the selected node as either a listed child or the root."""
        async with adapter.worker() as worker:
            nodes = [node async for node in worker.walk(path=walk_root)]
            selected = next(node for node in nodes if node.path == "//tmp/source")
            assert selected == DiscoveredNode(
                path="//tmp/source",
                kind=kind,
                node_id=key,
                content_revision=17,
                attribute_revision=23,
                dynamic=False,
                disk_size=0 if kind == "map_node" else 4096,
                estimated_size=0
                if kind == "map_node"
                else len(store.nodes[key]["data"]),
            )

    run(main())
    calls = store.clients[0].calls
    assert calls.count("get") == 2
    assert calls.count("list") == (walk_root == "//tmp") + (kind == "map_node")
    assert "transaction" not in calls


@pytest.mark.parametrize("dynamic", [False, True, yson.YsonBoolean(True)])
def test_walk_retains_explicit_table_dynamic_flag(adapter, store, dynamic):
    """Static and dynamic YSON booleans remain distinct discovery hints."""
    key = add_node(store=store)
    store.nodes[key]["dynamic"] = dynamic

    async def main():
        """Read the table hint returned by its parent listing."""
        async with adapter.worker() as worker:
            nodes = [node async for node in worker.walk(path="//tmp")]
            assert nodes[1].dynamic is bool(dynamic)

    run(main())


@pytest.mark.parametrize("walk_root", ["//tmp", "//tmp/source"])
def test_walk_missing_optional_hints_fall_back(adapter, store, walk_root):
    """Older clusters can omit revision, dynamic, or resource metadata safely."""
    key = add_node(store=store)
    store.missing_attributes.update(
        {
            "content_revision",
            "attribute_revision",
            "dynamic",
            "resource_usage",
            "data_weight",
            "uncompressed_data_size",
        }
    )

    async def main():
        """Keep the identity while leaving missing hints unset."""
        async with adapter.worker() as worker:
            nodes = [node async for node in worker.walk(path=walk_root)]
            assert nodes[-1] == DiscoveredNode(
                path="//tmp/source", kind="table", node_id=key
            )

    run(main())


@pytest.mark.parametrize("value", [None, -1, True, "1", 1.5, [], {}])
def test_walk_malformed_numeric_hints_fall_back(adapter, store, value):
    """Malformed optional revisions and storage weights cannot enable a skip."""
    key = add_node(store=store)
    store.nodes[key]["revision"] = value
    store.nodes[key]["attribute_revision"] = value
    store.nodes[key]["resource_usage"] = {"disk_space": value}

    async def main():
        """Reject unsafe hints without rejecting the discovered table."""
        async with adapter.worker() as worker:
            nodes = [node async for node in worker.walk(path="//tmp")]
            assert nodes[-1] == DiscoveredNode(
                path="//tmp/source",
                kind="table",
                node_id=key,
                dynamic=False,
                estimated_size=len(store.nodes[key]["data"]),
            )

    run(main())


@pytest.mark.parametrize("dynamic", [None, 0, 1, "false", [], {}])
def test_walk_malformed_dynamic_flag_is_unknown(adapter, store, dynamic):
    """An absent or ambiguous dynamic flag requires normal snapshot handling."""
    key = add_node(store=store)
    store.nodes[key]["dynamic"] = dynamic

    async def main():
        """Do not coerce integers or strings into a trustworthy static flag."""
        async with adapter.worker() as worker:
            nodes = [node async for node in worker.walk(path="//tmp")]
            assert nodes[-1].dynamic is None

    run(main())


def test_walk_hints_do_not_change_with_later_source_mutations(adapter, store):
    """Discovery hints retain their state while snapshots read the newer state."""
    key = add_node(store=store)

    async def main():
        """Change both revisions after discovery and compare with a fresh snapshot."""
        async with adapter.worker() as worker:
            nodes = [node async for node in worker.walk(path="//tmp")]
            discovered = nodes[-1]
            store.nodes[key]["revision"] = 17
            store.nodes[key]["attribute_revision"] = 23

            async with worker.snapshot(path=discovered.path) as snapshot:
                assert snapshot.node.content_revision == 17
                assert snapshot.node.attribute_revision == 23
            assert discovered.content_revision == 1
            assert discovered.attribute_revision == 1

    run(main())


@pytest.mark.parametrize("revision", [0, 23, yson.YsonUint64(2**63)])
def test_snapshot_reads_attribute_revision_in_existing_request(
    adapter, store, monkeypatch, revision
):
    """Save the snapshot revision without adding a metadata request."""
    key = add_node(store=store)
    store.nodes[key]["attribute_revision"] = revision
    original = Client.get
    requests = []

    def get(self, **kwargs):
        """Record the metadata fields requested within the snapshot."""
        requests.append(kwargs["attributes"])
        return original(self, **kwargs)

    monkeypatch.setattr(Client, "get", get)

    async def main():
        """Read the revision separately from restorable attributes."""
        async with (
            adapter.worker() as worker,
            worker.snapshot(path="//tmp/source") as snapshot,
        ):
            assert snapshot.node.attribute_revision == revision
            assert b"attribute_revision" not in yson.loads(
                string=snapshot.node.attributes, encoding=None
            )

    run(main())
    assert len(requests) == 3
    assert "attribute_revision" in requests[-1]
    assert sum("attribute_revision" in names for names in requests) == 1


@pytest.mark.parametrize("revision", [None, -1, True, "23", 2.5, [], {}])
def test_snapshot_invalid_attribute_revision_is_optional(adapter, store, revision):
    """Invalid revision hints disable the fast path without breaking an archive."""
    key = add_node(store=store)
    store.nodes[key]["attribute_revision"] = revision

    async def main():
        """Read a normal snapshot with no trusted attribute revision."""
        async with (
            adapter.worker() as worker,
            worker.snapshot(path="//tmp/source") as snapshot,
        ):
            assert snapshot.node.attribute_revision is None

    run(main())


def test_snapshot_missing_attribute_revision_is_optional(adapter, store):
    """A cluster without attribute revisions still supports the full copy path."""
    add_node(store=store)
    store.missing_attributes.add("attribute_revision")

    async def main():
        """Read portable metadata when the optional revision is omitted."""
        async with (
            adapter.worker() as worker,
            worker.snapshot(path="//tmp/source") as snapshot,
        ):
            assert snapshot.node.attribute_revision is None

    run(main())


def test_walk_no_follow_and_incomplete_listing(adapter, store):
    add_node(store=store, kind="link", name="link")
    add_node(store=store)

    async def main():
        async with adapter.worker() as worker:
            assert [node.path async for node in worker.walk(path="//tmp")] == [
                "//tmp",
                "//tmp/link",
                "//tmp/source",
            ]
            store.incomplete = True
            async with contextlib.aclosing(worker.walk(path="//tmp")) as paths:
                assert (await paths.__anext__()).path == "//tmp"
                with pytest.raises(ValueError, match="incomplete"):
                    await paths.__anext__()

    run(main())


@pytest.mark.parametrize("leaf_count", [1, 64])
def test_walk_requests_scale_with_directories(adapter, store, monkeypatch, leaf_count):
    """Resolve the root once and list each directory once, regardless of leaves."""
    add_node(store=store, kind="map_node", name="parent")
    root = add_node(store=store, kind="map_node", name="parent/root")
    first = add_node(store=store, kind="map_node", name="parent/root/first")
    second = add_node(store=store, kind="map_node", name="parent/root/second")
    expected = ["//tmp/parent/root"]
    for directory in ("first", "second"):
        expected.append(f"//tmp/parent/root/{directory}")
        for index in range(leaf_count):
            name = f"parent/root/{directory}/table-{index}"
            add_node(store=store, name=name)
            expected.append("//tmp/" + name)
    requests = []
    original_get = Client.get
    original_list = Client.list

    def get(self, **kwargs):
        """Capture actual metadata requests without depending on private helpers."""
        requests.append(("get", kwargs))
        return original_get(self, **kwargs)

    def list_children(self, **kwargs):
        """Capture each directory request and its requested child attributes."""
        requests.append(("list", kwargs))
        return original_list(self, **kwargs)

    monkeypatch.setattr(Client, "get", get)
    monkeypatch.setattr(Client, "list", list_children)

    async def main():
        """Read all paths in depth-first order from the public worker interface."""
        async with adapter.worker() as worker:
            assert [
                node.path async for node in worker.walk(path="//tmp/parent/root")
            ] == expected

    run(main())
    gets = [kwargs for action, kwargs in requests if action == "get"]
    listings = [kwargs for action, kwargs in requests if action == "list"]
    assert len(gets) == 2
    assert [request["path"] for request in listings] == [
        "#" + root + "&",
        "#" + first + "&",
        "#" + second + "&",
    ]
    expected_attributes = {
        "id",
        "type",
        "dynamic",
        "content_revision",
        "attribute_revision",
        "resource_usage",
        "data_weight",
        "uncompressed_data_size",
    }
    assert all(
        set(request["attributes"]) == expected_attributes for request in listings
    )
    assert set(gets[-1]["attributes"]) == expected_attributes


def test_parallel_discovery_uses_separate_sdk_clients(adapter, store, monkeypatch):
    """Concurrent scanners each retain one SDK client on its dedicated thread."""
    from ytclone.parallel.discovery import discover

    directory_ids = {
        add_node(store=store, kind="map_node", name=name) for name in ("a", "b", "c")
    }
    for name in ("a", "b", "c"):
        add_node(store=store, name=name + "/table")

    concurrent_requests = threading.Barrier(3)
    original = Client.list

    def list_children(self, **kwargs):
        """Require all three independent SDK request threads to make progress."""
        if kwargs["path"].removeprefix("#").removesuffix("&") in directory_ids:
            concurrent_requests.wait(timeout=3)
        return original(self, **kwargs)

    monkeypatch.setattr(Client, "list", list_children)
    observed = []

    async def emit(node):
        """Collect scanner results on the event-loop thread."""
        observed.append(node)

    run(discover(yt=adapter, source="//tmp", concurrency=3, emit=emit))

    assert len(observed) == len({node.path for node in observed}) == 7
    assert len(store.clients) == 3
    assert len({client.thread for client in store.clients}) == 3
    assert sum(client.calls.count("get") for client in store.clients) == 2
    assert sum(client.calls.count("list") for client in store.clients) == 4
    assert all(client.calls.count("cleanup") == 1 for client in store.clients)


@pytest.mark.parametrize("kind", ["table", "file", "link"])
def test_walk_leaf_root_never_lists_children(adapter, store, kind):
    """A root table, file, or link is yielded without listing its contents."""
    add_node(store=store, kind=kind)

    async def main():
        """Walk one leaf through a persistent worker."""
        async with adapter.worker() as worker:
            assert [node.path async for node in worker.walk(path="//tmp/source")] == [
                "//tmp/source"
            ]

    run(main())
    assert store.clients[0].calls.count("list") == 0
    assert store.clients[0].calls.count("get") <= 3


@pytest.mark.parametrize("missing", [b"id", b"type"])
def test_walk_rejects_missing_listing_attributes_before_children(
    adapter, store, monkeypatch, missing
):
    """Do not yield a partial child list when an entry has incomplete identity."""
    add_node(store=store, name="valid")
    add_node(store=store, name="invalid")
    original = Client.list

    def list_children(self, **kwargs):
        """Remove one required attribute only from the second listed child."""
        result = yson.loads(string=original(self, **kwargs), encoding=None)
        result[1].attributes.pop(missing)
        return yson.dumps(object=result, yson_format="binary")

    monkeypatch.setattr(Client, "list", list_children)

    async def main():
        """Only the root may be yielded before the malformed listing fails."""
        async with (
            adapter.worker() as worker,
            contextlib.aclosing(worker.walk(path="//tmp")) as paths,
        ):
            assert (await paths.__anext__()).path == "//tmp"
            with pytest.raises(ValueError):
                await paths.__anext__()

    run(main())


def test_walk_rejects_special_child_name_before_children(adapter, store):
    """Reject unsupported child names before yielding any sibling in that list."""
    add_node(store=store, name="valid")
    invalid = add_node(store=store, name="invalid")
    store.nodes[invalid]["name"] = "with/slash"

    async def main():
        """Read the root, then verify that the whole child list is rejected."""
        async with (
            adapter.worker() as worker,
            contextlib.aclosing(worker.walk(path="//tmp")) as paths,
        ):
            assert (await paths.__anext__()).path == "//tmp"
            with pytest.raises(ValueError, match="special names"):
                await paths.__anext__()

    run(main())


@pytest.mark.parametrize("operation", ["walk", "snapshot"])
@pytest.mark.parametrize("source_readable", [True, False])
def test_source_access_is_independent_of_ancestor_read(
    adapter, store, monkeypatch, operation, source_readable
):
    """Read an allowed subtree without requesting access to its parent."""
    client = Client(store)
    parent = client.create(type="map_node", path="//home")
    source = client.create(type="map_node", path="//home/games")
    child = client.create(type="file", path="//home/games/file")
    denied = {parent} if source_readable else {parent, source}
    original_get = Client.get
    requested_ids = []

    def get(self, *, path, **kwargs):
        key = self.find(path)
        requested_ids.append(key)
        if key in denied:
            raise yt_module.yt.YtResponseError(
                {"message": "Access denied", "code": 901}
            )

        return original_get(self, path=path, **kwargs)

    monkeypatch.setattr(Client, "get", get)

    async def main():
        async with adapter.worker() as worker:
            if operation == "walk":
                nodes = [node async for node in worker.walk(path="//home/games")]
                assert [node.node_id for node in nodes] == [source, child]
            else:
                async with worker.snapshot(path="//home/games") as snapshot:
                    assert snapshot.node.node_id == source

    if source_readable:
        run(main())
        assert requested_ids
        assert not denied.intersection(requested_ids)
    else:
        with pytest.raises(yt_module.yt.YtResponseError, match="Access denied"):
            run(main())

        assert requested_ids == [source]
        calls = store.clients[0].calls
        assert "list" not in calls
        assert "lock:snapshot" not in calls


def test_snapshot_keeps_resolved_id_when_path_is_replaced(adapter, store, monkeypatch):
    """Replacing the source path after resolution cannot redirect its snapshot."""
    source = add_node(store=store, kind="file", data=b"original")
    target = add_node(store=store, kind="file", name="target", data=b"other")
    original_get = Client.get
    original_lock = Client.lock
    locked = []
    replaced = False

    def get(self, *, path, **kwargs):
        nonlocal replaced
        response = original_get(self, path=path, **kwargs)

        if not replaced and self.find(path) == source:
            replaced = True
            self.nodes[source]["name"] = "detached"
            self.create(
                type="link",
                path="//tmp/source",
                attributes={"target_path": "//tmp/target"},
            )

        return response

    def lock(self, *, path, **kwargs):
        locked.append(path)
        return original_lock(self, path=path, **kwargs)

    monkeypatch.setattr(Client, "get", get)
    monkeypatch.setattr(Client, "lock", lock)

    async def main():
        async with (
            adapter.worker() as worker,
            worker.snapshot(path="//tmp/source") as snapshot,
        ):
            assert snapshot.node.node_id == source
            assert snapshot.node.node_id != target

    run(main())
    assert replaced
    assert locked == ["#" + source + "&"]


@pytest.mark.parametrize("link_path", ["//alias", "//tmp/alias", "//tmp/folder/alias"])
def test_walk_rejects_link_in_root_ancestor(adapter, store, link_path):
    """The initial path cannot enter a subtree through a link at any depth."""
    add_node(store=store, kind="map_node", name="folder")
    add_node(store=store, name="folder/source")
    Client(store).create(
        type="link",
        path=link_path,
        attributes={"target_path": "//tmp/folder"},
    )

    async def main():
        async with adapter.worker() as worker:
            with pytest.raises(
                yt_module.yt.YtResponseError, match="cannot have children"
            ):
                _ = [node async for node in worker.walk(path=link_path + "/source")]

    run(main())
    calls = store.clients[0].calls
    assert "list" not in calls
    assert "lock:snapshot" not in calls


def test_walk_uses_discovered_directory_id_after_path_replacement(
    adapter, store, monkeypatch
):
    """A replacement link cannot redirect an already discovered directory scan."""
    folder = add_node(store=store, kind="map_node", name="folder")
    add_node(store=store, name="folder/source")
    listed = []
    original = Client.list

    def list_children(self, **kwargs):
        """Record the identity used when listing each directory."""
        listed.append(kwargs["path"])
        return original(self, **kwargs)

    monkeypatch.setattr(Client, "list", list_children)

    async def main():
        """Replace the path between discovery and the child's directory listing."""
        async with adapter.worker() as worker:
            async with contextlib.aclosing(worker.walk(path="//tmp")) as paths:
                assert (await paths.__anext__()).path == "//tmp"
                assert (await paths.__anext__()).path == "//tmp/folder"
                store.nodes[folder]["name"] = "detached"
                add_node(store=store, kind="link", name="folder")
                assert (await paths.__anext__()).path == "//tmp/folder/source"
                with pytest.raises(StopAsyncIteration):
                    await paths.__anext__()
            with pytest.raises(
                yt_module.yt.YtResponseError, match="cannot have children"
            ):
                async with worker.snapshot(path="//tmp/folder/source"):
                    pass

    run(main())
    assert len(listed) == 2
    assert listed[-1] == "#" + folder + "&"


def test_walk_deleted_directory_fails_instead_of_skipping(adapter, store):
    """A vanished discovered ID is a discovery failure rather than an empty tree."""
    folder = add_node(store=store, kind="map_node", name="folder")

    async def main():
        """Delete a directory after it was listed by its parent."""
        async with (
            adapter.worker() as worker,
            contextlib.aclosing(worker.walk(path="//tmp")) as paths,
        ):
            assert (await paths.__anext__()).path == "//tmp"
            assert (await paths.__anext__()).path == "//tmp/folder"
            del store.nodes[folder]
            with pytest.raises(KeyError):
                await paths.__anext__()

    run(main())


def test_closing_walk_does_not_issue_more_requests(adapter, store):
    """Closing after a yielded path never lists further directories."""
    add_node(store=store, kind="map_node", name="folder")

    async def main():
        """Stop discovery immediately after receiving one directory."""
        async with adapter.worker() as worker:
            paths = worker.walk(path="//tmp")
            assert (await paths.__anext__()).path == "//tmp"
            assert (await paths.__anext__()).path == "//tmp/folder"
            before = store.clients[0].calls.copy()
            await paths.aclose()
            assert store.clients[0].calls == before

    run(main())


def test_cancelling_walk_drains_request_without_more_reads(adapter, store, monkeypatch):
    """Cancellation waits for the in-flight SDK call and starts no new requests."""
    add_node(store=store, kind="map_node", name="folder")
    entered = threading.Event()
    release = threading.Event()
    original = Client.list

    def list_children(self, **kwargs):
        """Block one directory request until the test releases cancellation drain."""
        entered.set()
        release.wait(timeout=3)
        return original(self, **kwargs)

    monkeypatch.setattr(Client, "list", list_children)

    async def main():
        """Cancel the next path while directory discovery is running in a thread."""
        async with (
            adapter.worker() as worker,
            contextlib.aclosing(worker.walk(path="//tmp")) as paths,
        ):
            assert (await paths.__anext__()).path == "//tmp"
            task = asyncio.create_task(paths.__anext__())
            assert await asyncio.to_thread(entered.wait, 3)
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert store.clients[0].calls.count("list") == 1
            assert store.clients[0].calls.count("get") <= 2

    run(main())


def test_link_in_ancestor_is_rejected(adapter, store):
    add_node(store=store, kind="link", name="link")

    async def main():
        async with adapter.worker() as worker:
            with pytest.raises(
                yt_module.yt.YtResponseError, match="cannot have children"
            ):
                async with worker.snapshot(path="//tmp/link/secret"):
                    pass

    run(main())


@pytest.mark.parametrize(
    "name", ["schema", "content_revision", "row_count", "data_weight", "custom"]
)
def test_missing_required_metadata_fails(adapter, store, name):
    add_node(store=store, attributes={"schema": [], "custom": 1})
    store.missing_attributes = {name}

    async def main():
        async with adapter.worker() as worker:
            with pytest.raises(ValueError, match="Missing required"):
                async with worker.snapshot(path="//tmp/source"):
                    pass

    run(main())


@pytest.mark.parametrize(
    "name", ["acl", "owner", "account", "expiration_time", RESTORE_MARKER]
)
def test_restore_rejects_nonportable_attributes(adapter, store, name):
    node = Node(
        path="//unused",
        kind="map_node",
        node_id="source",
        content_revision=1,
        attributes=canonical_attributes({name: "bad"}),
    )

    async def main():
        async with adapter.worker() as worker:
            with pytest.raises(ValueError, match="Non-portable"):
                async with worker.restore(path="//tmp/new", node=node, fingerprint="v"):
                    pass
            assert not any(record["name"] == "new" for record in store.nodes.values())

    run(main())


def test_ensure_directory_preserves_attributes_and_children(adapter, store):
    add_node(
        store=store,
        kind="map_node",
        name="existing",
        data=b"",
        attributes={"custom": 1},
    )

    async def main():
        async with adapter.worker() as worker:
            await worker.ensure_directory(path="//tmp/existing/nested")
            existing = next(
                record
                for record in store.nodes.values()
                if record["name"] == "existing"
            )
            assert existing["attrs"] == {"custom": 1}
            assert any(record["name"] == "nested" for record in store.nodes.values())

    run(main())


def test_postcommit_content_change_does_not_receive_marker(adapter, store):
    key = add_node(store=store)

    async def main():
        async with adapter.worker() as worker:
            async with worker.snapshot(path="//tmp/source") as snapshot:
                node = snapshot.node

            def change(client):
                destination = next(
                    record
                    for record in store.nodes.values()
                    if record["name"] == "restored"
                )
                destination["chunks"] = ["outside-mutation"]
                destination["revision"] += 1

            store.after_commit = change
            with pytest.raises(ValueError, match="content changed"):
                async with worker.restore(
                    path="//tmp/restored", node=node, fingerprint="v"
                ) as sink:
                    await sink.write_part(chunks=compressed(store.nodes[key]["data"]))
            destination = next(
                record
                for record in store.nodes.values()
                if record["name"] == "restored"
            )
            assert RESTORE_MARKER not in destination["attrs"]

    run(main())


def test_postcommit_replacement_does_not_receive_marker(adapter, store):
    key = add_node(store=store)

    async def main():
        async with adapter.worker() as worker:
            async with worker.snapshot(path="//tmp/source") as snapshot:
                node = snapshot.node

            def change(client):
                current = client.find("//tmp/restored")
                replacement = copy.deepcopy(store.nodes.pop(current))
                store.nodes["replacement"] = replacement

            store.after_commit = change
            with pytest.raises(ValueError, match="replaced"):
                async with worker.restore(
                    path="//tmp/restored", node=node, fingerprint="v"
                ) as sink:
                    await sink.write_part(chunks=compressed(store.nodes[key]["data"]))
            assert RESTORE_MARKER not in store.nodes["replacement"]["attrs"]

    run(main())


def test_restore_appends_parts_and_replaces_extra_attributes(adapter, store):
    key = add_node(
        store=store, kind="file", data=b"firstsecond", attributes={"custom": "current"}
    )
    add_node(store=store, kind="file", name="restored", attributes={"obsolete": True})

    async def main():
        async with adapter.worker() as worker:
            async with worker.snapshot(path="//tmp/source") as snapshot:
                node = snapshot.node
            async with worker.restore(
                path="//tmp/restored", node=node, fingerprint="v"
            ) as sink:
                await sink.write_part(chunks=compressed(b"first"))
                await sink.write_part(chunks=compressed(b"second"))
            destination = next(
                record
                for record in store.nodes.values()
                if record["name"] == "restored"
            )
            assert destination["data"] == store.nodes[key]["data"]
            assert "obsolete" not in destination["attrs"]
            assert await worker.matches(path="//tmp/restored", fingerprint="v")
            destination["revision"] += 1
            assert not await worker.matches(path="//tmp/restored", fingerprint="v")

    run(main())


def test_directory_restore_keeps_children_and_replaces_own_metadata(adapter, store):
    add_node(store=store, kind="map_node", data=b"", attributes={"custom": "source"})
    destination_id = add_node(
        store=store, kind="map_node", name="restored", data=b"", attributes={"old": 1}
    )
    child_id = Client(store).create(type="file", path="#" + destination_id + "/child")

    async def main():
        async with adapter.worker() as worker:
            async with worker.snapshot(path="//tmp/source") as snapshot:
                node = snapshot.node
            async with worker.restore(
                path="//tmp/restored", node=node, fingerprint="v"
            ):
                pass
            assert store.nodes[destination_id]["attrs"]["custom"] == b"source"
            assert "old" not in store.nodes[destination_id]["attrs"]
            assert child_id in store.nodes
            assert await worker.matches(path="//tmp/restored", fingerprint="v")
            # Child creation changes directory content revision, but its own metadata
            # remains identical and directory traversal is the engine's responsibility.
            store.nodes[destination_id]["revision"] += 1
            assert await worker.matches(path="//tmp/restored", fingerprint="v")

    run(main())


@pytest.mark.parametrize("kind,dynamic", [("link", False), ("table", True)])
def test_restore_refuses_link_and_dynamic_target(adapter, store, kind, dynamic):
    add_node(store=store)
    target = add_node(store=store, kind=kind, name="restored")
    store.nodes[target]["dynamic"] = dynamic

    async def main():
        async with adapter.worker() as worker:
            async with worker.snapshot(path="//tmp/source") as snapshot:
                node = snapshot.node
            with pytest.raises(ValueError, match="link or dynamic"):
                async with worker.restore(
                    path="//tmp/restored", node=node, fingerprint="v"
                ):
                    pass
            assert not await worker.matches(path="//tmp/restored", fingerprint="v")
            assert target in store.nodes

    run(main())


def test_restore_rejects_last_parent_link(adapter, store):
    add_node(store=store)
    add_node(store=store, kind="link", name="link")

    async def main():
        async with adapter.worker() as worker:
            async with worker.snapshot(path="//tmp/source") as snapshot:
                node = snapshot.node
            with pytest.raises(ValueError, match="type mismatch|Ancestor"):
                async with worker.restore(
                    path="//tmp/link/target", node=node, fingerprint="v"
                ):
                    pass

    run(main())


@pytest.mark.parametrize(
    "source_kind,target_kind", [("table", "map_node"), ("map_node", "table")]
)
def test_directory_data_type_collision_is_not_replaced(
    adapter, store, source_kind, target_kind
):
    add_node(store=store, kind=source_kind)
    previous = add_node(store=store, kind=target_kind, name="restored")

    async def main():
        async with adapter.worker() as worker:
            async with worker.snapshot(path="//tmp/source") as snapshot:
                node = snapshot.node
            with pytest.raises(ValueError, match="directory with data"):
                async with worker.restore(
                    path="//tmp/restored", node=node, fingerprint="v"
                ):
                    pass
            assert previous in store.nodes

    run(main())


def test_snapshot_ping_failure_is_not_success(adapter, store):
    add_node(store=store)

    async def main():
        async with adapter.worker() as worker:
            with pytest.raises(RuntimeError, match="ping failed"):
                async with worker.snapshot(path="//tmp/source"):
                    store.ping_alive = False

    run(main())
    assert store.aborts == 1


def test_incomplete_custom_attribute_names_fail(adapter, store, monkeypatch):
    add_node(store=store)
    original = Client.attrs

    def attrs(self, key):
        result = original(self, key)
        result["user_attribute_keys"] = yson.to_yson_type(
            [], attributes={"incomplete": True}
        )
        return result

    monkeypatch.setattr(Client, "attrs", attrs)

    async def main():
        async with adapter.worker() as worker:
            with pytest.raises(ValueError, match="Incomplete user"):
                async with worker.snapshot(path="//tmp/source"):
                    pass

    run(main())


@pytest.mark.parametrize(
    "path",
    [
        "relative",
        "//tmp/link&",
        "//tmp/t[#0:#5]",
        "//tmp/a\\@b",
        "//tmp/a//b",
        "//tmp/../a",
    ],
)
def test_invalid_paths_fail_before_requests(adapter, store, path):
    async def main():
        async with adapter.worker() as worker:
            with pytest.raises(ValueError):
                _ = [value async for value in worker.walk(path=path)]

    run(main())
    assert len(store.clients) == 1
    assert store.clients[0].calls == ["cleanup"]


@pytest.mark.parametrize("kind", ["table", "file"])
def test_empty_data_node_restores_without_parts(adapter, store, kind):
    add_node(store=store, kind=kind, data=b"")

    async def main():
        async with adapter.worker() as worker:
            async with worker.snapshot(path="//tmp/source") as snapshot:
                node = snapshot.node
            async with worker.restore(
                path="//tmp/restored", node=node, fingerprint="empty"
            ):
                pass
            assert await worker.matches(path="//tmp/restored", fingerprint="empty")

    run(main())


def test_wrong_file_size_aborts(adapter, store):
    add_node(store=store, kind="file", data=b"expected bytes")

    async def main():
        async with adapter.worker() as worker:
            async with worker.snapshot(path="//tmp/source") as snapshot:
                node = snapshot.node
            with pytest.raises(ValueError, match="file size mismatch"):
                async with worker.restore(
                    path="//tmp/restored", node=node, fingerprint="v"
                ) as sink:
                    await sink.write_part(chunks=compressed(b"short"))
            assert not any(
                record["name"] == "restored" for record in store.nodes.values()
            )

    run(main())


def test_cancel_during_transaction_begin_still_aborts(adapter, store, monkeypatch):
    started = threading.Event()
    release = threading.Event()
    original = Transaction.__enter__

    def begin(self):
        original(self)
        started.set()
        release.wait(timeout=3)

    monkeypatch.setattr(Transaction, "__enter__", begin)

    async def main():
        async with adapter.worker() as worker:

            async def snapshot():
                async with worker.snapshot(path="//tmp/source"):
                    pytest.fail("Cancelled transaction must not yield a snapshot")

            task = asyncio.create_task(snapshot())
            await asyncio.to_thread(started.wait, 3)
            task.cancel()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await task

    run(main())
    assert store.aborts == 1
    assert all(client.calls[-1] == "cleanup" for client in store.clients)


def test_dynamic_snapshot_does_not_fetch_full_metadata(adapter, store, monkeypatch):
    key = add_node(store=store)
    store.nodes[key]["dynamic"] = True
    original = Client.get
    requested = []

    def get(self, *, path, attributes, max_size, format):
        requested.extend(attributes)
        return original(
            self, path=path, attributes=attributes, max_size=max_size, format=format
        )

    monkeypatch.setattr(Client, "get", get)

    async def main():
        async with (
            adapter.worker() as worker,
            worker.snapshot(path="//tmp/source") as snapshot,
        ):
            assert snapshot.node.dynamic
            assert snapshot.node.attributes == canonical_attributes({})

    run(main())
    assert "schema" not in requested
    assert "chunk_ids" not in requested
    assert "data_weight" not in requested


@pytest.mark.parametrize("fail", [False, True])
def test_prefetch_overlaps_upload_and_propagates_errors(
    adapter, store, monkeypatch, fail
):
    from ytclone.yt import _PartReader, _ReadStream

    add_node(store=store)
    entered = threading.Event()
    release = threading.Event()
    closed = threading.Event()

    class Reader:
        calls = 0

        def read(self, *, size):
            self.calls += 1
            if self.calls == 1:
                return b"first"
            if self.calls == 2:
                entered.set()
                release.wait(timeout=3)
                if fail:
                    raise ValueError("source failed")
                return b"second"
            return b""

        def close(self):
            closed.set()

    reader = Reader()

    def open_part(self, **kwargs):
        self._stream = _ReadStream(raw=io.BytesIO(), compressed=reader)
        return self._stream

    monkeypatch.setattr(_PartReader, "_open_part", open_part)

    async def main():
        async with (
            adapter.worker() as worker,
            worker.snapshot(path="//tmp/source") as snapshot,
        ):
            parts = worker.read_part(
                snapshot=snapshot,
                part=Range(index=0, start=0, stop=2),
                compression_level=3,
                chunk_size=10,
            )
            assert await parts.__anext__() == b"first"
            # No second __anext__ yet: caller is uploading the first S3 chunk.
            assert await asyncio.to_thread(entered.wait, 3)
            assert not closed.is_set()
            release.set()
            if fail:
                with pytest.raises(ValueError, match="source failed"):
                    await parts.__anext__()
            else:
                assert await parts.__anext__() == b"second"
                with pytest.raises(StopAsyncIteration):
                    await parts.__anext__()
            assert closed.is_set()

    run(main())


def test_closing_prefetch_waits_before_closing_source(adapter, store, monkeypatch):
    from ytclone.yt import _PartReader, _ReadStream

    add_node(store=store)
    entered = threading.Event()
    release = threading.Event()
    read_finished = threading.Event()
    closed = threading.Event()

    class Reader:
        calls = 0

        def read(self, *, size):
            self.calls += 1
            if self.calls == 1:
                return b"first"
            entered.set()
            release.wait(timeout=3)
            read_finished.set()
            return b"second"

        def close(self):
            assert read_finished.is_set()
            closed.set()

    reader = Reader()

    def open_part(self, **kwargs):
        self._stream = _ReadStream(raw=io.BytesIO(), compressed=reader)
        return self._stream

    monkeypatch.setattr(_PartReader, "_open_part", open_part)

    async def main():
        async with (
            adapter.worker() as worker,
            worker.snapshot(path="//tmp/source") as snapshot,
        ):
            parts = worker.read_part(
                snapshot=snapshot,
                part=Range(index=0, start=0, stop=2),
                compression_level=3,
                chunk_size=10,
            )
            assert await parts.__anext__() == b"first"
            assert await asyncio.to_thread(entered.wait, 3)
            closer = asyncio.create_task(parts.aclose())
            await asyncio.sleep(0)
            assert not closed.is_set()
            release.set()
            await closer
            assert closed.is_set()

    run(main())


def test_cancelling_initial_read_closes_created_stream(adapter, store, monkeypatch):
    add_node(store=store)
    entered = threading.Event()
    release = threading.Event()
    stream = io.BytesIO(b"unused")

    def read_response(self, **kwargs):
        entered.set()
        release.wait(timeout=3)
        return stream

    monkeypatch.setattr(Client, "read_response", read_response)

    async def main():
        async with (
            adapter.worker() as worker,
            worker.snapshot(path="//tmp/source") as snapshot,
        ):
            parts = worker.read_part(
                snapshot=snapshot,
                part=Range(index=0, start=0, stop=2),
                compression_level=3,
                chunk_size=10,
            )
            task = asyncio.create_task(parts.__anext__())
            assert await asyncio.to_thread(entered.wait, 3)
            task.cancel()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert stream.closed

    run(main())


def test_worker_enables_retries_when_sdk_default_is_disabled(monkeypatch):
    """Preserve external file snapshots even when SDK defaults disable retries."""
    from yt.wrapper import YtClient
    from yt.wrapper.default_config import default_config

    monkeypatch.setitem(dic=default_config["read_retries"], name="enable", value=False)

    def factory(*, proxy, config):
        """Build the installed SDK client without reading credentials."""
        return YtClient(proxy=proxy, config={**config, "enable_token": False})

    adapter = YtAdapter(proxy="test", client_factory=factory)

    async def main():
        """Inspect the configured client without making network requests."""
        async with adapter.worker() as worker:
            retries = worker.session.client.config["read_retries"]
            assert retries["enable"] is True
            assert retries["create_transaction_and_take_snapshot_lock"] is False

    run(main())


def test_worker_reuses_client_and_thread_but_not_transactions(adapter, store):
    """Keep connections across nodes while giving every node its own transaction."""
    first = add_node(store=store)
    second = add_node(store=store, name="second")

    async def main():
        """Mix traversal, snapshots, and directory creation on one worker."""
        async with adapter.worker() as worker:
            assert [node.path async for node in worker.walk(path="//tmp")] == [
                "//tmp",
                "//tmp/source",
                "//tmp/second",
            ]
            async with worker.snapshot(path="//tmp/source") as snapshot:
                assert snapshot.node.node_id == first
            async with worker.snapshot(path="//tmp/second") as snapshot:
                assert snapshot.node.node_id == second
            await worker.ensure_directory(path="//tmp/created")
            assert not await worker.matches(path="//tmp/source", fingerprint="none")
            assert len(store.clients) == 1
            assert store.clients[0].calls.count("transaction") == 3
            assert "cleanup" not in store.clients[0].calls
            assert store.clients[0].working is None

    run(main())
    assert store.commits == 3
    assert store.clients[0].calls.count("cleanup") == 1


def test_worker_reuses_clean_client_after_skip_and_node_failure(adapter, store):
    """A skipped or aborted node must not end the worker lifetime."""
    dynamic = add_node(store=store, name="dynamic")
    store.nodes[dynamic]["dynamic"] = True
    add_node(store=store)

    async def main():
        """Skip, fail, and successfully read using the same connection pool."""
        async with adapter.worker() as worker:
            async with worker.snapshot(path="//tmp/dynamic") as snapshot:
                assert snapshot.node.dynamic
            with pytest.raises(ValueError, match="body failed"):
                async with worker.snapshot(path="//tmp/source"):
                    raise ValueError("body failed")
            async with worker.snapshot(path="//tmp/source") as snapshot:
                assert snapshot.node.row_count == 2
            assert len(store.clients) == 1
            assert store.commits == 2
            assert store.aborts == 1

    run(main())


@pytest.mark.parametrize("phase", ["constructor", "enter"])
def test_worker_replaces_client_after_begin_failure(adapter, store, monkeypatch, phase):
    """Discard possible partial transaction state before the next node."""
    add_node(store=store)
    original_constructor = Client.Transaction
    original_enter = Transaction.__enter__
    failed = False

    def constructor(self, *, timeout, interrupt_on_failed):
        """Simulate a constructor failure after touching client state."""
        nonlocal failed
        if phase == "constructor" and not failed:
            failed = True
            self.working = {"stale": {}}
            raise RuntimeError("begin failed")
        return original_constructor(
            self, timeout=timeout, interrupt_on_failed=interrupt_on_failed
        )

    def enter(self):
        """Fail once after transaction context initialization."""
        nonlocal failed
        original_enter(self)
        if phase == "enter" and not failed:
            failed = True
            raise RuntimeError("begin failed")

    monkeypatch.setattr(Client, "Transaction", constructor)
    monkeypatch.setattr(Transaction, "__enter__", enter)

    async def main():
        """Recover from a partial begin with a new client on the same thread."""
        async with adapter.worker() as worker:
            with pytest.raises(RuntimeError, match="begin failed"):
                async with worker.snapshot(path="//tmp/source"):
                    pytest.fail("Failed begin must not yield")
            async with worker.snapshot(path="//tmp/source") as snapshot:
                assert snapshot.node.row_count == 2
            assert len(store.clients) == 2
            assert store.clients[0].thread == store.clients[1].thread
            assert store.clients[0].calls[-1] == "cleanup"
            assert store.clients[1].working is None

    run(main())


@pytest.mark.parametrize("phase", ["commit", "abort"])
def test_worker_resets_after_transaction_end_failure(
    adapter, store, monkeypatch, phase
):
    """An uncertain end must retire the client before another transaction."""
    add_node(store=store)
    original = Transaction.__exit__
    failed = False

    def exit(self, error_type, error, traceback):
        """Fail once before transaction state cleanup."""
        nonlocal failed
        is_target = (phase == "commit" and error_type is None) or (
            phase == "abort" and error_type is not None
        )
        if is_target and not failed:
            failed = True
            raise RuntimeError("end failed")
        return original(self, error_type, error, traceback)

    monkeypatch.setattr(Transaction, "__exit__", exit)

    async def main():
        """Abort stale work, replace its client, and continue on the worker."""
        async with adapter.worker() as worker:
            with pytest.raises(RuntimeError, match="end failed"):
                async with worker.snapshot(path="//tmp/source"):
                    if phase == "abort":
                        raise ValueError("body failed")
            assert store.clients[0].working is not None
            async with worker.snapshot(path="//tmp/source") as snapshot:
                assert snapshot.node.row_count == 2
            assert len(store.clients) == 2
            assert store.clients[0].thread == store.clients[1].thread
            assert store.clients[0].working is None
            assert store.clients[0].calls[-1] == "cleanup"

    run(main())


def test_worker_never_reuses_client_if_disposal_fails(adapter, store, monkeypatch):
    """A failed disposal must prevent further operations on the old client."""
    add_node(store=store)
    original_constructor = Client.Transaction
    fail_begin = True

    def constructor(self, *, timeout, interrupt_on_failed):
        """Poison the first client's transaction state."""
        nonlocal fail_begin
        if fail_begin:
            fail_begin = False
            raise RuntimeError("begin failed")
        return original_constructor(
            self, timeout=timeout, interrupt_on_failed=interrupt_on_failed
        )

    def cleanup(self):
        """Refuse to dispose the client so recovery cannot proceed."""
        self.record("cleanup failed")
        raise RuntimeError("cleanup failed")

    monkeypatch.setattr(Client, "Transaction", constructor)
    monkeypatch.setattr(Client, "_cleanup", cleanup)

    async def main():
        """Fail safely without opening another transaction after failed disposal."""
        with pytest.raises(RuntimeError, match="cleanup failed"):
            async with adapter.worker() as worker:
                with pytest.raises(RuntimeError, match="begin failed"):
                    async with worker.snapshot(path="//tmp/source"):
                        pass
                with pytest.raises(RuntimeError, match="cleanup failed"):
                    async with worker.snapshot(path="//tmp/source"):
                        pass
                assert len(store.clients) == 1
                assert store.clients[0].calls == ["cleanup failed"]

    run(main())


def test_cancelled_begin_aborts_before_reusing_worker(adapter, store, monkeypatch):
    """Wait for a cancelled begin, abort it, then reuse a clean client."""
    add_node(store=store)
    entered = threading.Event()
    release = threading.Event()
    original = Transaction.__enter__
    first = True

    def enter(self):
        """Block the first begin until cancellation has been requested."""
        nonlocal first
        original(self)
        if first:
            first = False
            entered.set()
            release.wait(timeout=3)

    monkeypatch.setattr(Transaction, "__enter__", enter)

    async def main():
        """Continue on the same worker only after transaction abort completes."""
        async with adapter.worker() as worker:

            async def cancelled():
                """Open the transaction that will be cancelled during begin."""
                async with worker.snapshot(path="//tmp/source"):
                    pytest.fail("Cancelled begin must not yield")

            task = asyncio.create_task(cancelled())
            assert await asyncio.to_thread(entered.wait, 3)
            task.cancel()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert store.aborts == 1
            async with worker.snapshot(path="//tmp/source") as snapshot:
                assert snapshot.node.row_count == 2
            assert len(store.clients) == 1

    run(main())


def test_restore_commit_failure_never_writes_marker(adapter, store, monkeypatch):
    """A failed commit must not mark uncertain data as completed."""
    key = add_node(store=store)
    original = Transaction.__exit__
    fail_commit = False

    def exit(self, error_type, error, traceback):
        """Fail one restore commit before it takes effect."""
        nonlocal fail_commit
        if fail_commit and error_type is None:
            fail_commit = False
            raise RuntimeError("commit failed")
        return original(self, error_type, error, traceback)

    monkeypatch.setattr(Transaction, "__exit__", exit)

    async def main():
        """Recover a worker after an uncertain restore without publishing a marker."""
        nonlocal fail_commit
        async with adapter.worker() as worker:
            async with worker.snapshot(path="//tmp/source") as snapshot:
                node = snapshot.node
            fail_commit = True
            with pytest.raises(RuntimeError, match="commit failed"):
                async with worker.restore(
                    path="//tmp/restored", node=node, fingerprint="v"
                ) as sink:
                    await sink.write_part(chunks=compressed(store.nodes[key]["data"]))
            assert not any(
                RESTORE_MARKER in record["attrs"] for record in store.nodes.values()
            )
            async with worker.snapshot(path="//tmp/source"):
                pass
            assert len(store.clients) == 2
            assert not any(
                record["name"] == "restored" for record in store.nodes.values()
            )

    run(main())


def test_marker_failure_keeps_committed_data_and_clean_worker(
    adapter, store, monkeypatch
):
    """A marker error must not abort or poison an already committed restore."""
    key = add_node(store=store)
    original = Client.set
    fail_marker = True

    def set_attribute(self, *, path, value):
        """Fail one marker write and verify it occurs outside the transaction."""
        nonlocal fail_marker
        if path.endswith("/@" + RESTORE_MARKER):
            assert self.working is None
            if fail_marker:
                fail_marker = False
                raise RuntimeError("marker failed")
        return original(self, path=path, value=value)

    monkeypatch.setattr(Client, "set", set_attribute)

    async def main():
        """Retry a failed marker using the same clean worker connection."""
        async with adapter.worker() as worker:
            async with worker.snapshot(path="//tmp/source") as snapshot:
                node = snapshot.node
            with pytest.raises(RuntimeError, match="marker failed"):
                async with worker.restore(
                    path="//tmp/restored", node=node, fingerprint="v"
                ) as sink:
                    await sink.write_part(chunks=compressed(store.nodes[key]["data"]))
            assert store.aborts == 0
            assert not await worker.matches(path="//tmp/restored", fingerprint="v")
            async with worker.restore(
                path="//tmp/restored", node=node, fingerprint="v"
            ) as sink:
                await sink.write_part(chunks=compressed(store.nodes[key]["data"]))
            assert await worker.matches(path="//tmp/restored", fingerprint="v")
            assert len(store.clients) == 1

    run(main())


def test_cancel_during_commit_waits_and_never_marks(adapter, store, monkeypatch):
    """Cancellation waits for commit and leaves the persistent client clean."""
    key = add_node(store=store)
    entered = threading.Event()
    release = threading.Event()
    original = Transaction.__exit__
    block_commit = False

    def exit(self, error_type, error, traceback):
        """Delay one restore commit without changing its eventual outcome."""
        nonlocal block_commit
        if block_commit and error_type is None:
            block_commit = False
            entered.set()
            release.wait(timeout=3)
        return original(self, error_type, error, traceback)

    monkeypatch.setattr(Transaction, "__exit__", exit)

    async def main():
        """Cancel the waiter, then reuse the client after commit has finished."""
        nonlocal block_commit
        async with adapter.worker() as worker:
            async with worker.snapshot(path="//tmp/source") as snapshot:
                node = snapshot.node

            async def restore():
                """Write the node whose commit will overlap cancellation."""
                async with worker.restore(
                    path="//tmp/restored", node=node, fingerprint="v"
                ) as sink:
                    await sink.write_part(chunks=compressed(store.nodes[key]["data"]))

            block_commit = True
            task = asyncio.create_task(restore())
            assert await asyncio.to_thread(entered.wait, 3)
            task.cancel()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert store.clients[0].working is None
            restored = next(
                record
                for record in store.nodes.values()
                if record["name"] == "restored"
            )
            assert restored["data"] == store.nodes[key]["data"]
            assert RESTORE_MARKER not in restored["attrs"]
            async with worker.snapshot(path="//tmp/source"):
                pass
            assert len(store.clients) == 1
            assert store.aborts == 0

    run(main())


def test_worker_acquisition_failure_closes_executor(store):
    """A factory error must not leave an acquired worker thread alive."""
    worker_thread = None

    def factory(*, proxy, config):
        """Fail while acquiring the client on its dedicated thread."""
        nonlocal worker_thread
        worker_thread = threading.current_thread()
        raise RuntimeError("client unavailable")

    adapter = YtAdapter(proxy="test", client_factory=factory)

    async def main():
        """Observe acquisition failure after its resources have been closed."""
        with pytest.raises(RuntimeError, match="client unavailable"):
            async with adapter.worker():
                pytest.fail("Unavailable worker must not yield")

    run(main())
    assert worker_thread is not None
    assert not worker_thread.is_alive()


class Measurements:
    def __init__(self):
        """Record measurement scope and event-loop thread affinity."""
        self.events = []
        self.thread = threading.get_ident()

    @contextlib.contextmanager
    def measure(self, *, operation):
        """Record logical calls and their outcomes on the event loop."""
        assert threading.get_ident() == self.thread
        event = {"operation": operation, "finished": False, "error": None}
        self.events.append(event)
        try:
            yield
        except BaseException as error:
            event["error"] = type(error)
            raise
        finally:
            assert threading.get_ident() == self.thread
            event["finished"] = True


def test_progress_classifies_metadata_read_and_write(adapter, store):
    """Classify real adapter paths while excluding local housekeeping calls."""
    add_node(store=store)
    progress = Measurements()
    adapter.progress = progress

    async def main():
        """Read and restore one node through measured worker operations."""
        async with adapter.worker() as worker:
            assert progress.events == []
            async with worker.snapshot(path="//tmp/source") as snapshot:
                node = snapshot.node
                data = [
                    chunk
                    async for chunk in worker.read_part(
                        snapshot=snapshot,
                        part=Range(index=0, start=0, stop=2),
                        compression_level=3,
                        chunk_size=4,
                    )
                ]

            async def chunks():
                """Supply the compressed source bytes to restore."""
                for chunk in data:
                    yield chunk

            async with worker.restore(
                path="//tmp/restored", node=node, fingerprint="version"
            ) as sink:
                await sink.write_part(chunks=chunks())
            before_close = len(progress.events)
        assert len(progress.events) == before_close

    run(main())
    operations = [event["operation"] for event in progress.events]
    assert operations.count("yt_metadata") == 8
    assert operations.count("yt_read") > 1
    assert operations.count("yt_write") == 1
    assert all(
        event["finished"] and event["error"] is None for event in progress.events
    )


@pytest.mark.parametrize("operation", ["yt_metadata", "yt_read", "yt_write"])
def test_progress_records_operation_errors(adapter, operation):
    """A failed operation closes its measurement and keeps the exception."""
    progress = Measurements()
    adapter.progress = progress

    def fail():
        """Raise an SDK-style failure on the worker thread."""
        raise OSError("request failed")

    async def main():
        """Observe the failure after its measured scope has ended."""
        async with adapter.worker() as worker:
            with pytest.raises(OSError, match="request failed"):
                await worker.session.call(fail, operation=operation)

    run(main())
    assert progress.events == [
        {"operation": operation, "finished": True, "error": OSError}
    ]


def test_progress_cancellation_includes_safe_worker_drain(adapter):
    """Keep cancelled work measured until the SDK call releases its worker."""
    progress = Measurements()
    adapter.progress = progress
    entered = threading.Event()
    release = threading.Event()

    def wait():
        """Keep a synchronous request alive after its waiter is cancelled."""
        entered.set()
        release.wait(timeout=3)

    async def main():
        """Cancel an in-flight read and release it before closing the worker."""
        async with adapter.worker() as worker:
            task = asyncio.create_task(worker.session.call(wait, operation="yt_read"))
            assert await asyncio.to_thread(entered.wait, 3)
            try:
                task.cancel()
                await asyncio.sleep(0)
                assert not task.done()
                assert not progress.events[0]["finished"]
            finally:
                release.set()
            with pytest.raises(asyncio.CancelledError):
                await task

    run(main())
    assert progress.events == [
        {"operation": "yt_read", "finished": True, "error": asyncio.CancelledError}
    ]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (
            {"node_count": 7, "disk_space": 1024},
            SourceEstimate(node_count=7, disk_size=1024),
        ),
        ({"node_count": 7}, SourceEstimate(node_count=7)),
        ({"disk_space": 0}, SourceEstimate(disk_size=0)),
        ({"node_count": 0, "disk_space": 1024}, SourceEstimate(disk_size=1024)),
        ({"node_count": 7, "disk_space": -1}, SourceEstimate(node_count=7)),
        ({}, SourceEstimate()),
        (None, SourceEstimate()),
        (7, SourceEstimate()),
        ([], SourceEstimate()),
        ("invalid", SourceEstimate()),
        (
            {"node_count": yson.YsonInt64(7), "disk_space": yson.YsonUint64(1024)},
            SourceEstimate(node_count=7, disk_size=1024),
        ),
    ]
    + [
        (
            {"node_count": 7, "disk_space": 1024, key: invalid},
            SourceEstimate(
                node_count=None if key == "node_count" else 7,
                disk_size=None if key == "disk_space" else 1024,
            ),
        )
        for key in ("node_count", "disk_space")
        for invalid in (True, yson.YsonBoolean(True), 1.0, "7", None, -1)
    ],
)
def test_estimate_source_validates_fields_and_restores_config(
    adapter, store, monkeypatch, value, expected
):
    """Validate optional fields independently without scanning nodes or rows."""
    original_get = Client.get
    requests = []

    def get(self, *, path, **kwargs):
        """Check isolated request limits and return optional resource totals."""
        assert threading.get_ident() == self.thread
        assert self.config["proxy"]["request_timeout"] == 5_000
        assert self.config["proxy"]["connect_timeout"] == 5_000
        assert not self.config["proxy"]["retries"]["enable"]
        requests.append(path)
        if path.endswith("&/@recursive_resource_usage"):
            return value
        return original_get(self, path=path, **kwargs)

    monkeypatch.setattr(Client, "get", get)

    async def main():
        """Read an estimate and verify the exact original config object survives."""
        async with adapter.worker() as worker:
            original = worker.session.client.config
            before = copy.deepcopy(original)
            assert await worker.estimate_source(path="//tmp") == expected
            assert worker.session.client.config is original
            assert original == before

    run(main())
    assert len(requests) == 2
    assert all("list" not in client.calls for client in store.clients)
    assert all("read_table" not in client.calls for client in store.clients)


@pytest.mark.parametrize(
    "error", [KeyError("attribute unavailable"), OSError("timeout")]
)
def test_estimate_source_failure_is_optional(adapter, store, monkeypatch, error):
    """Unavailable attributes and transport failures never fail the copy."""
    add_node(store=store, kind="file")
    original_get = Client.get
    progress = Measurements()
    adapter.progress = progress

    def get(self, *, path, **kwargs):
        """Fail only the optional resource attribute lookup."""
        if path.endswith("&/@recursive_resource_usage"):
            raise error
        return original_get(self, path=path, **kwargs)

    monkeypatch.setattr(Client, "get", get)

    async def main():
        """Recover a usable worker after the optional request fails."""
        async with adapter.worker() as worker:
            original = worker.session.client.config
            assert await worker.estimate_source(path="//tmp/source") == SourceEstimate()
            assert worker.session.client.config is original
            assert progress.events[0]["error"] is type(error)
            async with worker.snapshot(path="//tmp/source") as snapshot:
                assert snapshot.node.kind == "file"

    run(main())
    assert store.aborts == 0


def test_estimate_invalid_path_never_reaches_sdk(adapter, store):
    """A malformed optional source estimate makes no metadata requests."""

    async def main():
        """Return an unknown estimate before resolving an invalid source path."""
        async with adapter.worker() as worker:
            assert await worker.estimate_source(path="//tmp/link&") == SourceEstimate()

    run(main())
    assert store.clients[0].calls == ["cleanup"]


def test_estimate_cancellation_propagates_and_restores_config(adapter, monkeypatch):
    """Cancellation cannot become an unknown estimate or leak request settings."""
    entered = threading.Event()
    release = threading.Event()
    original_get = Client.get

    def get(self, *, path, **kwargs):
        """Delay the estimate while leaving its parent resolution unchanged."""
        if path.endswith("&/@recursive_resource_usage"):
            entered.set()
            release.wait(timeout=3)
            return {"node_count": 7, "disk_space": 1024}
        return original_get(self, path=path, **kwargs)

    monkeypatch.setattr(Client, "get", get)

    async def main():
        """Wait for safe cancellation before checking the restored config."""
        async with adapter.worker() as worker:
            original = worker.session.client.config
            task = asyncio.create_task(worker.estimate_source(path="//tmp"))
            assert await asyncio.to_thread(entered.wait, 3)
            task.cancel()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert worker.session.client.config is original

    run(main())


def test_progress_records_elapsed_time_with_real_collector(store, monkeypatch):
    """The adapter and collector include elapsed time for success and failure."""
    clock = iter([10.0, 10.25, 20.0, 20.5])
    monkeypatch.setattr(
        progress_module, "time", SimpleNamespace(monotonic=lambda: next(clock))
    )
    progress = Progress()
    adapter = YtAdapter(proxy="test", client_factory=store.factory, progress=progress)

    def request(*, fail):
        """Finish or fail a synchronous logical request."""
        if fail:
            raise OSError("failed request")
        return 7

    async def main():
        """Run two metadata calls without timing worker setup or cleanup."""
        async with adapter.worker() as worker:
            assert await worker.session.call(request, fail=False) == 7
            with pytest.raises(OSError, match="failed request"):
                await worker.session.call(request, fail=True)

    run(main())
    assert list(progress.timings) == ["yt_metadata"]
    timing = progress.timings["yt_metadata"]
    assert timing.count == 2
    assert timing.errors == 1
    assert timing.seconds == 0.75


def test_raw_table_request_preserves_snapshot_and_flags(monkeypatch):
    """Send the exact pinned range and lossless format through the SDK driver."""
    from ytclone.format import ROW_FORMAT

    captured = {}
    response = object()
    client = object()

    def make_request(**kwargs):
        """Capture the low-level request without invoking SDK row parsing."""
        captured.update(kwargs)
        return response

    monkeypatch.setattr(yt_module, "make_request", make_request)
    actual = yt_module._read_table(
        client=client,
        pinned="#node-id",
        part=Range(index=2, start=7, stop=13),
        transaction_id="external-snapshot",
    )
    assert actual is response
    assert captured["client"] is client
    assert captured["command_name"] == "read_table"
    assert captured["return_content"] is False
    assert captured["use_heavy_proxy"] is True
    assert captured["allow_retries"] is False
    params = captured["params"]
    assert params["transaction_id"] == "external-snapshot"
    assert str(params["path"]) == "#node-id"
    assert params["path"].attributes["ranges"] == [
        {"lower_limit": {"row_index": 7}, "upper_limit": {"row_index": 13}}
    ]
    assert params["output_format"].to_yson_type().attributes == (
        yt_module.yt.YsonFormat(attributes=ROW_FORMAT).to_yson_type().attributes
    )
    assert params["unordered"] is False
    assert params["omit_inaccessible_columns"] is False
    assert params["omit_inaccessible_rows"] is False
    assert params["control_attributes"] == {
        "enable_row_index": False,
        "enable_table_index": False,
        "enable_range_index": False,
    }


@pytest.mark.parametrize("transaction_id", [None, "", "0-0-0-0"])
def test_raw_table_request_rejects_missing_snapshot(monkeypatch, transaction_id):
    """Never fall back to an untransactional read when a snapshot is absent."""

    def unexpected(**kwargs):
        """Reject a request that escaped the transaction guard."""
        pytest.fail("No HTTP request should be made without a snapshot")

    monkeypatch.setattr(yt_module, "make_request", unexpected)
    with pytest.raises(ValueError, match="snapshot transaction"):
        yt_module._read_table(
            client=object(),
            pinned="#node-id",
            part=Range(index=0, start=0, stop=1),
            transaction_id=transaction_id,
        )


def read_error(*, name):
    """Build representative SDK, transport and local failures for boundary tests."""
    from yt.packages.requests.exceptions import HTTPError, ReadTimeout
    from yt.wrapper.errors import create_response_error
    from yt.wrapper.format import YtFormatReadError

    if name == "timeout":
        return ReadTimeout("read interrupted")
    if name.startswith("http_"):
        return HTTPError(
            "HTTP read failed", response=SimpleNamespace(status_code=int(name[5:]))
        )
    if name == "format":
        return YtFormatReadError("invalid format")
    if name == "zstd":
        return zstandard.ZstdError("compression failed")
    if name == "value":
        return ValueError("invalid read input")
    if name == "cancel":
        return asyncio.CancelledError()
    codes = {
        "request_timeout": 3,
        "chunk_unavailable": 716,
        "no_transaction": 11000,
        "access": 901,
        "authentication": 900,
        "resolve": 500,
        "nested_no_transaction": 3,
    }
    error = {"message": name, "code": codes[name]}
    if name == "nested_no_transaction":
        error["inner_errors"] = [{"message": "snapshot lost", "code": 11000}]
    return create_response_error(error)


@pytest.mark.parametrize("stage", ["open", "stream"])
@pytest.mark.parametrize(
    ("name", "retryable"),
    [
        ("timeout", True),
        ("http_503", True),
        ("http_403", False),
        ("request_timeout", True),
        ("chunk_unavailable", True),
        ("no_transaction", False),
        ("access", False),
        ("authentication", False),
        ("resolve", False),
        ("nested_no_transaction", False),
        ("format", False),
        ("zstd", False),
        ("value", False),
        ("cancel", False),
    ],
)
def test_raw_read_classifies_only_transient_errors(
    adapter, store, monkeypatch, stage, name, retryable
):
    """Translate source failures at both request opening and response consumption."""
    from ytclone.interfaces import RetryableReadError

    add_node(store=store)
    error = read_error(name=name)

    class BrokenReader(io.BytesIO):
        def read(self, size=-1):
            """Fail inside zstd's read from the underlying SDK response."""
            raise error

    raw = BrokenReader()

    def read_response(self, **kwargs):
        """Fail before returning a response or during its first read."""
        self.record("read_table")
        if stage == "open":
            raise error
        return raw

    monkeypatch.setattr(Client, "read_response", read_response)

    async def main():
        """Consume one failed part and verify the original exception survives."""
        expected = RetryableReadError if retryable else type(error)
        with pytest.raises(expected) as caught:
            async with (
                adapter.worker() as worker,
                worker.snapshot(path="//tmp/source") as snapshot,
            ):
                async for _ in worker.read_part(
                    snapshot=snapshot,
                    part=Range(index=0, start=0, stop=2),
                    compression_level=3,
                    chunk_size=10,
                ):
                    pytest.fail("The failing source must not yield bytes")
        if retryable:
            assert caught.value.__cause__ is error
        else:
            assert caught.value is error

    run(main())
    if stage == "stream":
        assert raw.closed
    assert store.aborts == 1
    assert store.commits == 0


def test_second_read_attempt_uses_fresh_response_and_compressor(
    adapter, store, monkeypatch
):
    """Restart the entire range in the same snapshot after a transient read."""
    from ytclone.interfaces import RetryableReadError

    add_node(store=store)
    error = read_error(name="timeout")
    payload = b"raw bytes do not need to be parsed here" * 100
    requests = []
    streams = []

    class BrokenReader(io.BytesIO):
        def read(self, size=-1):
            """Produce a transport error from the first raw response."""
            raise error

    def make_request(**kwargs):
        """Return a broken stream followed by a complete fresh response."""
        requests.append(kwargs["params"])
        raw = BrokenReader() if len(requests) == 1 else io.BytesIO(payload)
        streams.append(raw)
        return raw

    monkeypatch.setattr(yt_module, "make_request", make_request)

    async def main():
        """Read the same part twice and decode only the successful zstd frame."""
        async with (
            adapter.worker() as worker,
            worker.snapshot(path="//tmp/source") as snapshot,
        ):
            options = {
                "part": Range(index=0, start=0, stop=2),
                "compression_level": 3,
                "chunk_size": 10,
            }
            with pytest.raises(RetryableReadError):
                async for _ in worker.read_part(snapshot=snapshot, **options):
                    pass
            assert streams[0].closed
            data = b"".join(
                [
                    chunk
                    async for chunk in worker.read_part(snapshot=snapshot, **options)
                ]
            )
            with zstandard.ZstdDecompressor().stream_reader(io.BytesIO(data)) as reader:
                assert reader.read() == payload

    run(main())
    assert len(requests) == 2
    assert all(stream.closed for stream in streams)
    assert {request["transaction_id"] for request in requests} == {
        "snapshot-transaction"
    }
    assert requests[0]["path"].attributes == requests[1]["path"].attributes
    assert store.clients[0].calls.count("transaction") == 1
    assert store.clients[0].calls.count("lock:snapshot") == 1


def test_lost_pinger_prevents_another_read_attempt(adapter, store, monkeypatch):
    """A failed keepalive stops a whole-part retry before its HTTP request."""
    from ytclone.interfaces import RetryableReadError

    add_node(store=store)
    calls = []

    def make_request(**kwargs):
        """Lose keepalive together with the first response."""
        calls.append(kwargs)
        store.ping_alive = False
        raise read_error(name="timeout")

    monkeypatch.setattr(yt_module, "make_request", make_request)

    async def main():
        """Retry explicitly to check the guard at the start of each attempt."""
        with pytest.raises(RuntimeError, match="ping failed"):
            async with (
                adapter.worker() as worker,
                worker.snapshot(path="//tmp/source") as snapshot,
            ):
                options = {
                    "part": Range(index=0, start=0, stop=2),
                    "compression_level": 3,
                    "chunk_size": 10,
                }
                with pytest.raises(RetryableReadError):
                    await worker.read_part(snapshot=snapshot, **options).__anext__()
                await worker.read_part(snapshot=snapshot, **options).__anext__()

    run(main())
    assert len(calls) == 1
    assert store.aborts == 1


def test_consumer_error_is_not_classified_as_source_error(adapter, store, monkeypatch):
    """An upload failure injected at yield remains an unwrapped consumer error."""
    add_node(store=store)
    error = read_error(name="timeout")

    async def main():
        """Throw an S3-shaped timeout into the active async source iterator."""
        async with (
            adapter.worker() as worker,
            worker.snapshot(path="//tmp/source") as snapshot,
        ):
            chunks = worker.read_part(
                snapshot=snapshot,
                part=Range(index=0, start=0, stop=2),
                compression_level=3,
                chunk_size=1,
            )
            assert await chunks.__anext__()
            with pytest.raises(type(error)) as caught:
                await chunks.athrow(error)
            assert caught.value is error
            assert worker._reader is None

    run(main())


@pytest.mark.parametrize("primary", ["timeout", "value", "cancel"])
def test_cleanup_failure_preserves_primary_error_and_forbids_retry(
    adapter, store, monkeypatch, primary
):
    """Uncertain response cleanup cannot hide the source error or allow reuse."""
    add_node(store=store)
    source_error = read_error(name=primary)
    cleanup_error = RuntimeError("close failed")

    class BrokenReader(io.BytesIO):
        def read(self, size=-1):
            """Fail while consuming the response."""
            raise source_error

        def close(self):
            """Report uncertain cleanup after closing the test's memory stream."""
            super().close()
            raise cleanup_error

    raw = BrokenReader()

    def read_response(self, **kwargs):
        """Return the response whose read and close both fail."""
        return raw

    monkeypatch.setattr(Client, "read_response", read_response)

    async def main():
        """Verify that a retry signal becomes its fatal original SDK error."""
        async with adapter.worker() as worker:
            with pytest.raises(type(source_error)) as caught:
                async with worker.snapshot(path="//tmp/source") as snapshot:
                    await worker.read_part(
                        snapshot=snapshot,
                        part=Range(index=0, start=0, stop=2),
                        compression_level=3,
                        chunk_size=10,
                    ).__anext__()
            assert caught.value is source_error
            assert worker.session.poisoned

    run(main())
    assert store.aborts == 1


def test_file_read_failure_is_left_to_sdk_retries(adapter, store, monkeypatch):
    """Keep the file path byte-oriented with its existing SDK retry policy."""
    add_node(store=store, kind="file")
    error = read_error(name="timeout")

    def read_file(self, **kwargs):
        """Model a file read failure after the SDK has exhausted its retries."""
        raise error

    monkeypatch.setattr(Client, "read_file", read_file)

    async def main():
        """Do not wrap an exhausted file read as a whole-table retry signal."""
        with pytest.raises(type(error)) as caught:
            async with (
                adapter.worker() as worker,
                worker.snapshot(path="//tmp/source") as snapshot,
            ):
                await worker.read_part(
                    snapshot=snapshot,
                    part=Range(index=0, start=0, stop=11),
                    compression_level=3,
                    chunk_size=10,
                ).__anext__()
        assert caught.value is error

    run(main())
