"""Opt-in round trips against the disposable services in compose.yml."""

import asyncio
import contextlib
import copy
import io
import os
import random
import uuid

import aiohttp
import pytest
import yt.wrapper as sdk
from aiohttp_s3_client import S3Client
from yt import yson
from yt.packages.requests.exceptions import ReadTimeout
from yt.wrapper.http_helpers import _get_session

import ytclone.yt as yt_adapter
from ytclone.engine import CopyOptions, export_node, export_tree, restore_tree
from ytclone.format import MIB, RESTORE_MARKER, ROW_FORMAT, Manifest, node_prefix
from ytclone.interfaces import RetryableReadError
from ytclone.progress import Progress
from ytclone.s3 import S3Storage
from ytclone.yt import YtAdapter

pytestmark = pytest.mark.skipif(
    os.environ.get("YTCLONE_LOCAL_TESTS") != "1",
    reason="Set YTCLONE_LOCAL_TESTS=1 to use disposable local YT/S3",
)


def local_client(*, proxy="http://localhost:8000", config=None):
    return sdk.YtClient(
        proxy=proxy,
        config={
            **(config or {}),
            "enable_token": False,
            "proxy": {"enable_proxy_discovery": False, "retries": {"count": 1}},
        },
    )


def rows(*, client, path):
    with contextlib.closing(
        client.read_table(
            table=path,
            format=sdk.YsonFormat(attributes=ROW_FORMAT),
            raw=True,
        )
    ) as stream:
        return list(yson.load(stream=stream, yson_type="list_fragment", encoding=None))


def test_local_allowed_subtree_without_ancestor_read_does_not_follow_links():
    """Read a guest-visible subtree without reading its denied parent or links."""
    root = "//tmp/ytclone-acl-" + uuid.uuid4().hex[:12]
    source = root + "/source"
    folder = source + "/folder"
    alias = source + "/alias"
    client = local_client()
    reader = local_client(
        config={
            "impersonation_user": "guest",
            # This fixture is immutable; guest cannot create SDK snapshot transactions.
            "read_retries": {"create_transaction_and_take_snapshot_lock": False},
        },
    )

    def guest_client(*, proxy, config):
        return local_client(
            proxy=proxy,
            config={**config, "impersonation_user": "guest"},
        )

    try:
        if not client.exists(path="//sys/users/guest"):
            pytest.skip("The disposable local cluster has no built-in guest user")

        client.create(
            type="map_node",
            path=root,
            attributes={
                "inherit_acl": False,
                "acl": [
                    {"action": "deny", "subjects": ["guest"], "permissions": ["read"]}
                ],
            },
        )
        client.create(
            type="map_node",
            path=source,
            attributes={
                "inherit_acl": False,
                "acl": [
                    {"action": "allow", "subjects": ["guest"], "permissions": ["read"]}
                ],
            },
        )
        client.create(type="map_node", path=folder)
        file_id = client.create(type="file", path=folder + "/file")
        client.write_file(destination=folder + "/file", stream=io.BytesIO(b"payload"))
        client.link(target_path=folder, link_path=alias)
        link_id = client.get(path=alias + "&/@id")

        with pytest.raises(sdk.YtResponseError) as denied:
            reader.get(path=root + "/@id")
        assert denied.value.is_access_denied()

        with contextlib.closing(
            reader.read_file(path=folder + "/file", enable_read_parallel=False)
        ) as stream:
            assert stream.read() == b"payload"

        async def scenario():
            yt = YtAdapter(
                proxy="http://localhost:8000",
                proxy_discovery=False,
                client_factory=guest_client,
            )
            async with yt.worker() as worker:
                node = await worker.inspect_node(path=folder + "/file")
                assert node.kind == "file" and node.node_id == file_id

                entries = [node async for node in worker.walk(path=source)]
                assert {node.path for node in entries} == {
                    source,
                    folder,
                    folder + "/file",
                    alias,
                }
                link = next(node for node in entries if node.path == alias)
                assert link.kind == "link" and link.node_id == link_id

                with pytest.raises((sdk.YtResponseError, ValueError)):
                    await worker.inspect_node(path=alias + "/file")

        asyncio.run(scenario())
    finally:
        try:
            client.remove(path=root, recursive=True, force=True)
        finally:
            reader._cleanup()
            client._cleanup()


def test_local_round_trip_restart_changes_and_corruption():
    run_id = uuid.uuid4().hex[:12]
    root = "//tmp/ytclone-main-" + run_id
    source, target = root + "/source", root + "/restored"
    prefix = "main-smoke-" + run_id
    client = local_client()
    schema = yson.to_yson_type(
        [
            {"name": "id", "type": "int64", "sort_order": "ascending"},
            {"name": "payload", "type": "string"},
            {"name": "u", "type": "uint64"},
            {"name": "details", "type": "any"},
        ],
        attributes={"strict": True, "unique_keys": True},
    )
    expected = [
        {
            b"id": 1,
            b"payload": b"\x00\xff",
            b"u": yson.YsonUint64(2**64 - 1),
            b"details": {b"x": [1, b"\xff"]},
        },
        {b"id": 2, b"payload": b"", b"u": yson.YsonUint64(0), b"details": []},
        {b"id": 3, b"payload": None, b"u": None, b"details": None},
    ]
    client.create(
        type="map_node",
        path=source,
        recursive=True,
        attributes={"note": "source folder"},
    )
    client.create(
        type="map_node", path=source + "/empty", attributes={"note": "empty folder"}
    )
    for name in ("table", "empty_table"):
        client.create(
            type="table",
            path=source + "/" + name,
            attributes={
                "schema": schema,
                "binary_metadata": yson.to_yson_type(
                    b"\xff", attributes={b"tag": b"raw"}
                ),
            },
        )
    client.write_table(
        table=source + "/table",
        input_stream=yson.dumps(
            object=expected, yson_type="list_fragment", yson_format="binary"
        ),
        format=sdk.YsonFormat(attributes=ROW_FORMAT),
        raw=True,
    )
    payload = bytes(range(256)) * 4096
    for name, content in (("file", payload), ("empty_file", b"")):
        client.create(
            type="file",
            path=source + "/" + name,
            attributes={
                "file_name": name,
                "executable": False,
                "note": "file metadata",
            },
        )
        if content:
            client.write_file(
                destination=source + "/" + name, stream=io.BytesIO(content)
            )
    client.create(type="map_node", path=target, attributes={"old_attribute": "remove"})
    client.create(type="file", path=target + "/unrelated")
    options = CopyOptions(
        concurrency=2,
        queue_size=2,
        part_size=256 * 1024,
        chunk_size=5 * MIB,
    )

    async def scenario():
        yt = YtAdapter(
            proxy="http://localhost:8000",
            proxy_discovery=False,
            client_factory=local_client,
        )
        async with aiohttp.ClientSession() as session:
            s3 = S3Client(
                session=session,
                url="http://localhost:8333/yt-backup",
                access_key_id="local-bench",
                secret_access_key="local-bench-secret",
                region="us-east-1",
            )
            storage = S3Storage(client=s3, chunk_size=options.chunk_size)
            export_kwargs = {
                "yt": yt,
                "storage": storage,
                "source": source,
                "prefix": prefix,
                "cluster": "local",
                "options": options,
            }
            restore_kwargs = {
                "yt": yt,
                "storage": storage,
                "source_prefix": node_prefix(prefix=prefix, path=source),
                "destination": target,
                "options": options,
            }
            exported = await export_tree(**export_kwargs)
            assert exported.failed == 0 and exported.completed == 6
            repeated = await export_tree(**export_kwargs)
            assert (
                repeated.failed == 0
                and repeated.skipped == 6
                and repeated.completed == 0
            )
            restored = await restore_tree(**restore_kwargs)
            assert restored.failed == 0 and restored.completed == 6
            assert rows(client=client, path=target + "/table") == rows(
                client=client, path=source + "/table"
            )
            assert client.get(path=target + "/table/@schema") == client.get(
                path=source + "/table/@schema"
            )
            assert client.get(path=target + "/table/@binary_metadata") == client.get(
                path=source + "/table/@binary_metadata"
            )
            with contextlib.closing(client.read_file(path=target + "/file")) as stream:
                assert stream.read() == payload
            assert client.exists(path=target + "/unrelated")
            assert not client.exists(path=target + "/@old_attribute")
            assert client.get(path=target + "/empty_table/@row_count") == 0
            assert client.get(path=target + "/empty_file/@uncompressed_data_size") == 0
            repeated = await restore_tree(**restore_kwargs)
            assert (
                repeated.failed == 0
                and repeated.skipped == 6
                and repeated.completed == 0
            )

            client.set(path=source + "/table/@new_attribute", value="changed")
            changed = await export_tree(**export_kwargs)
            assert (
                changed.failed == 0 and changed.completed == 1 and changed.skipped == 5
            )
            changed = await restore_tree(**restore_kwargs)
            assert (
                changed.failed == 0 and changed.completed == 1 and changed.skipped == 5
            )
            assert client.get(path=target + "/table/@new_attribute") == "changed"

            extra = {
                b"id": 4,
                b"payload": b"new row",
                b"u": yson.YsonUint64(7),
                b"details": None,
            }
            client.write_table(
                table=sdk.TablePath(name=source + "/table", append=True),
                input_stream=yson.dumps(
                    object=[extra], yson_type="list_fragment", yson_format="binary"
                ),
                format=sdk.YsonFormat(attributes=ROW_FORMAT),
                raw=True,
            )
            expected.append(extra)
            changed = await export_tree(**export_kwargs)
            assert (
                changed.failed == 0 and changed.completed == 1 and changed.skipped == 5
            )
            changed = await restore_tree(**restore_kwargs)
            assert (
                changed.failed == 0 and changed.completed == 1 and changed.skipped == 5
            )
            assert rows(client=client, path=target + "/table") == expected

            unwanted = {b"id": 5, b"payload": b"locally modified"}
            client.write_table(
                table=sdk.TablePath(name=target + "/table", append=True),
                input_stream=yson.dumps(
                    object=[unwanted], yson_type="list_fragment", yson_format="binary"
                ),
                format=sdk.YsonFormat(attributes=ROW_FORMAT),
                raw=True,
            )
            repaired = await restore_tree(**restore_kwargs)
            assert (
                repaired.failed == 0
                and repaired.completed == 1
                and repaired.skipped == 5
            )
            assert rows(client=client, path=target + "/table") == expected

            client.set(path=target + "/file/@note", value="modified locally")
            repaired = await restore_tree(**restore_kwargs)
            assert (
                repaired.failed == 0
                and repaired.completed == 1
                and repaired.skipped == 5
            )

            manifest_key = (
                node_prefix(prefix=prefix, path=source + "/table") + "/@manifest.json"
            )
            manifest = Manifest.from_bytes(await storage.read_bytes(key=manifest_key))
            ref = manifest.parts[0].object
            original = await storage.read_bytes(key=ref.key)
            await storage.put_bytes(key=ref.key, data=b"x" * len(original))
            client.remove(path=target + "/table/@" + RESTORE_MARKER)
            previous_id = client.get(path=target + "/table/@id")
            failed = await restore_tree(**restore_kwargs)
            assert failed.failed == 1
            assert client.get(path=target + "/table/@id") == previous_id
            assert rows(client=client, path=target + "/table") == expected
            await storage.put_bytes(key=ref.key, data=original)
            repaired = await restore_tree(**restore_kwargs)
            assert repaired.failed == 0 and repaired.completed == 1

    asyncio.run(scenario())
    print("Local fixtures:", root, "S3 prefix:", prefix)


@pytest.fixture(scope="module")
def retry_rows():
    """Make enough incompressible data to fail after a real multipart upload starts."""
    rng = random.Random(42)
    return [
        {b"id": index, b"payload": rng.randbytes(512 * 1024)} for index in range(32)
    ]


@pytest.mark.parametrize("failure", ["once", "exhausted", "fatal", "lost_snapshot"])
def test_local_raw_read_retries_preserve_snapshot(monkeypatch, retry_rows, failure):
    """Repeat the failed range in one snapshot or leave the archive unpublished."""
    run_id = uuid.uuid4().hex[:12]
    root = "//tmp/ytclone-raw-retry-" + run_id
    source, target = root + "/source", root + "/restored"
    prefix = "raw-retry-" + run_id
    client = local_client()
    client.create(
        type="table",
        path=source,
        recursive=True,
        attributes={
            "schema": [
                {"name": "id", "type": "int64"},
                {"name": "payload", "type": "string"},
            ],
            "note": "snapshot metadata",
        },
    )
    client.write_table(table=source, input_stream=retry_rows)

    commands, reads, streams, failures, s3_requests = [], [], [], [], []

    def traced_client(*, proxy, config):
        """Record real HTTP commands from copier clients, excluding the writer."""
        instance = local_client(proxy=proxy, config=config)
        session = _get_session(client=instance)
        send = session.send

        def traced_send(request, **kwargs):
            """Observe one HTTP attempt without recording headers or credentials."""
            commands.append(request.url.rsplit("/", 1)[-1].split("?", 1)[0])
            return send(request=request, **kwargs)

        session.send = traced_send
        return instance

    make_request = yt_adapter.make_request

    def interrupted_request(*, command_name, params, **kwargs):
        """Interrupt the second archive range after six MiB of delivered input."""
        reads.append(copy.deepcopy(params))
        stream = make_request(command_name=command_name, params=params, **kwargs)
        streams.append(stream)
        start = params["path"].attributes["ranges"][0]["lower_limit"]["row_index"]
        if start == 0 or (failure == "once" and failures):
            return stream

        original_read = stream.read
        delivered = 0

        def interrupted_read(length=None):
            """Inject a source error after the first S3 multipart buffer is filled."""
            nonlocal delivered
            if delivered >= 6 * MIB:
                failures.append(delivered)
                if failure in ("once", "lost_snapshot"):
                    writer = local_client()
                    try:
                        if failure == "once":
                            writer.write_table(
                                table=source,
                                input_stream=[
                                    {b"id": -1, b"payload": b"new source state"}
                                ],
                            )
                        else:
                            writer.abort_transaction(
                                transaction=params["transaction_id"]
                            )
                    finally:
                        writer._cleanup()
                elif failure == "fatal":
                    raise ValueError("injected fatal stream failure")
                raise ReadTimeout("injected interrupted table response")

            data = original_read(length)
            delivered += len(data)
            return data

        stream.read = interrupted_read
        return stream

    monkeypatch.setattr(yt_adapter, "make_request", interrupted_request)
    options = CopyOptions(concurrency=1, part_size=8 * MIB, chunk_size=5 * MIB)

    async def scenario():
        """Export through actual adapters and check publication, cleanup and restore."""
        yt = YtAdapter(
            proxy="http://localhost:8000",
            proxy_discovery=False,
            client_factory=traced_client,
        )
        trace = aiohttp.TraceConfig()

        async def request_finished(session, trace_config_ctx, params):
            """Capture multipart lifecycle requests without headers or payloads."""
            s3_requests.append((params.method, dict(params.url.query)))

        trace.on_request_end.append(request_finished)
        async with aiohttp.ClientSession(trace_configs=[trace]) as session:
            s3 = S3Client(
                session=session,
                url="http://localhost:8333/yt-backup",
                access_key_id="local-bench",
                secret_access_key="local-bench-secret",
                region="us-east-1",
            )
            storage = S3Storage(client=s3, chunk_size=options.chunk_size)
            archive = node_prefix(prefix=prefix, path=source)
            manifest_key = archive + "/@manifest.json"
            async with yt.worker() as worker:
                transfer = export_node(
                    yt=worker,
                    storage=storage,
                    path=source,
                    prefix=prefix,
                    cluster="local",
                    options=options,
                    progress=Progress(),
                )
                if failure == "once":
                    assert await transfer == ("completed", "copied")
                else:
                    expected_error = {
                        "exhausted": RetryableReadError,
                        "fatal": ValueError,
                        "lost_snapshot": sdk.YtResponseError,
                    }[failure]
                    with pytest.raises(expected_error):
                        await transfer

            assert (
                sum(name in ("start_tx", "start_transaction") for name in commands) == 1
            )
            assert commands.count("lock") == 1
            assert commands.count("read_table") == len(reads)
            assert len({str(params["transaction_id"]) for params in reads}) == 1
            assert all(stream.closed for stream in streams)
            assert all(size >= 6 * MIB for size in failures)
            failed_uploads = [
                params
                for method, params in s3_requests
                if method == "DELETE" and "uploadId" in params
            ]
            assert len(failed_uploads) == len(failures)

            ranges = [params["path"].attributes["ranges"] for params in reads]
            assert sum(value == ranges[0] for value in ranges) == 1
            if failure == "fatal":
                assert len(ranges) == 2
            else:
                assert ranges[1] == ranges[2]
            manifest_data = await storage.read_bytes(key=manifest_key)
            if failure != "once":
                assert manifest_data is None
                assert (
                    len(reads)
                    == {"exhausted": 4, "fatal": 2, "lost_snapshot": 3}[failure]
                )
                return

            manifest = Manifest.from_bytes(manifest_data)
            assert len(reads) == len(manifest.parts) + 1
            assert len(failures) == 1
            restored = await restore_tree(
                yt=yt,
                storage=storage,
                source_prefix=archive,
                destination=target,
                options=options,
            )
            assert restored.failed == 0 and restored.completed == 1
            assert rows(client=client, path=target) == retry_rows
            assert rows(client=client, path=source) == [
                {b"id": -1, b"payload": b"new source state"}
            ]
            assert client.get(path=target + "/@note") == "snapshot metadata"

    asyncio.run(scenario())
