"""Restore retries replay complete nodes only after a confirmed rollback."""

import asyncio
from collections import Counter
from types import SimpleNamespace

import aiohttp
import pytest
import zstandard
from test_engine import MemoryStorage, object_ref
from test_yt import Client, Store, Transaction, add_node
from yt import yson
from yt.packages.requests.exceptions import ConnectionError as YtConnectionError

from ytclone import restore_retry
from ytclone.engine import CopyOptions, restore_node, restore_tree
from ytclone.format import (
    RESTORE_MARKER,
    Manifest,
    Part,
    canonical_attributes,
    data_key,
)
from ytclone.interfaces import RetryableDownloadError, RetryableRestoreError
from ytclone.progress import Progress
from ytclone.yt import YtAdapter

PREFIX = "archive/source"
DESTINATION = "//tmp/restored"
ORIGINAL_RETRY_DELAY = restore_retry.retry_delay


class RecordingTransaction(Transaction):
    """Expose committed state and partial writes at every transaction boundary."""

    def __enter__(self):
        """Remember the old destination before opening the private working copy."""
        store = self.client.store
        store.transactions.append(self)
        store.visible_at_begin.append(store.nodes[store.original_id]["data"])
        return super().__enter__()

    def __exit__(self, error_type, error, traceback):
        """Record aborted bytes and optionally model an uncertain transaction end."""
        store = self.client.store
        if error_type is not None:
            record = self.client.nodes[self.client.find(DESTINATION)]
            store.aborted_data.append(record["data"])
            if store.fail_abort:
                store.fail_abort = False
                raise YtConnectionError("abort acknowledgement lost")

        result = super().__exit__(error_type, error, traceback)
        if error_type is None and store.fail_commit:
            store.fail_commit = False
            raise YtConnectionError("commit acknowledgement lost")
        return result


class RecordingClient(Client):
    """Keep SDK thread checks while injecting one failed write or transaction end."""

    def Transaction(self, *, timeout, interrupt_on_failed):
        """Create a distinct recorded transaction for each restore attempt."""
        self.record("transaction")
        return RecordingTransaction(self)

    def write(self, *, path, chunks):
        """Optionally disconnect after changing the transactional second part."""
        if path.attributes.get("append") and self.store.fail_write:
            self.store.fail_write = False
            first = next(iter(chunks))
            self.nodes[self.find(path)]["data"] += first
            raise YtConnectionError("write connection reset after partial append")
        return super().write(path=path, chunks=chunks)


class RecordingStore(Store):
    """Share the fake destination and capture transaction evidence."""

    def __init__(self):
        """Initialize failure switches and observed transaction state."""
        super().__init__()
        self.transactions = []
        self.visible_at_begin = []
        self.aborted_data = []
        self.original_id = None
        self.fail_abort = False
        self.fail_commit = False
        self.fail_write = False

    def factory(self, *, proxy, config):
        """Build one thread-affine client for the real adapter."""
        client = RecordingClient(self)
        client.config = config
        self.clients.append(client)
        return client


class FlakyStorage(MemoryStorage):
    """Break the second object once, after the first part has been written."""

    def __init__(self, *, error=None):
        """Keep an ordered download trace and one injected stream failure."""
        super().__init__(events=[])
        self.error = error
        self.failure_key = None
        self.failure_observer = None

    async def download(self, *, ref):
        """Expose a valid compressed frame before a possible EOF failure."""
        async for data in super().download(ref=ref):
            yield data
        if ref.key == self.failure_key and self.error is not None:
            error, self.error = self.error, None
            if self.failure_observer is not None:
                self.failure_observer()
            raise error


def make_case(*, kind, error=None):
    """Build a two-part archive and an existing destination of the same kind."""
    if kind == "table":
        chunks = [
            yson.dumps(object=rows, yson_type="list_fragment", yson_format="binary")
            for rows in ([{"x": 1}, {"x": 2}], [{"x": 3}])
        ]
        old = yson.dumps(
            object=[{"x": 99}], yson_type="list_fragment", yson_format="binary"
        )
        attrs = {b"schema": [{b"name": b"x", b"type": b"int64"}]}
        bounds = ((0, 2), (2, 3))
    else:
        chunks = [b"first-part-" * 40, b"second-part-" * 40]
        old = b"keep the old file until commit"
        attrs = {b"description": b"saved file"}
        bounds = ((0, len(chunks[0])), (len(chunks[0]), sum(map(len, chunks))))

    store = RecordingStore()
    Client(store).create(type="map_node", path="//tmp")
    store.original_id = add_node(
        store=store, kind=kind, name="restored", data=old, attributes=attrs
    )
    storage = FlakyStorage(error=error)
    metadata = canonical_attributes(attributes=attrs)
    attributes_key = PREFIX + "/@attributes.yson"
    storage.objects[attributes_key] = metadata
    parts = []
    for index, (data, (start, stop)) in enumerate(zip(chunks, bounds)):
        key = data_key(prefix=PREFIX, kind=kind, index=index)
        compressed = zstandard.ZstdCompressor().compress(data)
        storage.objects[key] = compressed
        parts.append(
            Part(object=object_ref(key=key, data=compressed), start=start, stop=stop)
        )
    storage.failure_key = parts[1].object.key
    manifest = Manifest(
        kind=kind,
        source={
            "cluster": "test",
            "path": "//source",
            "node_id": "original",
            "content_revision": 1,
        },
        fingerprint="a" * 64,
        attributes=object_ref(key=attributes_key, data=metadata),
        parts=tuple(parts),
        row_count=3 if kind == "table" else 0,
        data_size=0 if kind == "table" else sum(map(len, chunks)),
    )
    storage.objects[PREFIX + "/@manifest.json"] = manifest.to_bytes()
    return SimpleNamespace(
        store=store,
        storage=storage,
        adapter=YtAdapter(proxy="test", client_factory=store.factory),
        manifest=manifest,
        chunks=chunks,
        old=old,
        progress=Progress(),
    )


async def restore_case(case):
    """Run the real restore engine through the fake transactional SDK."""
    async with case.adapter.worker() as worker:
        return await restore_node(
            yt=worker,
            storage=case.storage,
            prefix=PREFIX,
            destination=DESTINATION,
            progress=case.progress,
        )


@pytest.fixture(autouse=True)
def immediate_retries(monkeypatch):
    """Keep retry tests fast while retaining their real bounded policy."""
    monkeypatch.setattr(restore_retry, "retry_delay", lambda *, attempt: 0)


@pytest.mark.parametrize("kind", ["file", "table"])
@pytest.mark.parametrize("failure", ["download", "write"])
def test_retry_replays_all_parts_in_a_fresh_transaction(kind, failure):
    """Rollback a partial node, reread part zero and credit completion just once."""
    case = make_case(
        kind=kind,
        error=RetryableDownloadError("stream reset") if failure == "download" else None,
    )
    case.store.fail_write = failure == "write"

    def confirm_first_part_written():
        """Observe a committed old node alongside the private first restored part."""
        client = case.store.clients[0]
        assert client.nodes[client.find(DESTINATION)]["data"] == case.chunks[0]
        assert case.store.nodes[case.store.original_id]["data"] == case.old

    case.storage.failure_observer = confirm_first_part_written
    assert asyncio.run(restore_case(case)) == ("completed", "copied")
    assert len(case.store.transactions) == 2
    assert case.store.transactions[0] is not case.store.transactions[1]
    assert case.store.visible_at_begin == [case.old, case.old]
    assert case.store.aborts == 1
    assert case.store.commits == 1
    restored = next(x for x in case.store.nodes.values() if x["name"] == "restored")
    assert restored["data"] == b"".join(case.chunks)
    assert restored["attrs"][RESTORE_MARKER]["fingerprint"] == "a" * 64
    expected_keys = [part.object.key for part in case.manifest.parts]
    assert case.storage.downloaded == expected_keys * 2
    assert Counter(case.storage.reads) == {
        PREFIX + "/@manifest.json": 1,
        PREFIX + "/@attributes.yson": 1,
    }
    assert case.progress.completed_bytes == sum(
        part.object.size for part in case.manifest.parts
    )


@pytest.mark.parametrize("kind", ["file", "table"])
@pytest.mark.parametrize("failure", ["checksum", "auth"])
def test_permanent_stream_failure_aborts_without_replay(kind, failure):
    """Do not retry corrupt archives or denied access to their data."""
    error = (
        ValueError("download checksum mismatch")
        if failure == "checksum"
        else aiohttp.ClientResponseError(
            request_info=SimpleNamespace(real_url="https://example.test/part"),
            history=(),
            status=403,
            message="Forbidden",
        )
    )
    case = make_case(kind=kind, error=error)
    with pytest.raises(type(error)):
        asyncio.run(restore_case(case))
    assert len(case.store.transactions) == 1
    assert case.store.aborts == 1
    assert case.store.commits == 0
    assert case.store.nodes[case.store.original_id]["data"] == case.old
    assert case.progress.completed_bytes == 0
    assert case.storage.downloaded == [part.object.key for part in case.manifest.parts]


@pytest.mark.parametrize("failure", ["commit", "abort"])
def test_uncertain_transaction_end_is_never_retried(failure):
    """Never replay after losing acknowledgement of commit or rollback."""
    case = make_case(
        kind="file",
        error=RetryableDownloadError("stream reset") if failure == "abort" else None,
    )
    case.store.fail_commit = failure == "commit"
    case.store.fail_abort = failure == "abort"
    with pytest.raises((YtConnectionError, RetryableDownloadError)):
        asyncio.run(restore_case(case))
    assert len(case.store.transactions) == 1
    assert case.progress.completed_bytes == 0
    assert case.storage.downloaded == [part.object.key for part in case.manifest.parts]
    restored = next(x for x in case.store.nodes.values() if x["name"] == "restored")
    assert RESTORE_MARKER not in restored["attrs"]
    if failure == "commit":
        assert restored["data"] == b"".join(case.chunks)
    else:
        assert restored["data"] == case.old


def test_whole_node_retry_exhausts_exact_attempt_budget(monkeypatch):
    """Stop after the configured count instead of retrying indefinitely."""
    monkeypatch.setattr(restore_retry, "MAX_ATTEMPTS", 3)
    attempts = []

    @restore_retry.retry
    async def fail(*, destination):
        """Represent a safely rolled-back transient restore failure."""
        attempts.append(destination)
        raise RetryableRestoreError("temporary")

    with pytest.raises(RetryableRestoreError):
        asyncio.run(fail(destination=DESTINATION))
    assert attempts == [DESTINATION] * 3


def test_cancel_during_backoff_does_not_start_a_new_attempt(monkeypatch):
    """Cancellation interrupts the pause and prevents another transaction."""
    backoff_started = asyncio.Event()
    attempts = []

    def delay(*, attempt):
        """Signal the backoff boundary before its long cancellable sleep."""
        backoff_started.set()
        return 3600

    monkeypatch.setattr(restore_retry, "retry_delay", delay)

    @restore_retry.retry
    async def fail(*, destination):
        """Represent a transient failure on the first attempt."""
        attempts.append(destination)
        raise RetryableRestoreError("temporary")

    async def scenario():
        """Cancel once the first attempt has reached backoff."""
        task = asyncio.create_task(fail(destination=DESTINATION))
        await asyncio.wait_for(backoff_started.wait(), timeout=2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())
    assert attempts == [DESTINATION]


def test_plain_network_error_is_not_a_safe_retry_marker():
    """The engine policy only accepts errors certified after rollback."""
    attempts = []

    @restore_retry.retry
    async def fail(*, destination):
        """An unclassified connection error has unknown transaction status."""
        attempts.append(destination)
        raise YtConnectionError("unknown transaction state")

    with pytest.raises(YtConnectionError):
        asyncio.run(fail(destination=DESTINATION))
    assert attempts == [DESTINATION]


@pytest.mark.parametrize("kind", ["file", "table"])
def test_pipeline_counts_a_recovered_node_only_once(kind):
    """A recovered attempt becomes one completed node and no failed nodes."""
    case = make_case(kind=kind, error=RetryableDownloadError("stream reset"))

    async def scenario():
        """Restore a leaf through the real discovery and worker pipeline."""
        return await restore_tree(
            yt=case.adapter,
            storage=case.storage,
            source_prefix=PREFIX,
            destination=DESTINATION,
            options=CopyOptions(concurrency=1),
            progress=case.progress,
        )

    result = asyncio.run(scenario())
    assert (result.completed, result.failed, result.skipped) == (1, 0, 0)
    assert (case.progress.completed, case.progress.failed) == (1, 0)
    assert (case.progress.discovered, case.progress.active, case.progress.queued) == (
        1,
        0,
        0,
    )
    assert case.progress.completed_bytes == sum(
        part.object.size for part in case.manifest.parts
    )
    assert case.store.aborts == 1
    assert case.store.commits == 1


@pytest.mark.parametrize(
    "attempt, ceiling", [(1, 1), (2, 2), (5, 16), (6, 30), (29, 30)]
)
def test_restore_backoff_is_exponential_jitter_with_a_cap(
    monkeypatch, attempt, ceiling
):
    """Bound pauses independently of the size and duration of a restored node."""
    bounds = []

    def uniform(*, a, b):
        """Observe the requested jitter range deterministically."""
        bounds.append((a, b))
        return b

    monkeypatch.setattr(restore_retry.random, "uniform", uniform)
    assert ORIGINAL_RETRY_DELAY(attempt=attempt) == ceiling
    assert bounds == [(ceiling / 2, ceiling)]


def test_whole_restore_policy_has_no_deadline_or_attempt_timeout(monkeypatch):
    """Large nodes must not be cancelled by the per-request retry time budget."""
    original = restore_retry.aiomisc.asyncbackoff
    policies = []

    def backoff(**kwargs):
        """Capture the actual policy while retaining aiomisc execution."""
        policies.append(kwargs)
        return original(**kwargs)

    monkeypatch.setattr(restore_retry.aiomisc, "asyncbackoff", backoff)

    @restore_retry.retry
    async def successful(*, destination):
        """Return an ordinary completed node."""
        return destination

    assert asyncio.run(successful(destination=DESTINATION)) == DESTINATION
    assert len(policies) == 1
    assert policies[0]["attempt_timeout"] is None
    assert policies[0]["deadline"] is None
    assert policies[0]["max_tries"] == 30


def test_unconfirmed_timeout_does_not_trigger_aiomisc_implicit_retry():
    """An unclassified TimeoutError cannot bypass the rollback marker gate."""
    attempts = []

    @restore_retry.retry
    async def fail(*, destination):
        """Model a timeout outside the confirmed transaction rollback path."""
        attempts.append(destination)
        raise asyncio.TimeoutError("unknown transaction state")

    with pytest.raises(asyncio.TimeoutError):
        asyncio.run(fail(destination=DESTINATION))
    assert attempts == [DESTINATION]


def test_retry_log_reports_original_http_error_without_its_sensitive_text(caplog):
    """Report the node, attempt and HTTP status without exception bodies or headers."""
    attempts = 0
    http_error = aiohttp.ClientResponseError(
        request_info=SimpleNamespace(real_url="https://example.test/part"),
        history=(),
        status=503,
        message="SECRET_ERROR_BODY",
        headers={"Authorization": "SECRET_AUTHORIZATION"},
    )
    download_error = RetryableDownloadError("SECRET_DOWNLOAD_CONTEXT")
    download_error.__cause__ = http_error
    restore_error = RetryableRestoreError("SECRET_RESTORE_CONTEXT")
    restore_error.__cause__ = download_error

    @restore_retry.retry
    async def restored(*, destination):
        """Fail once after rollback and then complete the node."""
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise restore_error
        return destination

    with caplog.at_level("WARNING", logger=restore_retry.__name__):
        assert asyncio.run(restored(destination=DESTINATION)) == DESTINATION
    messages = [
        record.getMessage()
        for record in caplog.records
        if record.name == restore_retry.__name__
    ]
    assert len(messages) == 1
    message = messages[0]
    assert f"restore_retry path={DESTINATION}" in message
    assert "attempt=2/30" in message
    assert "error=ClientResponseError" in message
    assert "status=503" in message
    assert "SECRET_" not in caplog.text
    assert "Authorization" not in caplog.text
