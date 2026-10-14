"""Permit restore retries only after a confirmed transaction rollback."""

import copy

import pytest
from test_yt import (
    Client,
    Transaction,
    add_node,
    compressed,
    read_error,
    run,
)
from test_yt import adapter as adapter  # noqa: PLC0414 - re-export shared fixtures
from test_yt import store as store  # noqa: PLC0414 - re-export shared fixtures

from ytclone.format import RESTORE_MARKER
from ytclone.interfaces import RetryableDownloadError, RetryableRestoreError
from ytclone.yt import Restore


@pytest.mark.parametrize("kind", ["table", "file"])
def test_partial_write_retries_only_after_rollback(adapter, store, monkeypatch, kind):
    """Discard written bytes before a fresh transaction replaces the old target."""
    source = add_node(store=store, kind=kind)
    previous = add_node(store=store, kind=kind, name="restored")
    original = Client.write
    failure = read_error(name="timeout")
    attempts = 0

    def write(self, *, path, chunks):
        """Fail after the first write has already changed transactional data."""
        nonlocal attempts
        attempts += 1
        original(self, path=path, chunks=chunks)
        if attempts == 1:
            assert self.working[self.find(path)]["data"]
            raise failure

    monkeypatch.setattr(Client, "write", write)

    async def main():
        """Verify old data survives failure and the next attempt has no duplicates."""
        async with adapter.worker() as worker:
            async with worker.snapshot(path="//tmp/source") as snapshot:
                node = snapshot.node
            before = copy.deepcopy(store.nodes)

            with pytest.raises(RetryableRestoreError) as caught:
                async with worker.restore(
                    path="//tmp/restored", node=node, fingerprint="version"
                ) as sink:
                    await sink.write_part(
                        chunks=compressed(store.nodes[source]["data"])
                    )

            assert caught.value.__cause__ is failure
            assert store.nodes == before
            assert previous in store.nodes
            assert store.aborts == 1
            assert worker.session.transaction is None
            assert not worker.session.poisoned

            async with worker.restore(
                path="//tmp/restored", node=node, fingerprint="version"
            ) as sink:
                await sink.write_part(chunks=compressed(store.nodes[source]["data"]))

            target = next(
                record
                for record in store.nodes.values()
                if record["name"] == "restored"
            )
            assert target["data"] == store.nodes[source]["data"]
            assert RESTORE_MARKER in target["attrs"]
            assert previous not in store.nodes
            assert attempts == 2

    run(main())


@pytest.mark.parametrize("phase", ["prepare", "validate"])
def test_transient_restore_metadata_retries_after_abort(
    adapter, store, monkeypatch, phase
):
    """Only successfully aborted metadata failures can restart a restore."""
    source = add_node(store=store, kind="file")
    add_node(store=store, kind="file", name="restored")
    original = getattr(Restore, phase)
    failure = read_error(name="timeout")

    def fail(self):
        """Fail after the selected restore phase has mutated or checked the node."""
        original(self)
        raise failure

    monkeypatch.setattr(Restore, phase, fail)

    async def main():
        """Check eligibility only once all target mutations have been rolled back."""
        async with adapter.worker() as worker:
            async with worker.snapshot(path="//tmp/source") as snapshot:
                node = snapshot.node
            before = copy.deepcopy(store.nodes)

            with pytest.raises(RetryableRestoreError) as caught:
                async with worker.restore(
                    path="//tmp/restored", node=node, fingerprint="version"
                ) as sink:
                    await sink.write_part(
                        chunks=compressed(store.nodes[source]["data"])
                    )

            assert caught.value.__cause__ is failure
            assert store.aborts == 1
            assert store.nodes == before
            assert worker.session.transaction is None

    run(main())


def test_download_failure_retries_after_abort(adapter, store):
    """A temporary S3 stream failure becomes retryable after target rollback."""
    source = add_node(store=store, kind="file")
    add_node(store=store, kind="file", name="restored")
    failure = RetryableDownloadError("download disconnected")

    async def main():
        """Propagate the download signal through the synchronous writer bridge."""
        async with adapter.worker() as worker:
            async with worker.snapshot(path="//tmp/source") as snapshot:
                node = snapshot.node
            before = copy.deepcopy(store.nodes)

            with pytest.raises(RetryableRestoreError) as caught:
                async with worker.restore(
                    path="//tmp/restored", node=node, fingerprint="version"
                ) as sink:
                    await sink.write_part(
                        chunks=compressed(store.nodes[source]["data"], error=failure)
                    )

            assert caught.value.__cause__ is failure
            assert store.aborts == 1
            assert store.nodes == before

    run(main())


@pytest.mark.parametrize(
    "name",
    [
        "value",
        "zstd",
        "format",
        "access",
        "authentication",
        "http_403",
        "no_transaction",
        "cancel",
    ],
)
def test_nontransient_restore_failure_is_never_wrapped(adapter, store, name):
    """Integrity, access, snapshot and cancellation failures remain fatal."""
    add_node(store=store, kind="file")
    add_node(store=store, kind="file", name="restored")
    failure = read_error(name=name)

    async def main():
        """Keep the original exception even when rollback succeeds."""
        async with adapter.worker() as worker:
            async with worker.snapshot(path="//tmp/source") as snapshot:
                node = snapshot.node
            before = copy.deepcopy(store.nodes)

            with pytest.raises(type(failure)) as caught:
                async with worker.restore(
                    path="//tmp/restored", node=node, fingerprint="version"
                ):
                    raise failure

            assert caught.value is failure
            assert store.aborts == 1
            assert store.nodes == before

    run(main())


@pytest.mark.parametrize("download", [False, True])
def test_failed_abort_preserves_primary_failure_without_retry(
    adapter, store, monkeypatch, download
):
    """An uncertain abort must not authorize a second write transaction."""
    add_node(store=store, kind="file")
    add_node(store=store, kind="file", name="restored")
    failure = (
        RetryableDownloadError("download failed")
        if download
        else read_error(name="timeout")
    )
    cleanup_failure = RuntimeError("abort failed")
    original = Transaction.__exit__
    failed = False

    def exit(self, error_type, error, traceback):
        """Fail the first rollback; permit later worker disposal to clean up."""
        nonlocal failed
        if error is failure and not failed:
            failed = True
            raise cleanup_failure
        return original(self, error_type, error, traceback)

    monkeypatch.setattr(Transaction, "__exit__", exit)

    async def main():
        """Retain the source error while keeping the client poisoned."""
        async with adapter.worker() as worker:
            async with worker.snapshot(path="//tmp/source") as snapshot:
                node = snapshot.node
            before = copy.deepcopy(store.nodes)

            with pytest.raises(type(failure)) as caught:
                async with worker.restore(
                    path="//tmp/restored", node=node, fingerprint="version"
                ):
                    raise failure

            assert caught.value is failure
            assert caught.value.__cause__ is cleanup_failure
            assert not isinstance(caught.value, RetryableRestoreError)
            assert worker.session.poisoned
            assert worker.session.transaction is not None
            assert store.nodes == before

    run(main())


def test_begin_failure_never_authorizes_restore_retry(adapter, store, monkeypatch):
    """A transient failure while entering the transaction is outside the gate."""
    add_node(store=store, kind="file")
    failure = read_error(name="timeout")
    original = Transaction.__enter__
    fail_begin = False

    def enter(self):
        """Simulate a transaction begun on the server with its reply interrupted."""
        original(self)
        if fail_begin:
            raise failure

    monkeypatch.setattr(Transaction, "__enter__", enter)

    async def main():
        """Reject replay even if subsequent cleanup of the begin failure succeeds."""
        nonlocal fail_begin
        async with adapter.worker() as worker:
            async with worker.snapshot(path="//tmp/source") as snapshot:
                node = snapshot.node
            fail_begin = True

            with pytest.raises(type(failure)) as caught:
                async with worker.restore(
                    path="//tmp/restored", node=node, fingerprint="version"
                ):
                    pytest.fail("The restore body must not start")

            assert caught.value is failure
            assert worker.session.poisoned
            assert store.aborts == 1

    run(main())


def test_commit_lost_reply_never_authorizes_restore_retry(adapter, store, monkeypatch):
    """A commit that may already have applied must never replay the node."""
    source = add_node(store=store, kind="file")
    failure = read_error(name="timeout")
    original = Transaction.__exit__
    fail_commit = False

    def exit(self, error_type, error, traceback):
        """Commit data, then lose its successful server response."""
        original(self, error_type, error, traceback)
        if fail_commit and error_type is None:
            raise failure

    monkeypatch.setattr(Transaction, "__exit__", exit)

    async def main():
        """Preserve the committed target without publishing a completion marker."""
        nonlocal fail_commit
        async with adapter.worker() as worker:
            async with worker.snapshot(path="//tmp/source") as snapshot:
                node = snapshot.node
            fail_commit = True

            with pytest.raises(type(failure)) as caught:
                async with worker.restore(
                    path="//tmp/restored", node=node, fingerprint="version"
                ) as sink:
                    await sink.write_part(
                        chunks=compressed(store.nodes[source]["data"])
                    )

            assert caught.value is failure
            assert worker.session.poisoned
            target = next(
                record
                for record in store.nodes.values()
                if record["name"] == "restored"
            )
            assert target["data"] == store.nodes[source]["data"]
            assert RESTORE_MARKER not in target["attrs"]

    run(main())


def test_postcommit_marker_failure_is_not_retryable(adapter, store, monkeypatch):
    """A transient marker failure does not authorize replaying committed data."""
    source = add_node(store=store, kind="file")
    failure = read_error(name="timeout")
    original = Client.set

    def set_attribute(self, *, path, value):
        """Fail only the completion marker after the data transaction committed."""
        if path.endswith("/@" + RESTORE_MARKER):
            assert self.working is None
            raise failure
        return original(self, path=path, value=value)

    monkeypatch.setattr(Client, "set", set_attribute)

    async def main():
        """Keep committed bytes and propagate the original marker error."""
        async with adapter.worker() as worker:
            async with worker.snapshot(path="//tmp/source") as snapshot:
                node = snapshot.node

            with pytest.raises(type(failure)) as caught:
                async with worker.restore(
                    path="//tmp/restored", node=node, fingerprint="version"
                ) as sink:
                    await sink.write_part(
                        chunks=compressed(store.nodes[source]["data"])
                    )

            assert caught.value is failure
            assert worker.session.transaction is None
            assert not worker.session.poisoned
            assert store.aborts == 0
            target = next(
                record
                for record in store.nodes.values()
                if record["name"] == "restored"
            )
            assert target["data"] == store.nodes[source]["data"]
            assert RESTORE_MARKER not in target["attrs"]

    run(main())
