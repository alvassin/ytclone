"""Export exclusions prune discovery before any subtree reads or uploads."""

import asyncio
import sys

import pytest
from test_engine import MemoryStorage, MemoryYt, make_node
from test_parallel_discovery import FakeAdapter, FakeWorker, node

from ytclone import cli
from ytclone.engine import CopyOptions, export_tree
from ytclone.exclusions import is_excluded, normalize_excludes
from ytclone.parallel import cli as parallel_cli
from ytclone.parallel.discovery import discover
from ytclone.yt import YtWorker


def test_normalization_and_component_boundaries():
    excludes = normalize_excludes(
        source="//root",
        paths=("tmp/", "//root/nested/cache/", "tmp", "missing"),
    )
    assert excludes == ("//root/tmp", "//root/nested/cache", "//root/missing")
    for path in ("//root/tmp", "//root/tmp/deep/file", "//root/nested/cache/file"):
        assert is_excluded(path=path, excludes=excludes)
    for path in ("//root", "//root/tmp_old", "//root/nested/tmp"):
        assert not is_excluded(path=path, excludes=excludes)


@pytest.mark.parametrize(
    "path",
    [
        "",
        "/",
        "//",
        "/tmp",
        ".",
        "..",
        "a/../b",
        "a//b",
        "//root",
        "//other/a",
        "//root_old/a",
        "tmp/*",
        "tmp?",
        "tmp/@attr",
        "tmp&",
        "tmp[0]",
    ],
)
def test_invalid_exclusions_fail_before_clients_start(path, monkeypatch):
    for module in (cli, parallel_cli):
        monkeypatch.setattr(
            module.aiomisc, "run", lambda *a, **k: pytest.fail("Started resources")
        )
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "ytclone",
                "//root",
                "s3://bucket/export",
                "--yt-proxy",
                "yt.invalid",
                "--exclude",
                path,
            ],
        )
        with pytest.raises(SystemExit) as error:
            module.main()
        assert error.value.code == 2


def test_cli_rejects_exclusions_on_restore(monkeypatch):
    monkeypatch.setattr(
        cli.aiomisc, "run", lambda *a, **k: pytest.fail("Started resources")
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "ytclone",
            "s3://bucket/export",
            "//root",
            "--yt-proxy",
            "yt.invalid",
            "--exclude",
            "tmp",
        ],
    )
    with pytest.raises(SystemExit) as error:
        cli.main()
    assert error.value.code == 2


@pytest.mark.parametrize("module", [cli, parallel_cli])
def test_cli_passes_repeated_exclusions(module, monkeypatch):
    async def run(*, arguments, options):
        assert options.exclude == ("//root/tmp", "//root/nested/cache")
        return 0

    monkeypatch.setattr(module, "run", run)
    monkeypatch.setattr(module.aiomisc, "run", lambda coro, **kw: asyncio.run(coro))
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "ytclone",
            "//root",
            "s3://bucket/export",
            "--yt-proxy",
            "yt.invalid",
            "--exclude",
            "tmp/",
            "--exclude",
            "//root/nested/cache",
        ],
    )
    assert module.main() == 0


@pytest.mark.parametrize("parallel", [False, True])
def test_excluded_directories_are_never_listed(parallel):
    async def main():
        adapter = FakeAdapter(
            tree={
                "//root": [
                    node(path="//root/tmp"),
                    node(path="//root/tmp_old"),
                    node(path="//root/nested"),
                ],
                "//root/tmp_old": [node(path="//root/tmp_old/file", kind="file")],
                "//root/nested": [
                    node(path="//root/nested/cache"),
                    node(path="//root/nested/tmp", kind="table"),
                ],
            }
        )
        observed = []
        excludes = ("tmp/", "//root/nested/cache", "missing")
        if parallel:

            async def emit(entry):
                observed.append(entry.path)

            await discover(
                yt=adapter, source="//root", concurrency=3, emit=emit, exclude=excludes
            )
        else:
            worker = FakeWorker(adapter=adapter)
            observed = [
                entry.path
                async for entry in YtWorker.walk(
                    worker, path="//root", exclude=excludes
                )
            ]
        assert set(observed) == {
            "//root",
            "//root/tmp_old",
            "//root/tmp_old/file",
            "//root/nested",
            "//root/nested/tmp",
        }
        assert set(adapter.listed) == {"//root", "//root/tmp_old", "//root/nested"}

    asyncio.run(main())


def test_export_keeps_existing_excluded_archive_untouched():
    async def main():
        events = []
        yt = MemoryYt(
            nodes=[
                make_node(path="//root", kind="map_node"),
                make_node(path="//root/tmp", kind="map_node"),
                make_node(path="//root/tmp/file"),
                make_node(path="//root/tmp_old"),
            ],
            events=events,
        )
        storage = MemoryStorage(events=events)
        kwargs = {
            "yt": yt,
            "storage": storage,
            "source": "//root",
            "prefix": "archive",
            "cluster": "test",
        }
        initial = await export_tree(**kwargs, options=CopyOptions())
        assert initial.completed == 4
        saved = dict(storage.objects)
        storage.reads.clear()
        events.clear()
        result = await export_tree(**kwargs, options=CopyOptions(exclude=("tmp",)))
        assert result.failed == 0
        assert result.skipped == 2
        assert storage.objects == saved
        assert not any(key.startswith("archive/root/tmp/") for key in storage.reads)
        assert not any(
            path.startswith("//root/tmp/") or path == "//root/tmp"
            for kind, path in events
            if kind == "snapshot_enter"
        )

    asyncio.run(main())


def test_parallel_run_passes_exclusions_to_discovery(monkeypatch):
    args = parallel_cli.Arguments()
    args.parse_args(
        ["//root", "s3://bucket/export", "--yt-proxy", "yt.invalid", "--exclude", "tmp"]
    )
    monkeypatch.setattr(parallel_cli, "YtAdapter", lambda **kw: object())
    calls = []

    async def scan(**kwargs):
        calls.append(kwargs["exclude"])

    async def supervise(**kwargs):
        await kwargs["discover"](emit=None)

    monkeypatch.setattr(parallel_cli, "discover", scan)
    monkeypatch.setattr(parallel_cli, "supervise", supervise)
    assert (
        asyncio.run(
            parallel_cli.run(arguments=args, options=parallel_cli.validate(args))
        )
        == 0
    )
    assert calls == [("//root/tmp",)]
