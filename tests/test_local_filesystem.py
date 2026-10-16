"""Opt-in CLI round trips using disposable localhost YT and a real local archive."""

import contextlib
import io
import os
import re
import signal
import subprocess
import sys
import uuid

import pytest
import yt.wrapper as sdk
from yt import yson

from ytclone.format import RESTORE_MARKER, ROW_FORMAT, Manifest

pytestmark = pytest.mark.skipif(
    os.environ.get("YTCLONE_LOCAL_TESTS") != "1",
    reason="Set YTCLONE_LOCAL_TESTS=1 to use disposable local YT",
)

YT_PROXY = "http://localhost:8000"
NODE_COUNT = 6
CLI_TIMEOUT = 180


def run_cli(*, module, source, destination, directory, name, extra=(), expected=0):
    """Run only against localhost without AWS credentials or the user's YT config."""
    log_file = directory / (name + ".log")
    config_file = directory / "yt-client.conf"
    config_file.write_text("{}")
    environment = {
        "PATH": os.defpath,
        "LANG": "C.UTF-8",
        "PYTHONUNBUFFERED": "1",
        "YT_CONFIG_PATH": str(config_file),
        "YT_TOKEN_PATH": str(directory / "unused-token"),
        "YT_CONFIG_PATCHES": "{enable_token=%false;proxy={retries={count=1;};};};",
    }
    if os.environ.get("PYTHONPATH"):
        environment["PYTHONPATH"] = os.environ["PYTHONPATH"]
    process = subprocess.Popen(
        args=[
            sys.executable,
            "-m",
            module,
            str(source),
            str(destination),
            "--yt-proxy",
            YT_PROXY,
            "--no-proxy-discovery",
            "--concurrency",
            "2",
            "--part-size-mib",
            "1",
            "--buffer-size-mib",
            "5",
            "--log-file",
            str(log_file),
            "--progress-interval",
            "1",
            *extra,
        ],
        cwd=directory,
        env=environment,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )
    timed_out = False
    try:
        output, _ = process.communicate(timeout=CLI_TIMEOUT)
    except BaseException as error:
        # The parallel supervisor forwards termination to its own workers.
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGTERM)
        try:
            output, _ = process.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
            output, _ = process.communicate(timeout=10)
        if not isinstance(error, subprocess.TimeoutExpired):
            raise
        timed_out = True
    if timed_out or process.returncode != expected:
        diagnostics = [output[-4000:]]
        for path in sorted(directory.glob(name + "*.log")):
            diagnostics.append(path.name + "\n" + path.read_text()[-4000:])
        pytest.fail(
            f"{name}: timeout={timed_out}, exit={process.returncode}\n"
            + "\n".join(diagnostics)
        )
    return output, log_file.read_text()


def assert_summary(*, output, copied, unchanged, errors=0):
    state = "failed" if errors else "finished"
    summaries = [line for line in output.splitlines() if f" {state} " in line]
    assert len(summaries) == 1, output
    fields = dict(re.findall(r"(\w+)=(\S+)", summaries[0]))
    assert fields["copied"] == str(copied), summaries[0]
    assert fields["unchanged"] == str(unchanged), summaries[0]
    assert fields["skipped"] == "0", summaries[0]
    assert fields["errors"] == str(errors), summaries[0]


def rows(*, client, path):
    with contextlib.closing(
        client.read_table(
            table=path,
            format=sdk.YsonFormat(attributes=ROW_FORMAT),
            raw=True,
        )
    ) as stream:
        return list(yson.load(stream=stream, yson_type="list_fragment", encoding=None))


@pytest.fixture
def local_tree():
    """Create and remove only this invocation's unique disposable YT subtree."""
    root = "//tmp/ytclone-filesystem-" + uuid.uuid4().hex[:12]
    source, target = root + "/source", root + "/restored"
    client = sdk.YtClient(
        proxy=YT_PROXY,
        config={
            "enable_token": False,
            "proxy": {"enable_proxy_discovery": False, "retries": {"count": 1}},
        },
    )
    schema = yson.to_yson_type(
        [
            {"name": "id", "type": "int64", "sort_order": "ascending"},
            {"name": "payload", "type": "string"},
            {"name": "u", "type": "uint64"},
        ],
        attributes={"strict": True, "unique_keys": True},
    )
    expected_rows = [
        {b"id": 1, b"payload": b"\x00\xff", b"u": yson.YsonUint64(2**64 - 1)},
        {b"id": 2, b"payload": None, b"u": None},
    ]
    payload = bytes(range(256)) * 8192
    try:
        client.create(
            type="map_node",
            path=source,
            recursive=True,
            attributes={"note": "source folder"},
        )
        client.create(
            type="map_node",
            path=source + "/empty",
            attributes={"note": "empty folder"},
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
                object=expected_rows, yson_type="list_fragment", yson_format="binary"
            ),
            format=sdk.YsonFormat(attributes=ROW_FORMAT),
            raw=True,
        )
        for name in ("file", "empty_file"):
            client.create(
                type="file",
                path=source + "/" + name,
                attributes={"file_name": name, "note": "file metadata"},
            )
        client.write_file(destination=source + "/file", stream=io.BytesIO(payload))
        yield client, source, target, expected_rows, payload
    finally:
        try:
            client.remove(path=root, recursive=True, force=True)
        finally:
            client._cleanup()


@pytest.mark.parametrize("module", ["ytclone", "ytclone.parallel"])
def test_filesystem_cli_round_trip_relocation_restart_and_corruption(
    tmp_path, local_tree, module
):
    client, source, target, expected_rows, payload = local_tree
    archive = tmp_path / "disk archive"
    extra = ("--processes", "2") if module == "ytclone.parallel" else ()
    for repeated in (False, True):
        output, logs = run_cli(
            module=module,
            source=source,
            destination=archive,
            directory=tmp_path,
            name="export-repeat" if repeated else "export",
            extra=extra,
        )
        assert_summary(
            output=output,
            copied=0 if repeated else NODE_COUNT,
            unchanged=NODE_COUNT if repeated else 0,
        )
        if module == "ytclone.parallel":
            workers = re.findall(r"worker_started worker=(\d+) pid=(\d+)", logs)
            assert {worker for worker, _ in workers} == {"1", "2"}, logs
            assert len({pid for _, pid in workers}) == 2, logs

    relocated = tmp_path / "relocated archive"
    archive.rename(relocated)
    selected = relocated / source.removeprefix("//")
    for repeated in (False, True):
        output, _ = run_cli(
            module="ytclone",
            source=selected,
            destination=target,
            directory=tmp_path,
            name="restore-repeat" if repeated else "restore",
        )
        assert_summary(
            output=output,
            copied=0 if repeated else NODE_COUNT,
            unchanged=NODE_COUNT if repeated else 0,
        )

    for name in ("table", "empty_table"):
        assert rows(client=client, path=target + "/" + name) == (
            expected_rows if name == "table" else []
        )
        for attribute in ("schema", "binary_metadata"):
            assert client.get(path=target + "/" + name + "/@" + attribute) == (
                client.get(path=source + "/" + name + "/@" + attribute)
            )
    with contextlib.closing(client.read_file(path=target + "/file")) as stream:
        assert stream.read() == payload
    assert client.get(path=target + "/file/@file_name") == "file"
    assert client.get(path=target + "/file/@note") == "file metadata"
    assert client.get(path=target + "/empty_file/@uncompressed_data_size") == 0
    assert client.get(path=target + "/@note") == "source folder"
    assert list(client.list(path=target + "/empty")) == []
    assert client.get(path=target + "/empty/@note") == "empty folder"

    manifest = Manifest.from_bytes((selected / "file/@manifest.json").read_bytes())
    object_path = relocated / manifest.parts[0].object.key
    original = object_path.read_bytes()
    object_path.write_bytes(bytes([original[0] ^ 255]) + original[1:])
    client.remove(path=target + "/file/@" + RESTORE_MARKER)
    previous_id = client.get(path=target + "/file/@id")
    output, _ = run_cli(
        module="ytclone",
        source=selected / "file",
        destination=target + "/file",
        directory=tmp_path,
        name="restore-corrupt",
        expected=1,
    )
    assert_summary(output=output, copied=0, unchanged=0, errors=1)
    assert client.get(path=target + "/file/@id") == previous_id
    with contextlib.closing(client.read_file(path=target + "/file")) as stream:
        assert stream.read() == payload
    object_path.write_bytes(original)
    output, _ = run_cli(
        module="ytclone",
        source=selected / "file",
        destination=target + "/file",
        directory=tmp_path,
        name="restore-repaired",
    )
    assert_summary(output=output, copied=1, unchanged=0)
