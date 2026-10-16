"""Exercise installed, Docker or portable CLI commands against local services."""

import contextlib
import io
import os
import re
import shutil
import signal
import subprocess
import sys
import uuid
from pathlib import Path

import pytest
import yt.wrapper as sdk
from yt import yson

from ytclone.format import ROW_FORMAT

pytestmark = pytest.mark.skipif(
    os.environ.get("YTCLONE_LOCAL_TESTS") != "1",
    reason="Set YTCLONE_LOCAL_TESTS=1 to use disposable local YT/S3",
)

YT_PROXY = "http://localhost:8000"
S3_ENDPOINT = "http://localhost:8333"
NODE_COUNT = 6
CLI_TIMEOUT = 180


def stop_container(*, docker, container, environment):
    """Stop only this invocation's container before stopping its Docker client."""
    for arguments, timeout in (
        (("stop", "--time", "30", container), 40),
        (("rm", "--force", container), 10),
    ):
        with contextlib.suppress(OSError, subprocess.TimeoutExpired):
            subprocess.run(
                args=[docker, *arguments],
                env=environment,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=timeout,
                check=False,
            )


def run_cli(*, module, source, destination, directory, name, extra=()):
    """Use a clean environment and bound both execution and cancellation."""
    log_file = directory / (name + ".log")
    config_file = directory / "yt-client.conf"
    config_file.write_text("{}")
    environment = {
        "PATH": os.defpath,
        "LANG": "C.UTF-8",
        "PYTHONUNBUFFERED": "1",
        "AWS_ACCESS_KEY_ID": "local-bench",
        "AWS_SECRET_ACCESS_KEY": "local-bench-secret",
        "AWS_EC2_METADATA_DISABLED": "true",
        "YT_CONFIG_PATH": str(config_file),
        "YT_TOKEN_PATH": str(directory / "unused-token"),
        "YT_CONFIG_PATCHES": "{enable_token=%false;proxy={retries={count=1;};};};",
    }
    arguments = [
        source,
        destination,
        "--yt-proxy",
        YT_PROXY,
        "--no-proxy-discovery",
        "--endpoint",
        S3_ENDPOINT,
        "--region",
        "us-east-1",
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
    ]
    command = [sys.executable, "-m", module, *arguments]
    image = os.environ.get("YTCLONE_DOCKER_IMAGE")
    portable = os.environ.get("YTCLONE_PORTABLE_DIR")
    assert not (image and portable), (
        "Choose either YTCLONE_DOCKER_IMAGE or YTCLONE_PORTABLE_DIR"
    )
    executable = "ytclone-parallel" if module == "ytclone.parallel" else "ytclone"
    if portable:
        binary = Path(portable).absolute() / executable
        assert binary.is_file() and os.access(binary, os.X_OK), binary
        command = [str(binary), *arguments]
    container = None
    if image:
        docker = shutil.which("docker")
        assert docker is not None, "YTCLONE_DOCKER_IMAGE requires Docker"
        container = "ytclone-cli-test-" + uuid.uuid4().hex
        command = [
            docker,
            "run",
            "--rm",
            "--name",
            container,
            "--network",
            "host",
            "--user",
            f"{os.getuid()}:{os.getgid()}",
            "--mount",
            f"type=bind,source={directory},target={directory}",
            "--workdir",
            str(directory),
        ]
        for key, value in environment.items():
            if key != "PATH":  # Preserve the image's virtualenv PATH.
                command.extend(("--env", key + "=" + value))
        command.extend((image, executable, *arguments))
        # Docker needs the user's context, but none of it enters the container.
        environment = {"PATH": os.defpath}
        if "HOME" in os.environ:
            environment["HOME"] = os.environ["HOME"]

    process = subprocess.Popen(
        args=command,
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
        if container is not None:
            stop_container(docker=docker, container=container, environment=environment)
        # The supervisor handles SIGTERM and stops its own worker sessions.
        # Never signal pytest's process group or unrelated local processes.
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

    if timed_out or process.returncode:
        diagnostics = [output[-4000:]]
        for path in sorted(directory.glob(name + "*.log")):
            diagnostics.append(path.name + "\n" + path.read_text()[-4000:])
        pytest.fail(
            f"{name}: timeout={timed_out}, exit={process.returncode}\n"
            + "\n".join(diagnostics)
        )
    return output, log_file.read_text()


def assert_summary(*, output, copied, unchanged):
    summaries = [line for line in output.splitlines() if " finished " in line]
    assert len(summaries) == 1, output
    fields = dict(re.findall(r"(\w+)=(\S+)", summaries[0]))
    assert fields["copied"] == str(copied), summaries[0]
    assert fields["unchanged"] == str(unchanged), summaries[0]
    assert fields["skipped"] == "0", summaries[0]
    assert fields["errors"] == "0", summaries[0]


def test_local_installed_parallel_export_and_cli_restore(tmp_path):
    run_id = uuid.uuid4().hex[:12]
    root = "//tmp/ytclone-cli-" + run_id
    source, destination = root + "/source", root + "/restored"
    archive = "s3://yt-backup/cli-smoke-" + run_id
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
        ],
        attributes={"strict": True, "unique_keys": True},
    )
    expected_rows = [{b"id": 1, b"payload": b"\x00\xff"}]
    payload = bytes(range(256)) * 8192
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
            attributes={"schema": schema, "note": "table metadata"},
        )
    client.write_table(
        table=source + "/table",
        input_stream=yson.dumps(
            object=expected_rows, yson_type="list_fragment", yson_format="binary"
        ),
        format=sdk.YsonFormat(attributes=ROW_FORMAT),
        raw=True,
    )
    client.create(
        type="file",
        path=source + "/file",
        attributes={"file_name": "payload.bin", "note": "file metadata"},
    )
    client.write_file(destination=source + "/file", stream=io.BytesIO(payload))

    document = {b"checkpoint": [yson.YsonUint64(2**64 - 1), b"\xff", None]}
    client.create(
        type="document",
        path=source + "/document",
        attributes={"value": document, "note": "document metadata"},
    )

    for repeated in (False, True):
        output, logs = run_cli(
            module="ytclone.parallel",
            source=source,
            destination=archive,
            directory=tmp_path,
            name="export-repeat" if repeated else "export",
            extra=("--processes", "2", "--discovery-concurrency", "2"),
        )
        assert_summary(
            output=output,
            copied=0 if repeated else NODE_COUNT,
            unchanged=NODE_COUNT if repeated else 0,
        )
        workers = re.findall(r"worker_started worker=(\d+) pid=(\d+)", logs)
        assert {worker for worker, _ in workers} == {"1", "2"}, logs
        assert len({pid for _, pid in workers}) == 2, logs

        output, _ = run_cli(
            module="ytclone",
            source=archive + "/" + source.removeprefix("//"),
            destination=destination,
            directory=tmp_path,
            name="restore-repeat" if repeated else "restore",
        )
        assert_summary(
            output=output,
            copied=0 if repeated else NODE_COUNT,
            unchanged=NODE_COUNT if repeated else 0,
        )

    assert client.get(path=destination + "/document/@type") == "document"
    restored_document = yson.loads(
        client.get(
            path=destination + "/document/@value",
            format=sdk.YsonFormat(attributes={"format": "binary"}),
        ),
        encoding=None,
    )
    assert yson.dumps(
        restored_document, yson_format="binary", sort_keys=True
    ) == yson.dumps(document, yson_format="binary", sort_keys=True)
    assert client.get(path=destination + "/document/@note") == "document metadata"

    for name in ("table", "empty_table"):
        with contextlib.closing(
            client.read_table(
                table=destination + "/" + name,
                format=sdk.YsonFormat(attributes=ROW_FORMAT),
                raw=True,
            )
        ) as stream:
            restored = list(
                yson.load(stream=stream, yson_type="list_fragment", encoding=None)
            )
        assert restored == (expected_rows if name == "table" else [])
        assert client.get(path=destination + "/" + name + "/@schema") == client.get(
            path=source + "/" + name + "/@schema"
        )
        assert client.get(path=destination + "/" + name + "/@note") == "table metadata"
    with contextlib.closing(client.read_file(path=destination + "/file")) as stream:
        assert stream.read() == payload
    assert client.get(path=destination + "/file/@file_name") == "payload.bin"
    assert client.get(path=destination + "/file/@note") == "file metadata"
    assert client.get(path=destination + "/@note") == "source folder"
    assert client.get(path=destination + "/empty/@type") == "map_node"
    assert list(client.list(path=destination + "/empty")) == []
    assert client.get(path=destination + "/empty/@note") == "empty folder"
