"""Check a portable bundle without importing its packages into the host Python."""

import argparse
import asyncio
import contextlib
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

STARTUP_TIMEOUT = 60
SHUTDOWN_TIMEOUT = 10
KNOWN_SHUTDOWN_ERROR = (
    "aiomisc.entrypoint:Shutdown did not happen in 60.0 seconds, aborting."
)


def environment(directory: Path) -> dict[str, str]:
    """Exclude host Python paths, credentials and user configuration."""
    config = directory / "yt-client.conf"
    config.write_text("{}")
    return {
        "PATH": os.defpath,
        "HOME": str(directory),
        "LANG": "C.UTF-8",
        "AWS_ACCESS_KEY_ID": "portable-check",
        "AWS_SECRET_ACCESS_KEY": "portable-check-secret",
        "AWS_EC2_METADATA_DISABLED": "true",
        "YT_CONFIG_PATH": str(config),
        "YT_TOKEN_PATH": str(directory / "unused-token"),
        "YT_CONFIG_PATCHES": "{enable_token=%false;proxy={retries={count=1;};};};",
    }


def command(*, binary: Path, argument: str, directory: Path, env: dict) -> str:
    """Run a standalone executable from outside its own directory."""
    result = subprocess.run(
        args=[str(binary), argument],
        cwd=directory,
        env=env,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=STARTUP_TIMEOUT,
        check=True,
    )
    if result.stderr:
        raise RuntimeError(f"{binary.name} {argument}: {result.stderr}")
    return result.stdout


def inspect_binary(*, binary: Path, version: str, directory: Path, env: dict) -> dict:
    """Check command routing, frozen imports, native codecs and TLS certificates."""
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise RuntimeError(f"Missing executable: {binary}")
    output = command(binary=binary, argument="--help", directory=directory, env=env)
    required = ["usage:", "--yt-proxy", "--endpoint"]
    if binary.name == "ytclone-parallel":
        required.append("--processes")
    if not all(option in output for option in required):
        raise RuntimeError(f"Unexpected {binary.name} help: {output}")
    output = command(binary=binary, argument="--version", directory=directory, env=env)
    if output.strip() != f"{binary.name} {version}":
        raise RuntimeError(f"Unexpected {binary.name} version: {output}")
    info = json.loads(
        command(binary=binary, argument="--portable-info", directory=directory, env=env)
    )
    if not (
        info["version"] == version
        and info["frozen"] is True
        and info["yson"] == "BINARY"
        and info["compression_roundtrip"] is True
        and type(info["tls_ca_count"]) is int
        and info["tls_ca_count"] > 0
        and isinstance(info["python"], str)
        and info["python"].startswith("3.")
        and isinstance(info["aiomisc_version"], str)
    ):
        raise RuntimeError(f"Incomplete portable runtime: {info}")
    return info


def worker_config() -> dict:
    """Construct clients without jobs or network access; use only loopback URLs."""
    return {
        "yt_proxy": "http://127.0.0.1:1",
        "proxy_discovery": False,
        "endpoint": "http://127.0.0.1:1",
        "region": "us-east-1",
        "bucket": "portable-check",
        "prefix": "check",
        "concurrency": 2,
        "part_size": 1024**2,
        "chunk_size": 5 * 1024**2,
        "compression_level": 3,
        "progress_interval": 60,
        "log_file": None,
        "log_level": "info",
        "log_format": "stream",
    }


def protocol(output: bytes) -> list[dict]:
    """Reject logging, truncation and unexpected output on the private pipe."""
    if output and not output.endswith(b"\n"):
        raise RuntimeError(f"Truncated worker output: {output!r}")
    messages = [json.loads(line) for line in output.splitlines()]
    if messages != [{"type": "ready"}]:
        raise RuntimeError(f"Unexpected worker protocol: {messages}")
    return messages


async def check_worker(
    *, binary: Path, mode: str, directory: Path, env: dict, aiomisc_version: str
) -> dict:
    """Exercise real worker startup, idle shutdown and parent disappearance."""
    process = await asyncio.create_subprocess_exec(
        str(binary),
        "--parallel-worker",
        cwd=directory,
        env=env,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    errors = asyncio.create_task(process.stderr.read())
    remaining = None
    try:
        process.stdin.write(
            (json.dumps({"type": "config", "config": worker_config()}) + "\n").encode()
        )
        await process.stdin.drain()
        ready = await asyncio.wait_for(
            process.stdout.readline(), timeout=STARTUP_TIMEOUT
        )
        if ready != b'{"type": "ready"}\n':
            raise RuntimeError(f"{mode}: worker did not become ready: {ready!r}")
        remaining = asyncio.create_task(process.stdout.read())
        started = time.monotonic()
        if mode == "stop":
            process.stdin.write(b'{"type": "stop"}\n')
            await process.stdin.drain()
        elif mode == "eof":
            process.stdin.close()
            await process.stdin.wait_closed()
        else:
            process.send_signal(getattr(signal, mode))
        # Keep stdin open until signal handling finishes. communicate() would close
        # it immediately and could accidentally exercise EOF instead of the signal.
        await asyncio.wait_for(process.wait(), timeout=SHUTDOWN_TIMEOUT)
        elapsed = time.monotonic() - started
        stdout = ready + await remaining
        stderr = (await errors).decode(errors="replace")
        protocol(stdout)
        if any(
            marker in stderr
            for marker in ("Traceback", "Unclosed", "ResourceWarning", "never awaited")
        ):
            raise RuntimeError(f"{mode}: worker cleanup diagnostics: {stderr}")
        expected = {"stop": 0, "eof": 1}.get(mode, 130)
        known_error = (
            mode in ("SIGINT", "SIGTERM")
            and aiomisc_version == "18.0.33"
            and process.returncode == 70
            and KNOWN_SHUTDOWN_ERROR in stderr
        )
        if process.returncode != expected and not known_error:
            raise RuntimeError(f"{mode}: exit={process.returncode}: {stderr}")
        return {
            "mode": mode,
            "exit": process.returncode,
            "seconds": round(elapsed, 3),
            "known_aiomisc_shutdown_error": known_error,
        }
    finally:
        if process.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
            await process.wait()
        if remaining is not None:
            await remaining
        stderr = (await errors).decode(errors="replace")
        if process.stdin is not None:
            process.stdin.close()
        if process.returncode not in (0, 1, 70, 130) and stderr:
            print(stderr, end="", file=sys.stderr)


def main() -> None:
    """Validate unpacked release contents using only the host standard library."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", required=True, type=Path)
    parser.add_argument("--version", required=True)
    arguments = parser.parse_args()
    bundle = arguments.bundle.absolute()
    with tempfile.TemporaryDirectory(prefix="ytclone-portable-check-") as temporary:
        directory = Path(temporary)
        env = environment(directory)
        runtimes = {
            name: inspect_binary(
                binary=bundle / name,
                version=arguments.version,
                directory=directory,
                env=env,
            )
            for name in ("ytclone", "ytclone-parallel")
        }
        results = []
        for mode in ("stop", "eof", "SIGINT", "SIGTERM"):
            results.append(
                asyncio.run(
                    check_worker(
                        binary=bundle / "ytclone-parallel",
                        mode=mode,
                        directory=directory,
                        env=env,
                        aiomisc_version=runtimes["ytclone-parallel"]["aiomisc_version"],
                    )
                )
            )
    print(json.dumps({"runtime": runtimes, "worker_checks": results}, indent=2))


if __name__ == "__main__":
    main()
