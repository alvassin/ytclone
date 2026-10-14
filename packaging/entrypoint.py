"""Dispatch the two portable commands from one shared executable."""

import json
import os
import platform
import ssl
import sys
from importlib.metadata import version
from pathlib import Path

from yt.packages.requests.certs import where as certificate_path


def configure_certificates() -> None:
    """Use bundled roots when the target has neither custom nor system roots."""
    if (
        not any(os.environ.get(key) for key in ("SSL_CERT_FILE", "SSL_CERT_DIR"))
        and not ssl.create_default_context().cert_store_stats()["x509_ca"]
    ):
        os.environ["SSL_CERT_FILE"] = certificate_path()


def main() -> int:
    """Select the public command or the supervisor's private worker entry."""
    configure_certificates()
    command = Path(sys.argv[0]).name
    if sys.argv[1:] == ["--version"]:
        print(f"{command} {version('ytclone')}")
        return 0
    if sys.argv[1:] == ["--portable-info"]:
        import zstandard
        from yt import yson

        payload = b"ytclone portable compression check"
        compressed = zstandard.ZstdCompressor().compress(payload)
        print(
            json.dumps(
                {
                    "version": version("ytclone"),
                    "python": platform.python_version(),
                    "frozen": bool(getattr(sys, "frozen", False)),
                    "yson": yson.TYPE,
                    "compression_roundtrip": zstandard.ZstdDecompressor().decompress(
                        compressed
                    )
                    == payload,
                    "tls_ca_count": ssl.create_default_context().cert_store_stats()[
                        "x509_ca"
                    ],
                    "aiomisc_version": version("aiomisc"),
                }
            )
        )
        return 0
    if sys.argv[1:] == ["--parallel-worker"]:
        from ytclone.parallel.worker import main as worker_main

        return worker_main()
    if command == "ytclone-parallel":
        from ytclone.parallel.cli import main as parallel_main

        return parallel_main()
    from ytclone import main as copy_main

    return copy_main()


if __name__ == "__main__":
    raise SystemExit(main())
