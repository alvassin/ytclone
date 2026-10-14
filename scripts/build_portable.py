"""Bundle an installed wheel into a versioned archive for the current platform."""

import argparse
import hashlib
import os
import platform
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
from importlib.metadata import version
from pathlib import Path

PLATFORMS = {("Linux", "x86_64"): "linux-x86_64", ("Darwin", "arm64"): "macos-arm64"}
VERSION_PATTERN = re.compile(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)")


def build(*, expected_version: str, output_dir: Path) -> Path:
    """Freeze the installed wheel and archive the shared dependency directory."""
    target = PLATFORMS.get((platform.system(), platform.machine()))
    if target is None:
        raise ValueError("Portable builds support Linux x86_64 and macOS ARM64")
    if VERSION_PATTERN.fullmatch(expected_version) is None:
        raise ValueError("Expected a stable SemVer version")
    if version("ytclone") != expected_version:
        raise ValueError("Installed wheel version differs from requested version")
    if sys.version_info[:2] != (3, 14):
        raise ValueError("Build portable archives with Python 3.14")
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    name = f"ytclone-{expected_version}-{target}"
    archive = output_dir / f"{name}.tar.gz"
    if archive.exists() or archive.with_suffix(".gz.sha256").exists():
        raise FileExistsError(f"Refusing to overwrite an existing release: {archive}")
    root = Path(__file__).resolve().parents[1]
    with tempfile.TemporaryDirectory(prefix="ytclone-portable-") as temporary:
        work = Path(temporary)
        environment = dict(os.environ)
        environment["PYINSTALLER_CONFIG_DIR"] = str(work / "cache")
        subprocess.run(
            args=[
                sys.executable,
                "-m",
                "PyInstaller",
                "--noconfirm",
                "--clean",
                "--distpath",
                str(work / "dist"),
                "--workpath",
                str(work / "build"),
                str(root / "packaging" / "ytclone.spec"),
            ],
            cwd=work,
            env=environment,
            check=True,
        )
        bundle = work / "dist" / "ytclone"
        (bundle / "ytclone-parallel").symlink_to("ytclone")
        for filename in ("README.md", "USAGE.md", "LICENSE"):
            shutil.copy2(src=root / filename, dst=bundle / filename)
        (bundle / "VERSION").write_text(expected_version + "\n")
        with tarfile.open(
            name=archive, mode="w:gz", format=tarfile.PAX_FORMAT
        ) as output:
            output.add(name=bundle, arcname=name)
    digest = hashlib.sha256()
    with archive.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    archive.with_suffix(".gz.sha256").write_text(
        f"{digest.hexdigest()}  {archive.name}\n"
    )
    return archive


def main() -> None:
    """Build one current-platform archive without changing the source checkout."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    archive = build(expected_version=args.version, output_dir=args.output_dir)
    print(f"Created {archive} ({archive.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
