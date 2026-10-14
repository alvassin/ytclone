# Build from the wheel installed in the isolated builder environment.
import sys
from pathlib import Path

from PyInstaller.utils.hooks import (
    collect_data_files,
    collect_entry_point,
    copy_metadata,
)

plugin_data, plugin_imports = collect_entry_point("aiomisc")

analysis = Analysis(
    [str(Path(SPECPATH) / "entrypoint.py")],
    datas=plugin_data
    + copy_metadata("ytclone", recursive=True)
    + collect_data_files("yt.packages.requests", includes=["cacert.pem"]),
    hiddenimports=["yt_yson_bindings.yson_lib", *plugin_imports],
    excludes=["pytest", "PyInstaller", "setuptools", "pip"],
    noarchive=False,
)
pyz = PYZ(analysis.pure)
executable = EXE(
    pyz,
    analysis.scripts,
    [],
    exclude_binaries=True,
    name="ytclone",
    console=True,
    strip=False,
    upx=False,
    target_arch="arm64" if sys.platform == "darwin" else None,
)
COLLECT(
    executable,
    analysis.binaries,
    analysis.datas,
    strip=False,
    upx=False,
    name="ytclone",
)
