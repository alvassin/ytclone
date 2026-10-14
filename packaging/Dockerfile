# Build tools stay in this image; users receive only the portable archive.
FROM ghcr.io/astral-sh/uv:0.12.19@sha256:04d046b13e60d6bcec73cbc5e1cad25d680dea90c8573340950a0ac2d1aef424 AS uv
FROM quay.io/pypa/manylinux_2_28_x86_64@sha256:4131ddd7acb9c6c966af02fe7e71edc51e078c2d41f475966697676430bfa2ae
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_PYTHON_INSTALL_DIR=/opt/portable-python UV_PYTHON_PREFERENCE=only-managed
# manylinux's own interpreters are static; PyInstaller needs libpython.so.
RUN uv python install 3.14.7
