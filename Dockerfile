FROM python:3.12-slim@sha256:a6e34c598f2467ed0e9a8d349809fcd8b5c603269512df273a0bb1784edc11b1 AS runtime

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install --no-install-recommends -y libstdc++6 tini \
    && rm -rf /var/lib/apt/lists/*

FROM runtime AS builder
COPY --from=ghcr.io/astral-sh/uv:0.12.19@sha256:04d046b13e60d6bcec73cbc5e1cad25d680dea90c8573340950a0ac2d1aef424 /uv /usr/local/bin/uv
WORKDIR /build
COPY pyproject.toml uv.lock ./
RUN uv export --locked --no-dev --no-emit-project --no-python-downloads > requirements.txt \
    && uv venv --python /usr/local/bin/python /opt/venv \
    && uv pip install --python /opt/venv/bin/python --require-hashes --no-build --no-cache -r requirements.txt
COPY dist/*.whl /dist/
RUN uv pip install --python /opt/venv/bin/python --no-deps --no-cache /dist/*.whl \
    && uv pip check --python /opt/venv/bin/python

FROM runtime
ARG VERSION=0.1.0
ARG REVISION=unknown
LABEL org.opencontainers.image.title="ytclone" \
      org.opencontainers.image.description="Resumable copying between YTsaurus and S3" \
      org.opencontainers.image.source="https://github.com/alvassin/ytclone" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.version="$VERSION" \
      org.opencontainers.image.revision="$REVISION"
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
COPY --from=builder /opt/venv /opt/venv
RUN python -c 'import importlib.metadata, sys; assert importlib.metadata.version("ytclone") == sys.argv[1]' "$VERSION" \
    && useradd --create-home --uid 10001 --user-group --shell /usr/sbin/nologin ytclone \
    && mkdir /data && chown ytclone:ytclone /data
USER 10001:10001
WORKDIR /data
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["ytclone", "--help"]
