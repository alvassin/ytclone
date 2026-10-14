"""Check credential forwarding through the real SDK without network access."""

import asyncio

import pytest
from yt.wrapper import http_helpers

from ytclone.yt import YtAdapter


@pytest.mark.parametrize("token", ["dummy-env-token", "", None])
def test_credentials_from_environment(monkeypatch, tmp_path, token):
    """Prefer the exported token, then use the configured token file."""
    token_file = tmp_path / "token"
    token_file.write_text("dummy-file-token")
    token_file.chmod(0o600)
    monkeypatch.setenv("YT_TOKEN_PATH", str(token_file))
    if token is None:
        monkeypatch.delenv("YT_TOKEN", raising=False)
    else:
        monkeypatch.setenv("YT_TOKEN", token)

    async def check():
        """Resolve credentials on the same thread as normal SDK requests."""
        adapter = YtAdapter(proxy="http://127.0.0.1:1")
        async with adapter.worker() as worker:
            actual = await worker.session.call(
                http_helpers.get_token, client=worker.session.client
            )
            assert actual == (token or "dummy-file-token")

    asyncio.run(check())


@pytest.mark.parametrize("file_token", ["dummy-default-file-token", None])
def test_default_token_file_fallback(monkeypatch, file_token):
    """Keep SDK token-file fallback and anonymous local clusters working."""
    monkeypatch.delenv("YT_TOKEN", raising=False)
    monkeypatch.delenv("YT_TOKEN_PATH", raising=False)

    def read_token_file(client):
        """Replace the user's token file with an isolated test value."""
        assert client.config["token"] is None
        assert client.config["token_path"] is None
        return file_token

    monkeypatch.setattr(http_helpers, "_get_token_from_file", read_token_file)
    monkeypatch.setattr(http_helpers, "RECEIVE_TOKEN_FROM_SSH_SESSION", False)

    async def check():
        """Resolve the fallback without contacting SSH or an HTTP proxy."""
        adapter = YtAdapter(proxy="http://127.0.0.1:1")
        async with adapter.worker() as worker:
            actual = await worker.session.call(
                http_helpers.get_token, client=worker.session.client
            )
            assert actual == file_token

    asyncio.run(check())
