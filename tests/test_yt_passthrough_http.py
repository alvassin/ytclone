"""Exercise the SDK's real HTTP decoding and trailers without opening sockets."""

import gzip
import http.client
import io
import json
import struct
from collections import deque
from urllib.parse import urlsplit

import pytest
import yt.wrapper as yt
import zstandard
from yt.packages.requests.adapters import HTTPAdapter
from yt.packages.urllib3.response import HTTPResponse
from yt.wrapper.driver import make_request
from yt.wrapper.errors import YtError, YtRequestTimedOut
from yt.wrapper.http_helpers import _get_session

from ytclone.format import Range
from ytclone.yt import _is_retryable_read, _read_table
from ytclone.yt_zstd import ZstdBlockReader, _codec_id

CODEC = "z-zstd_3"
TRANSACTION = "1-2-3-4"


class _Socket:
    """Provide HTTPResponse with a file containing a complete wire response."""

    def __init__(self, wire):
        self.file = io.BytesIO(wire)

    def makefile(self, *args, **kwargs):
        return self.file


class _HTTP:
    def __init__(self):
        self.replies = deque()
        self.requests = []
        self.responses = []
        self.closed_responses = []

    def enqueue(self, body, *, headers=None, trailers=None, status=200):
        self.replies.append((body, headers or {}, trailers or {}, status))

    def send(self, adapter, request, **kwargs):
        self.requests.append(request)
        path = urlsplit(request.url).path
        if path == "/api/v4":
            body = json.dumps(
                [
                    {
                        "name": name,
                        "input_type": None,
                        "output_type": output,
                        "is_volatile": False,
                        "is_heavy": name != "get",
                    }
                    for name, output in (
                        ("read_table", "tabular"),
                        ("read_file", "binary"),
                        ("get", "structured"),
                    )
                ]
            ).encode()
            headers, trailers, status = {}, {}, 200
        else:
            assert path in ("/api/v4/read_table", "/api/v4/read_file", "/api/v4/get")
            body, headers, trailers, status = self.replies.popleft()

        wire = bytearray(
            f"HTTP/1.1 {status} OK\r\nTransfer-Encoding: chunked\r\n".encode()
        )
        for name, value in headers.items():
            wire.extend(f"{name}: {value}\r\n".encode())
        wire.extend(b"\r\n")
        # HTTP chunks intentionally split codec headers and Zstd frames.
        for offset in range(0, len(body), 3):
            chunk = body[offset : offset + 3]
            wire.extend(f"{len(chunk):x}\r\n".encode() + chunk + b"\r\n")
        wire.extend(b"0\r\n")
        for name, value in trailers.items():
            wire.extend(f"{name}: {value}\r\n".encode())
        wire.extend(b"\r\n")
        socket = _Socket(bytes(wire))
        underlying = http.client.HTTPResponse(socket, method=request.method)
        underlying.begin()
        raw = HTTPResponse.from_httplib(underlying, preload_content=False)
        response = adapter.build_response(request, raw)
        original_close = response.close

        def close():
            self.closed_responses.append(response)
            original_close()

        response.close = close
        self.responses.append((path, response, socket))
        return response

    @property
    def table_response(self):
        return next(
            response
            for path, response, _ in reversed(self.responses)
            if path == "/api/v4/read_table"
        )

    @property
    def table_socket(self):
        return next(
            socket
            for path, _, socket in reversed(self.responses)
            if path == "/api/v4/read_table"
        )


@pytest.fixture
def http_sdk(monkeypatch):
    http = _HTTP()

    def send(adapter, request, **kwargs):
        return http.send(adapter, request, **kwargs)

    monkeypatch.setattr(HTTPAdapter, "send", send)
    client = yt.YtClient(
        proxy="http://yt.invalid",
        config={
            "token": "dummy-http-test-token",
            "api_version": "v4",
            "apply_remote_patch_at_start": None,
            "read_buffer_size": 7,
            "proxy": {
                "accept_encoding": "gzip",
                "header_format": "json",
                "enable_proxy_discovery": False,
                "commands_with_framing": ["read_table"],
            },
        },
    )
    yield http, client
    # Also close a response when a failing assertion interrupts its consumer.
    for _, response, _ in http.responses:
        response.close()


def _open(client):
    return _read_table(
        client=client,
        pinned="#a-b-c-d",
        part=Range(index=0, start=0, stop=2),
        transaction_id=TRANSACTION,
        encoding=CODEC,
    )


def _transport(*chunks):
    frames = []
    records = []
    for chunk in (*chunks, b""):
        frame = zstandard.ZstdCompressor(level=3).compress(chunk)
        frames.append(frame)
        records.append(
            struct.pack("<HQQ", _codec_id(CODEC), len(frame) + 8, len(chunk)) + frame
        )
    return b"".join(records), frames


def test_sdk_preserves_encoded_bytes_and_scopes_request_config(http_sdk):
    http, client = http_sdk
    body, _ = _transport(b'{"a"=1;};', b'{"a"=2;};')
    http.enqueue(body, headers={"Content-Encoding": CODEC})
    original = client.config
    framing_commands = list(original["proxy"]["commands_with_framing"])
    session = _get_session(client)
    original_send = session.send
    raw = _open(client)
    try:
        assert client.config is original
        assert session.send == original_send
        assert raw.read() == body
        assert raw.closed
        assert http.table_response.raw.trailers() == {}
        request = next(
            request
            for request in http.requests
            if urlsplit(request.url).path == "/api/v4/read_table"
        )
        assert request.headers["Accept-Encoding"] == CODEC
        assert "X-YT-Accept-Framing" not in request.headers
        assert request.headers["Authorization"] == "OAuth dummy-http-test-token"
        assert json.loads(request.headers["X-YT-Parameters"])["transaction_id"] == (
            TRANSACTION
        )
        assert all(
            CODEC not in request.headers.get("Accept-Encoding", "")
            for request in http.requests
            if urlsplit(request.url).path == "/api/v4"
        )
    finally:
        raw.close()

    for command in ("get", "read_file"):
        http.enqueue(gzip.compress(b"ordinary"), headers={"Content-Encoding": "gzip"})
        result = make_request(
            command_name=command,
            params={"path": "//test"},
            return_content=True,
            allow_retries=False,
            client=client,
        )
        assert result == b"ordinary"
        assert http.requests[-1].headers["Accept-Encoding"] == "gzip"
    assert client.config is original
    assert session.send == original_send
    assert original["proxy"]["commands_with_framing"] == framing_commands


def test_block_reader_reaches_http_eof_after_terminator(http_sdk):
    http, client = http_sdk
    body, frames = _transport(b'{"a"=1;};', b'{"a"=2;};')
    http.enqueue(body, headers={"Content-Encoding": CODEC})
    raw = _open(client)
    try:
        reader = ZstdBlockReader(raw, codec=CODEC)
        encoded = b"".join(iter(lambda: reader.read(5), b""))
        assert encoded == b"".join(frames[:-1])
        with zstandard.ZstdDecompressor().stream_reader(io.BytesIO(encoded)) as decoded:
            assert decoded.read() == b'{"a"=1;};{"a"=2;};'
        assert raw.closed
        assert http.table_socket.file.closed
    finally:
        raw.close()


def test_sdk_explicitly_disables_content_decoding_before_prefetch(
    http_sdk, monkeypatch
):
    """Retain encoded bytes even if urllib3 gains support for YT's codec name."""
    http, client = http_sdk
    body, _ = _transport(b"proxy-compressed payload")
    http.enqueue(body, headers={"Content-Encoding": CODEC})
    original = HTTPResponse.stream
    observed = []

    def stream(self, amt=2**16, decode_content=None):
        if self.headers.get("Content-Encoding") == CODEC:
            observed.append(decode_content)
            assert decode_content is False
        return original(self, amt=amt, decode_content=decode_content)

    monkeypatch.setattr(HTTPResponse, "stream", stream)
    raw = _open(client)
    try:
        assert raw.read() == body
    finally:
        raw.close()
    assert observed


def test_block_reader_preserves_late_http_error_after_valid_terminator(http_sdk):
    http, client = http_sdk
    body, frames = _transport(b'{"a"=1;};')
    http.enqueue(
        body,
        headers={"Content-Encoding": CODEC},
        trailers={
            "X-YT-Response-Code": "1",
            "X-YT-Error": json.dumps({"code": 1, "message": "late transport failure"}),
        },
    )
    raw = _open(client)
    try:
        reader = ZstdBlockReader(raw, codec=CODEC)
        assert reader.read(len(frames[0])) == frames[0]
        with pytest.raises(YtError, match="late transport failure"):
            reader.read(1)
        assert http.table_response.raw.trailers()["x-yt-response-code"] == "1"
    finally:
        raw.close()
    assert http.table_socket.file.closed


@pytest.mark.parametrize("encoding", [None, "identity", "gzip", "z-zstd_5"])
def test_unexpected_encoding_is_rejected_and_response_closed(http_sdk, encoding):
    http, client = http_sdk
    body = gzip.compress(b"unexpected") if encoding == "gzip" else b"unexpected"
    headers = {"Content-Encoding": encoding} if encoding else {}
    http.enqueue(body, headers=headers)
    original = client.config
    session = _get_session(client)
    original_send = session.send
    with pytest.raises(ValueError, match="requested Zstd transport codec"):
        _open(client)
    assert client.config is original
    assert session.send == original_send
    assert http.table_response in http.closed_responses
    assert http.table_socket.file.closed


def test_unsolicited_framing_is_rejected_and_response_closed(http_sdk):
    http, client = http_sdk
    body = b"\x01" + struct.pack("<I", 3) + b"abc"
    http.enqueue(body, headers={"Content-Encoding": CODEC, "X-YT-Framing": "1"})
    original = client.config
    with pytest.raises(ValueError, match="framing"):
        _open(client)
    assert client.config is original
    assert http.table_socket.file.closed


def test_constructor_prefetch_error_restores_config_and_closes_response(http_sdk):
    http, client = http_sdk
    # The SDK interprets this before returning its ResponseStream to _read_table.
    http.enqueue(
        b"\xffunexpected framing bytes",
        headers={"Content-Encoding": CODEC, "X-YT-Framing": "1"},
    )
    original = client.config
    with pytest.raises((YtError, ValueError)) as caught:
        _open(client)
    assert "fram" in str(caught.value).lower()
    assert client.config is original
    assert http.table_socket.file.closed


@pytest.mark.parametrize("status", [200, 202])
def test_initial_yt_error_keeps_its_type_without_content_encoding(http_sdk, status):
    http, client = http_sdk
    http.enqueue(
        b"body need not be read",
        headers={
            "X-YT-Response-Code": "3",
            "X-YT-Error": json.dumps({"code": 3, "message": "header failure"}),
        },
        status=status,
    )
    original = client.config
    with pytest.raises(YtRequestTimedOut, match="header failure") as caught:
        _open(client)
    assert _is_retryable_read(error=caught.value)
    assert client.config is original
    assert http.table_response in http.closed_responses
    assert http.table_socket.file.closed


def test_initial_prefetch_trailer_error_closes_captured_response(http_sdk):
    http, client = http_sdk
    http.enqueue(
        b"",
        headers={"Content-Encoding": CODEC},
        trailers={
            "X-YT-Response-Code": "3",
            "X-YT-Error": json.dumps({"code": 3, "message": "initial trailer failure"}),
        },
    )
    session = _get_session(client)
    original_send = session.send
    with pytest.raises(YtRequestTimedOut, match="initial trailer failure") as caught:
        _open(client)
    assert _is_retryable_read(error=caught.value)
    assert session.send == original_send
    assert http.table_response in http.closed_responses
    assert http.table_socket.file.closed


def test_passthrough_rejects_rpc_before_any_request(http_sdk):
    http, client = http_sdk
    client.config["backend"] = "rpc"
    with pytest.raises(ValueError, match="requires the HTTP backend"):
        _open(client)
    assert http.requests == []
